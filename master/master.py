import logging
import os
import queue
import random
import threading
import time
from typing import Dict, List, Optional

import httpx
from flask import Flask, jsonify, request

# -------------------------------
# Налаштування логування
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MASTER] %(message)s")

app = Flask(__name__)

# -------------------------------
# Локальне сховище повідомлень
# -------------------------------
messages: List[dict] = []  # [{id, text}]
messages_lock = threading.Lock()
next_id = 1  # глобальний порядковий номер повідомлення
counter_lock = threading.Lock()

SECONDARIES = ["http://secondary1:5000", "http://secondary2:5000"]

# Налаштування повторних спроб
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.5"))
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "5.0"))
MASTER_WAIT_TIMEOUT = float(os.getenv("MASTER_WAIT_TIMEOUT", "0"))  # 0 -> без таймауту


class AckFuture:
    """Невеликий thread-safe future для очікування ACK від secondary."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._result = False

    def set_result(self, value: bool) -> None:
        with self._lock:
            if self._event.is_set():
                return
            self._result = value
            self._event.set()

    def wait(self, timeout: Optional[float] = None) -> bool:
        self._event.wait(timeout=timeout)
        with self._lock:
            return self._result and self._event.is_set()

    def done(self) -> bool:
        return self._event.is_set()


class SecondaryWorker:
    """Черга реплікації для конкретного secondary з безкінечними ретраями."""

    def __init__(self, url: str) -> None:
        self.url = url
        # Кожен елемент: (message, ack_future, ack_queue)
        self.queue: queue.Queue = queue.Queue()
        self._client = httpx.Client(timeout=5.0)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        self._status_lock = threading.Lock()
        self._status = "healthy"
        self._consecutive_failures = 0
        self._last_error: str | None = None
        self._last_success: float | None = None

    def enqueue(self, message: dict, ack_queue: Optional[queue.Queue] = None) -> AckFuture:
        ack = AckFuture()
        self.queue.put((message, ack, ack_queue))
        return ack

    def _update_status_success(self) -> None:
        with self._status_lock:
            self._status = "healthy"
            self._consecutive_failures = 0
            self._last_error = None
            self._last_success = time.time()

    def _update_status_failure(self, error: Exception | str) -> None:
        with self._status_lock:
            self._consecutive_failures += 1
            self._last_error = str(error)
            if self._consecutive_failures >= 3:
                self._status = "unhealthy"
            else:
                self._status = "suspected"

    def get_status(self) -> Dict[str, object]:
        with self._status_lock:
            return {
                "status": self._status,
                "consecutive_failures": self._consecutive_failures,
                "last_error": self._last_error,
                "last_success_ts": self._last_success,
            }

    def _run(self) -> None:
        while True:
            message, ack, ack_queue = self.queue.get()
            delay = RETRY_BASE_DELAY
            while True:
                try:
                    response = self._client.post(f"{self.url}/replicate", json=message)
                    if response.status_code == 200:
                        logging.info(f"Успішна реплікація на {self.url} -> {message}")
                        ack.set_result(True)
                        if ack_queue is not None:
                            ack_queue.put(ack)
                        self._update_status_success()
                        break
                    else:
                        error = f"HTTP {response.status_code}: {response.text}"
                        logging.warning(f"Помилка реплікації на {self.url}: {error}")
                        self._update_status_failure(error)
                except Exception as exc:  # noqa: BLE001
                    logging.warning(f"Помилка реплікації на {self.url}: {exc}")
                    self._update_status_failure(exc)

                sleep_for = min(delay, RETRY_MAX_DELAY)
                jitter = random.uniform(0, sleep_for * 0.5)
                time.sleep(sleep_for + jitter)
                delay = min(delay * 2, RETRY_MAX_DELAY)

            self.queue.task_done()


workers = [SecondaryWorker(url) for url in SECONDARIES]


@app.route("/message", methods=["POST"])
def post_message():
    """
    Обробка нового повідомлення від клієнта.
    1. Присвоюємо унікальний id.
    2. Записуємо в локальний список.
    3. Реплікуємо на secondary.
    4. Чекаємо підтверджень (залежно від w).
    """
    global next_id

    data = request.get_json() or {}
    text = data.get("text")

    total_replicas = len(SECONDARIES) + 1
    requested_w = data.get("w")
    if requested_w is None:
        w = total_replicas
    else:
        requested_w = int(requested_w)
        if requested_w > total_replicas:
            logging.warning(
                f"Запитаний рівень write concern {requested_w} перевищує доступні репліки, "
                f"використовую {total_replicas}"
            )
        w = max(1, min(requested_w, total_replicas))

    with counter_lock:
        msg_id = next_id
        next_id += 1

    msg = {"id": msg_id, "text": text}

    # 1. Запис на master
    with messages_lock:
        is_duplicate = any(existing["id"] == msg["id"] for existing in messages)
        if not is_duplicate:
            messages.append(msg)
            messages.sort(key=lambda item: item["id"])

    if is_duplicate:
        logging.info(f"Отримав дубль повідомлення {msg}")
    else:
        logging.info(f"Отримав повідомлення {msg}, w={w}")

    # 2. Реплікація на Secondaries
    ack_queue: queue.Queue = queue.Queue()
    for worker in workers:
        worker.enqueue(msg, ack_queue)

    # 3. Чекаємо потрібну кількість ACK
    ack_target = w
    ack_count = 1  # master вже зарахований

    if ack_target > 1:
        required_from_secondaries = ack_target - 1
        start_time = time.time()
        while required_from_secondaries > 0:
            timeout = None
            if MASTER_WAIT_TIMEOUT > 0:
                elapsed = time.time() - start_time
                remaining = MASTER_WAIT_TIMEOUT - elapsed
                if remaining <= 0:
                    logging.warning(
                        "Таймаут очікування ACK від secondary, завершую запит"
                    )
                    break
                timeout = remaining

            try:
                ack_future = ack_queue.get(timeout=timeout)
            except queue.Empty:
                logging.warning("Не вдалося дочекатися ACK від secondary (таймаут)")
                break

            if ack_future.wait(timeout=0):
                ack_count += 1
                required_from_secondaries -= 1
                logging.info(f"ACK від secondary отримано ({ack_count}/{ack_target})")
            else:
                logging.warning("Secondary повернув негативний ACK")

            ack_queue.task_done()

    status_code = 200 if ack_count >= ack_target else 202
    logging.info(f"ACK отримано: {ack_count}/{ack_target}")

    return jsonify({"status": "ok", "acks": ack_count, "msg": msg}), status_code


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення на master"""
    with messages_lock:
        snapshot = sorted(messages, key=lambda item: item["id"])
    return jsonify(snapshot)


@app.route("/pending", methods=["POST"])
def resend_pending():
    """
    Secondary викликає цей метод при рестарті,
    щоб отримати "втрачені" повідомлення.
    """
    data = request.get_json()
    url = data.get("url")
    logging.info(f"Secondary {url} запросив pending")

    with messages_lock:
        snapshot = list(messages)

    for msg in snapshot:
        for worker in workers:
            if worker.url == url:
                worker.enqueue(msg)
                break

    return jsonify({"status": "resend queued"})


@app.route("/health", methods=["GET"])
def health():
    """Поточний стан secondaries."""
    status = {worker.url: worker.get_status() for worker in workers}
    return jsonify(status)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
