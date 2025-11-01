import os
import time
import logging
import threading
from typing import List

import httpx
from flask import Flask, request, jsonify

# -------------------------------
# Налаштування логування
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SECONDARY] %(message)s")

app = Flask(__name__)

# -------------------------------
# Локальне сховище
# -------------------------------
messages: List[dict] = []  # [{id, text}]
messages_lock = threading.Lock()

# Затримка для емуляції inconsistency
REPLICA_DELAY = int(os.getenv("REPLICA_DELAY", "0"))

# Налаштування для синхронізації з master після рестарту
MASTER_URL = os.getenv("MASTER_URL", "http://master:5000")
SECONDARY_URL = os.getenv("SECONDARY_URL")


def request_pending_from_master(max_retries: int = 5, retry_delay: float = 2.0) -> None:
    """Отримати пропущені повідомлення з master після рестарту."""

    if not MASTER_URL:
        logging.warning("MASTER_URL не задано, пропускаю синхронізацію pending")
        return

    if not SECONDARY_URL:
        logging.warning("SECONDARY_URL не задано, пропускаю синхронізацію pending")
        return

    payload = {"url": SECONDARY_URL}

    for attempt in range(1, max_retries + 1):
        try:
            logging.info(
                f"Спроба #{attempt} отримати pending з {MASTER_URL} для {SECONDARY_URL}"
            )
            with httpx.Client(timeout=10.0) as client:
                response = client.post(f"{MASTER_URL}/pending", json=payload)

            if response.status_code == 200:
                logging.info("Успішно синхронізовано pending з master")
                return

            logging.warning(
                "Неочікувана відповідь від master під час синхронізації pending: "
                f"{response.status_code} {response.text}"
            )
        except Exception as exc:  # pragma: no cover - логування помилок
            logging.warning(f"Не вдалося отримати pending з master: {exc}")

        time.sleep(retry_delay)

    logging.error("Вичерпано спроби отримати pending з master")


@app.route("/replicate", methods=["POST"])
def replicate():
    """
    Secondary отримує повідомлення від Master.
    - додає його у список, якщо ще немає (deduplication)
    - сортує список за id (total ordering)
    - може затримати ACK (щоб показати блокування / eventual consistency)
    """
    msg = request.get_json()

    # Штучна затримка
    if REPLICA_DELAY > 0:
        logging.info(f"Затримка {REPLICA_DELAY}s перед записом...")
        time.sleep(REPLICA_DELAY)

    with messages_lock:
        is_duplicate = any(m["id"] == msg["id"] for m in messages)
        if not is_duplicate:
            messages.append(msg)
            # Total ordering
            messages.sort(key=lambda m: m["id"])

    if is_duplicate:
        logging.info(f"Ігноровано дубль {msg}")
    else:
        logging.info(f"Записано повідомлення {msg}")

    return jsonify({"status": "replicated", "msg": msg}), 200


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення Secondary"""
    with messages_lock:
        snapshot = sorted(messages, key=lambda item: item["id"])
    return jsonify(snapshot)


def schedule_pending_sync() -> None:
    """Запустити синхронізацію pending у фоні."""

    thread = threading.Thread(
        target=request_pending_from_master,
        name="pending-sync",
        daemon=True,
    )
    thread.start()


def launch_pending_sync() -> None:
    """Ensure pending sync runs after the server starts accepting requests."""

    schedule_pending_sync()


if __name__ == "__main__":
    launch_pending_sync()
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True, use_reloader=False)
