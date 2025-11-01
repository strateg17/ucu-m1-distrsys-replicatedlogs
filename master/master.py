import asyncio
import logging
import threading
from typing import Dict, List

from flask import Flask, request, jsonify
import httpx

# -------------------------------
# Налаштування логування
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MASTER] %(message)s")

app = Flask(__name__)

# -------------------------------
# Локальне сховище повідомлень
# -------------------------------
messages: List[dict] = []      # [{id, text}]
messages_lock = threading.Lock()
next_id = 1                    # глобальний порядковий номер повідомлення
counter_lock = threading.Lock()
pending_lock = threading.Lock()
pending: Dict[str, List[dict]] = {}

SECONDARIES = ["http://secondary1:5000", "http://secondary2:5000"]


def _drain_task_result(task: asyncio.Task) -> None:
    """Helper to log exceptions from background replication tasks."""
    try:
        exc = task.exception()
        if exc is not None:
            logging.warning(f"Помилка реплікації у фоновому режимі: {exc}")
    except asyncio.CancelledError:
        logging.warning("Фонова задача реплікації була скасована")


def _enqueue_pending(url: str, msg: dict) -> None:
    """Додає повідомлення в чергу pending для secondary, зберігаючи порядок."""
    with pending_lock:
        queue = pending.setdefault(url, [])
        if any(item["id"] == msg["id"] for item in queue):
            return
        queue.append(msg)
        queue.sort(key=lambda item: item["id"])


def _remove_from_pending(url: str, msg_id: int) -> None:
    """Видаляє повідомлення з pending, якщо воно там є."""
    with pending_lock:
        queue = pending.get(url)
        if not queue:
            return
        pending[url] = [item for item in queue if item["id"] != msg_id]
        if not pending[url]:
            pending.pop(url, None)


async def _send_to_secondary(url: str, msg: dict) -> bool:
    """Надсилає повідомлення на secondary, повертає True/False залежно від успіху."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{url}/replicate", json=msg, timeout=5.0)
            if r.status_code == 200:
                logging.info(f"Успішна реплікація на {url} -> {msg}")
                return True
            logging.warning(
                f"Реплікація на {url} повернула статус {r.status_code}: {r.text}"
            )
    except Exception as exc:
        logging.warning(f"Помилка реплікації на {url}: {exc}")
    return False


def _is_message_pending(url: str, msg_id: int) -> bool:
    """Перевіряє, чи залишилося повідомлення в черзі pending для secondary."""
    with pending_lock:
        queue = pending.get(url, [])
    return any(item["id"] == msg_id for item in queue)


async def _flush_pending_queue(url: str) -> None:
    """Послідовно надсилає всі pending-повідомлення на secondary, поки не вичерпає чергу."""
    while True:
        with pending_lock:
            queue = pending.get(url, [])
            if not queue:
                return
            msg = queue[0]

        success = await _send_to_secondary(url, msg)
        if success:
            _remove_from_pending(url, msg["id"])
            continue

        # Не вдалося доставити поточне повідомлення — зупиняємося,
        # черга залишиться для майбутніх спроб.
        return



@app.route("/message", methods=["POST"])
async def post_message():
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

    necessary_acks = len(SECONDARIES) + 1  # master + всі secondary
    requested_w = data.get("w")
    if requested_w is None:
        w = necessary_acks
    else:
        requested_w = int(requested_w)
        if requested_w > necessary_acks:
            logging.warning(
                f"Запитаний рівень write concern {requested_w} перевищує доступні ACK, "
                f"використовую {necessary_acks}"
            )
        w = max(1, min(requested_w, necessary_acks))

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

    # Навіть у випадку дублю, продовжуємо реплікацію, щоб secondary гарантовано
    # отримали останній стан.

    # 2. Реплікація на Secondaries
    tasks = []
    for sec in SECONDARIES:
        task = asyncio.create_task(replicate_to_secondary(sec, msg))
        task.add_done_callback(_drain_task_result)
        tasks.append(task)

    # 3. Чекаємо потрібну кількість ACK
    ack_target = w
    required_secondary_acks = max(0, ack_target - 1)
    confirmed_secondary_acks = 0

    if required_secondary_acks > 0 and tasks:
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
            except Exception as exc:
                logging.warning(f"Помилка реплікації: {exc}")
                result = False

            if result:
                confirmed_secondary_acks += 1

            if confirmed_secondary_acks >= required_secondary_acks:
                break

    ack_count = 1 + confirmed_secondary_acks  # master + secondary ACK, які ми дочекалися
    logging.info(f"ACK отримано: {ack_count}/{ack_target}")

    if ack_count < ack_target:
        logging.warning(
            f"Не вдалося отримати {ack_target} підтверджень, повертаю помилку клієнту"
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "acks": ack_count,
                    "required": ack_target,
                    "msg": msg,
                }
            ),
            503,
        )

    return jsonify({"status": "ok", "acks": ack_count, "msg": msg})


async def replicate_to_secondary(url, msg):
    """
    Відправка повідомлення на secondary.
    Якщо secondary недоступний, додаємо його в pending.
    """
    _enqueue_pending(url, msg)
    await _flush_pending_queue(url)
    delivered = not _is_message_pending(url, msg["id"])
    if delivered:
        logging.info(f"Повідомлення {msg['id']} синхронізовано з {url}")
    return delivered


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення на master"""
    with messages_lock:
        snapshot = sorted(messages, key=lambda item: item["id"])
    return jsonify(snapshot)


@app.route("/pending", methods=["POST"])
async def resend_pending():
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
        await replicate_to_secondary(url, msg)

    return jsonify({"status": "resend complete"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
