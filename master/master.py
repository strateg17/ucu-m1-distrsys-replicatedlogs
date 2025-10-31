import asyncio
import logging
import threading
from typing import List

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
pending: List[tuple] = []       # список Secondary, яким потрібно догнати реплікацію
pending_lock = threading.Lock()

SECONDARIES = ["http://secondary1:5000", "http://secondary2:5000"]


def _drain_task_result(task: asyncio.Task) -> None:
    """Helper to log exceptions from background replication tasks."""
    try:
        exc = task.exception()
        if exc is not None:
            logging.warning(f"Помилка реплікації у фоновому режимі: {exc}")
    except asyncio.CancelledError:
        logging.warning("Фонова задача реплікації була скасована")



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
        messages.append(msg)
    logging.info(f"Отримав повідомлення {msg}, w={w}")

    # 2. Реплікація на Secondaries
    tasks = []
    for sec in SECONDARIES:
        task = asyncio.create_task(replicate_to_secondary(sec, msg))
        task.add_done_callback(_drain_task_result)
        tasks.append(task)

    # 3. Чекаємо потрібну кількість ACK
    ack_count = 1  # master вже зарахований
    ack_target = w

    if ack_count < ack_target and tasks:
        for future in asyncio.as_completed(tasks):
            try:
                result = await future
            except Exception as exc:
                logging.warning(f"Помилка реплікації: {exc}")
                result = False

            if result:
                ack_count += 1

            if ack_count >= ack_target:
                break

    logging.info(f"ACK отримано: {ack_count}/{ack_target}")

    return jsonify({"status": "ok", "acks": ack_count, "msg": msg})


async def replicate_to_secondary(url, msg):
    """
    Відправка повідомлення на secondary.
    Якщо secondary недоступний, додаємо його в pending.
    """
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{url}/replicate", json=msg, timeout=5.0)
            if r.status_code == 200:
                logging.info(f"Успішна реплікація на {url} -> {msg}")
                return True
    except Exception as e:
        logging.warning(f"Помилка реплікації на {url}: {e}")
        with pending_lock:
            pending.append((url, msg))
    return False


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення на master"""
    with messages_lock:
        snapshot = list(messages)
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
