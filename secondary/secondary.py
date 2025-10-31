import os
import random
import time
import logging
import threading
from typing import Dict, List

from flask import Flask, request, jsonify

# -------------------------------
# Налаштування логування
# -------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [SECONDARY] %(message)s")

app = Flask(__name__)

# -------------------------------
# Локальне сховище
# -------------------------------
messages: List[dict] = []  # лише підтверджені повідомлення [{id, text}]
messages_lock = threading.Lock()
next_expected_id = 1
pending_buffer: Dict[int, dict] = {}

# Затримка для емуляції inconsistency
REPLICA_DELAY = int(os.getenv("REPLICA_DELAY", "0"))
ERROR_RATE = float(os.getenv("ERROR_RATE", "0"))  # ймовірність внутрішньої помилки


@app.route("/replicate", methods=["POST"])
def replicate():
    """
    Secondary отримує повідомлення від Master.
    - додає його у список, якщо ще немає (deduplication)
    - застосовує лише у правильному порядку (total ordering)
    - може затримати ACK (щоб показати блокування / eventual consistency)
    """
    global next_expected_id

    msg = request.get_json()
    msg_id = int(msg.get("id"))

    # Штучна затримка
    if REPLICA_DELAY > 0:
        logging.info(f"Затримка {REPLICA_DELAY}s перед записом...")
        time.sleep(REPLICA_DELAY)

    with messages_lock:
        existing_ids = {m["id"] for m in messages}

        if msg_id in existing_ids:
            logging.info(f"Ігноровано дубль {msg}")
        elif msg_id in pending_buffer:
            logging.info(f"Оновлено відкладене повідомлення {msg}")
            pending_buffer[msg_id] = msg
        elif msg_id < next_expected_id:
            logging.info(f"Отримано старе повідомлення {msg}")
        elif msg_id == next_expected_id:
            messages.append(msg)
            next_expected_id += 1

            while next_expected_id in pending_buffer:
                buffered = pending_buffer.pop(next_expected_id)
                messages.append(buffered)
                next_expected_id += 1

            logging.info(f"Записано повідомлення {msg}")
        else:
            pending_buffer[msg_id] = msg
            logging.info(
                "Отримано повідомлення поза порядком %s, очікуємо id %s",
                msg,
                next_expected_id,
            )

    if ERROR_RATE > 0 and random.random() < ERROR_RATE:
        logging.warning("Відтворюємо внутрішню помилку після обробки повідомлення")
        return jsonify({"status": "error", "msg": msg}), 500

    return jsonify({"status": "replicated", "msg": msg}), 200


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі підтверджені повідомлення Secondary"""
    with messages_lock:
        snapshot = list(sorted(messages, key=lambda item: item["id"]))
    return jsonify(snapshot)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
