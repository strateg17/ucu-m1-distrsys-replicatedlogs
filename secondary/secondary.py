import os
import time
import logging
import threading
from typing import List

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
