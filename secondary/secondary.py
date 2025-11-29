import os
import time
import logging
import threading
from typing import Dict, List

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
messages_by_id: Dict[int, dict] = {}
committed_upto = 0
messages_lock = threading.Lock()

MASTER_URL = os.getenv("MASTER_URL", "http://master:5000")
SECONDARY_URL = os.getenv("SECONDARY_URL", "http://secondary:5000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2"))

heartbeat_started = threading.Event()
resync_in_progress = threading.Event()

# Затримка для емуляції inconsistency
REPLICA_DELAY = int(os.getenv("REPLICA_DELAY", "0"))


def _get_local_count() -> int:
    with messages_lock:
        return committed_upto


def _request_full_resync(expected_count: int) -> None:
    if resync_in_progress.is_set():
        return

    resync_in_progress.set()
    try:
        logging.info(
            f"Виявлено розбіжність журналів (має бути {expected_count}, локально {_get_local_count()}). "
            "Запитую повний ресинх..."
        )
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(f"{MASTER_URL}/pending", json={"url": SECONDARY_URL})
            if resp.status_code == 200:
                logging.info("Повний ресинх завершено")
            else:
                logging.warning(
                    f"Не вдалося виконати ресинх: статус {resp.status_code}, тіло {resp.text}"
                )
    except Exception as exc:  # pragma: no cover - мережеві помилки
        logging.warning(f"Помилка під час ресинху: {exc}")
    finally:
        resync_in_progress.clear()


def _heartbeat_loop() -> None:
    while True:
        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(f"{MASTER_URL}/health")
            if response.status_code != 200:
                logging.warning(
                    f"Хартбіт до master повернув статус {response.status_code}: {response.text}"
                )
            else:
                data = response.json()
                master_messages = data.get("master", {}).get("messages")
                if master_messages is not None and master_messages != _get_local_count():
                    _request_full_resync(master_messages)

        except Exception as exc:  # pragma: no cover - мережеві помилки
            logging.warning(f"Не вдалося виконати хартбіт до master: {exc}")

        time.sleep(HEARTBEAT_INTERVAL)


def _start_heartbeat_thread() -> None:
    if heartbeat_started.is_set():
        return

    heartbeat_started.set()
    thread = threading.Thread(target=_heartbeat_loop, name="heartbeat-to-master", daemon=True)
    thread.start()

@app.route("/replicate", methods=["POST"])
def replicate():
    """
    Secondary отримує повідомлення від Master.
    - додає його у список, якщо ще немає (deduplication)
    - сортує список за id (total ordering)
    - може затримати ACK (щоб показати блокування / eventual consistency)
    """
    msg = request.get_json()

    global committed_upto

    # Штучна затримка
    if REPLICA_DELAY > 0:
        logging.info(f"Затримка {REPLICA_DELAY}s перед записом...")
        time.sleep(REPLICA_DELAY)

    with messages_lock:
        is_duplicate = msg["id"] in messages_by_id
        if not is_duplicate:
            messages_by_id[msg["id"]] = msg

            expected_next_id = committed_upto + 1
            if msg["id"] < expected_next_id:
                logging.info(
                    f"Отримано запізніле повідомлення {msg['id']}, очікував {expected_next_id}"
                )

            while messages_by_id.get(committed_upto + 1):
                committed_upto += 1

    if is_duplicate:
        logging.info(f"Ігноровано дубль {msg}")
    else:
        logging.info(f"Записано повідомлення {msg}")

    return jsonify({"status": "replicated", "msg": msg}), 200


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення Secondary"""
    with messages_lock:
        snapshot: List[dict] = [messages_by_id[i] for i in range(1, committed_upto + 1)]
    return jsonify(snapshot)


@app.route("/health", methods=["GET"])
def healthcheck():
    with messages_lock:
        replicated = committed_upto
    return jsonify({
        "status": "ok",
        "committed_upto": replicated,
        "replica_delay": REPLICA_DELAY,
    })


if hasattr(app, "before_serving"):
    app.before_serving(_start_heartbeat_thread)
else:  # Flask 3.x fallback
    app.before_request(_start_heartbeat_thread)


if __name__ == "__main__":
    _start_heartbeat_thread()
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True, use_reloader=False)
