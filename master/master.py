import asyncio
import logging
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
messages = []      # [{id, text}]
next_id = 1        # глобальний порядковий номер повідомлення
pending = []       # список Secondary, яким потрібно догнати реплікацію


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

    data = request.get_json()
    text = data.get("text")
    w = int(data.get("w", 1))   # write concern

    msg = {"id": next_id, "text": text}
    next_id += 1

    # 1. Запис на master
    messages.append(msg)
    logging.info(f"Отримав повідомлення {msg}, w={w}")

    # 2. Реплікація на Secondaries
    secondaries = ["http://secondary1:5000", "http://secondary2:5000"]
    tasks = []
    for sec in secondaries:
        tasks.append(replicate_to_secondary(sec, msg))

    # 3. Чекаємо потрібну кількість ACK
    ack_count = 1  # master вже зарахований
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:
        if not isinstance(r, Exception) and r:
            ack_count += 1
        if ack_count >= w:
            break

    logging.info(f"ACK отримано: {ack_count}/{w}")

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
        pending.append((url, msg))
    return False


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення на master"""
    return jsonify(messages)


@app.route("/pending", methods=["POST"])
async def resend_pending():
    """
    Secondary викликає цей метод при рестарті,
    щоб отримати "втрачені" повідомлення.
    """
    data = request.get_json()
    url = data.get("url")
    logging.info(f"Secondary {url} запросив pending")

    for msg in messages:
        await replicate_to_secondary(url, msg)

    return jsonify({"status": "resend complete"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
