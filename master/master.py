import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Coroutine, Optional, Union

from concurrent.futures import Future

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

HEALTHY = "Healthy"
SUSPECTED = "Suspected"
UNHEALTHY = "Unhealthy"

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 3.0
SUSPECT_LATENCY_THRESHOLD = 1.0

health_lock = threading.Lock()
secondary_health: Dict[str, Dict[str, Any]] = {}
secondary_availability: Dict[str, threading.Event] = {}
heartbeat_handles: List[Future] = []
heartbeat_started = threading.Event()


def _log_future_result(fut: Future) -> None:
    """Log unexpected errors from background replication tasks."""
    try:
        fut.result()
    except asyncio.CancelledError:
        logging.warning("Фонова задача реплікації була скасована")
    except Exception as exc:
        logging.warning(f"Помилка реплікації у фоновому режимі: {exc}")


def _replication_loop_worker(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


replication_loop = asyncio.new_event_loop()
replication_thread = threading.Thread(
    target=_replication_loop_worker,
    args=(replication_loop,),
    name="replication-loop",
    daemon=True,
)
replication_thread.start()


def _schedule_replication(
    coro: Coroutine[Any, Any, Any]
) -> Union[asyncio.Future, Future]:
    """Submit replication coroutine to the dedicated background loop."""
    cfut = asyncio.run_coroutine_threadsafe(coro, replication_loop)
    cfut.add_done_callback(_log_future_result)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # Виклик може відбуватися поза асинхронним контекстом (наприклад, під час
        # ініціалізації heartbeat моніторів). У такому випадку повертаємо звичайний
        # future з executor, щоб зберегти посилання на задачу.
        return cfut

    return asyncio.wrap_future(cfut, loop=loop)


def _init_health_tracking() -> None:
    with health_lock:
        for url in SECONDARIES:
            if url in secondary_health:
                continue
            secondary_health[url] = {
                "status": HEALTHY,
                "latency": None,
                "last_checked": None,
                "error": None,
            }
            event = threading.Event()
            event.set()
            secondary_availability[url] = event


_init_health_tracking()


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


def _set_secondary_status(
    url: str,
    status: str,
    *,
    latency: Optional[float] = None,
    error: Optional[str] = None,
) -> None:
    with health_lock:
        info = secondary_health.setdefault(
            url, {"status": UNHEALTHY, "latency": None, "last_checked": None, "error": None}
        )
        previous = info["status"]
        info.update(
            {
                "status": status,
                "latency": latency,
                "last_checked": time.time(),
                "error": error,
            }
        )

    if previous != status:
        logging.info(f"Стан {url} змінено: {previous} -> {status}")

    availability = secondary_availability.setdefault(url, threading.Event())
    if status == UNHEALTHY:
        availability.clear()
    else:
        availability.set()


def _record_heartbeat_success(url: str, latency: float) -> None:
    status = HEALTHY if latency <= SUSPECT_LATENCY_THRESHOLD else SUSPECTED
    error = None if status == HEALTHY else f"Latency {latency:.2f}s перевищує ліміт"
    _set_secondary_status(url, status, latency=latency, error=error)


def _record_heartbeat_failure(url: str, error: Exception) -> None:
    _set_secondary_status(url, UNHEALTHY, latency=None, error=str(error))


async def _heartbeat_monitor(url: str) -> None:
    await asyncio.sleep(0.1)
    while True:
        start = asyncio.get_running_loop().time()
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{url}/health",
                    timeout=HEARTBEAT_TIMEOUT,
                )
            latency = asyncio.get_running_loop().time() - start
            if response.status_code == 200:
                _record_heartbeat_success(url, latency)
            else:
                _record_heartbeat_failure(
                    url,
                    RuntimeError(
                        f"Unexpected status {response.status_code}: {response.text}"
                    ),
                )
        except Exception as exc:  # pragma: no cover - network errors
            _record_heartbeat_failure(url, exc)

        await asyncio.sleep(HEARTBEAT_INTERVAL)


async def _wait_for_availability(url: str) -> None:
    availability = secondary_availability.setdefault(url, threading.Event())
    while not availability.is_set():
        logging.info(f"{url} недоступний, очікую відновлення")
        await asyncio.sleep(1.0)


def _start_heartbeat_tasks() -> None:
    if heartbeat_started.is_set():
        return

    heartbeat_started.set()
    for url in SECONDARIES:
        handle = _schedule_replication(_heartbeat_monitor(url))
        heartbeat_handles.append(handle)


@app.before_first_request
def _bootstrap_background_tasks() -> None:
    _start_heartbeat_tasks()


def _format_timestamp(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


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


async def _flush_pending_queue(url: str) -> bool:
    """Послідовно надсилає всі pending-повідомлення на secondary, поки не вичерпає чергу.

    Повертає ``True``, якщо вдалося доставити хоча б одне повідомлення під час виклику.
    """
    delivered_any = False
    while True:
        with pending_lock:
            queue = pending.get(url, [])
            if not queue:
                return delivered_any
            msg = queue[0]

        success = await _send_to_secondary(url, msg)
        if success:
            delivered_any = True
            _remove_from_pending(url, msg["id"])
            continue

        # Не вдалося доставити поточне повідомлення — зупиняємося,
        # черга залишиться для майбутніх спроб.
        return delivered_any



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

    _start_heartbeat_tasks()

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
        task = _schedule_replication(replicate_to_secondary(sec, msg))
        tasks.append(task)

    # 3. Чекаємо потрібну кількість ACK
    ack_target = w
    required_secondary_acks = max(0, ack_target - 1)
    confirmed_secondary_acks = 0

    if tasks:
        if required_secondary_acks > 0:
            logging.info(
                f"Очікую підтверджень від {required_secondary_acks} secondary вузлів"
            )
            for future in asyncio.as_completed(tasks):
                try:
                    result = await future
                except Exception as exc:
                    logging.warning(f"Помилка реплікації: {exc}")
                    result = False

                if result:
                    confirmed_secondary_acks += 1
                    logging.info(
                        f"Отримано підтвердження {confirmed_secondary_acks}/"
                        f"{required_secondary_acks} secondary"
                    )

                if confirmed_secondary_acks >= required_secondary_acks:
                    break
        else:
            # Для w=1 лише запускаємо фонові задачі та одразу повертаємо відповідь.
            # Фоновий цикл реплікації гарантує, що задачі не буде скасовано.
            pass

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
    backoff = 0.5
    max_backoff = 5.0

    while True:
        await _wait_for_availability(url)
        made_progress = await _flush_pending_queue(url)
        delivered = not _is_message_pending(url, msg["id"])
        if delivered:
            logging.info(f"Повідомлення {msg['id']} синхронізовано з {url}")
            return True

        if not made_progress:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
        else:
            backoff = 0.5


@app.route("/messages", methods=["GET"])
def get_messages():
    """Повертає всі повідомлення на master"""
    with messages_lock:
        snapshot = sorted(messages, key=lambda item: item["id"])
    return jsonify(snapshot)


@app.route("/health", methods=["GET"])
def get_secondaries_health():
    with health_lock:
        snapshot = {
            url: {
                "status": info.get("status", UNHEALTHY),
                "latency_seconds": info.get("latency"),
                "latency_ms": (info["latency"] * 1000) if info.get("latency") is not None else None,
                "last_checked": _format_timestamp(info.get("last_checked")),
                "error": info.get("error"),
            }
            for url, info in secondary_health.items()
        }
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
    _start_heartbeat_tasks()
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
