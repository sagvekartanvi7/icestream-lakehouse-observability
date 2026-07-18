import asyncio
import json
from collections import deque
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer

app = FastAPI()

ERROR_THRESHOLD = 0.02
WINDOW_SIZE = 20
recent_results = deque(maxlen=WINDOW_SIZE)
connected_clients = []


@app.get("/")
async def get_page():
    with open("dashboard.html") as f:
        return HTMLResponse(f.read())


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except Exception:
        connected_clients.remove(websocket)


async def broadcast(data):
    dead = []
    for client in connected_clients:
        try:
            await client.send_json(data)
        except Exception:
            dead.append(client)
    for d in dead:
        connected_clients.remove(d)


def kafka_reader(loop):
    consumer = KafkaConsumer(
        "checkout-orders",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
    )
    for message in consumer:
        order = message.value
        is_good = order["tax_amount"] is not None
        recent_results.append(is_good)
        error_rate = recent_results.count(False) / len(recent_results)

        payload = {
            "order_id": order["order_id"][:8],
            "good": is_good,
            "error_rate": round(error_rate, 3),
            "tripped": error_rate > ERROR_THRESHOLD,
        }
        asyncio.run_coroutine_threadsafe(broadcast(payload), loop)


@app.on_event("startup")
async def start_kafka_thread():
    import threading
    loop = asyncio.get_event_loop()
    threading.Thread(target=kafka_reader, args=(loop,), daemon=True).start()