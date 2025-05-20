import os, json, time, uuid
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika, redis

app = FastAPI()

# ---------- Config ---------------------------------------------------
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
redis_cli = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
QUEUE = "tasks_queue"
# ---------------------------------------------------------------------

def publish_task(task: dict, retries: int = 5):
    attempt = 0
    while attempt < retries:
        try:
            with pika.BlockingConnection(pika.URLParameters(RABBIT_URL)) as conn:
                ch = conn.channel()
                ch.queue_declare(queue=QUEUE, durable=True)
                ch.basic_publish(
                    exchange="",
                    routing_key=QUEUE,
                    body=json.dumps(task).encode(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            return
        except pika.exceptions.AMQPConnectionError:
            attempt += 1
            time.sleep(2)
    raise HTTPException(503, "RabbitMQ no disponible")

class Query(BaseModel):
    natural_language_query: str
    context_path: str

@app.post("/api/v1/query")
def submit_query(q: Query):
    if q.context_path.startswith("/") or ".." in q.context_path:
        raise HTTPException(400, "context_path inválido")

    job_id = str(uuid.uuid4())
    task = {"job_id": job_id,
            "query": q.natural_language_query,
            "context_path": q.context_path}

    publish_task(task)
    # Simulamos procesamiento
    redis_cli.hmset(f"job:{job_id}", {"status": "PROCESSING"})

    return {"job_id": job_id}

@app.get("/api/v1/result/{job_id}")
def get_result(job_id: str):
    data = redis_cli.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(404, "job_id no encontrado")

    # Si nunca llegó un resultado real, devolvemos “en proceso”
    return {
        "job_id": job_id,
        "status": data.get("status", "PROCESSING"),
        "result": data.get("result", "En proceso…")
    }

