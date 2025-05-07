import os
import json
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pika
import redis

app = FastAPI()

# --- Configuración ---------------------------------------------------
RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")

redis_client = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
# ---------------------------------------------------------------------

def publish_task(task: dict, max_retries: int = 5):
    """Abre conexión breve a RabbitMQ y publica el mensaje, reintentando si es necesario."""
    attempt = 0
    while attempt < max_retries:
        try:
            with pika.BlockingConnection(pika.URLParameters(RABBIT_URL)) as conn:
                ch = conn.channel()
                ch.queue_declare(queue="file_op_tasks", durable=True)
                ch.basic_publish(
                    exchange="",
                    routing_key="file_op_tasks",
                    body=json.dumps(task).encode(),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
            return
        except pika.exceptions.AMQPConnectionError:
            attempt += 1
            time.sleep(2)
    raise HTTPException(status_code=503, detail="RabbitMQ no disponible")

class QueryPayload(BaseModel):
    natural_language_query: str
    context_path: str

@app.post("/api/v1/query")
def submit_query(payload: QueryPayload):
    # Validación simple de path (no absoluto, sin '..')
    if payload.context_path.startswith("/") or ".." in payload.context_path:
        raise HTTPException(status_code=400, detail="context_path inválido")

    job_id = str(uuid.uuid4())
    task = {
        "job_id": job_id,
        "natural_language_query": payload.natural_language_query,
        "context_path": payload.context_path
    }

    # Publicar en la cola y guardar estado inicial
    publish_task(task)
    redis_client.hmset(f"job:{job_id}", {"status": "QUEUED"})

    return {"job_id": job_id}
