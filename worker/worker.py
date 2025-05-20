import os, json, time, pathlib, shlex, logging, traceback, uuid
import pika, redis, openai
from pydantic import BaseModel, Field, ValidationError

# ───────────────────────── Configuración ────────────────────────────
RABBIT_URL  = os.getenv("RABBIT_URL",  "amqp://guest:guest@rabbitmq:5672/")
REDIS_HOST  = os.getenv("REDIS_HOST",  "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
QUEUE       = os.getenv("QUEUE", "tasks_queue")
BASE_DIR    = pathlib.Path(os.getenv("BASE_DIR", "/data")).resolve()
MODEL_NAME  = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# —————— Aquí cargamos clave y endpoint de OpenRouter ——————
openai.api_key  = os.getenv("OPENAI_API_KEY")
openai.base_url = os.getenv("OPENAI_API_BASE", openai.base_url)

redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ────────────────────────── Esquemas ────────────────────────────────
class ParsedCommand(BaseModel):
    action: str               # ls | mv | rm
    pattern: str              # glob (*.pdf) o nombre
    destination: str | None = None   # solo para mv

# ─────────────── Utilidades de seguridad de ruta ────────────────────
def resolve_path(pattern: str) -> list[pathlib.Path]:
    if pattern.startswith(("/", "~")) or ".." in pathlib.Path(pattern).parts:
        raise ValueError("Ruta/patrón no permitido")
    return list(BASE_DIR.glob(pattern))

def safe_dest_path(dest: str) -> pathlib.Path:
    d = (BASE_DIR / dest).resolve()
    if not d.is_relative_to(BASE_DIR):
        raise ValueError("Destino fuera de BASE_DIR")
    d.mkdir(parents=True, exist_ok=True)
    return d

# ─────────────── Paso 1: llamar al modelo LLM ───────────────────────
SYSTEM_PROMPT = """
Convierte la instrucción del usuario sobre archivos en un JSON con los
campos: action (ls|mv|rm), pattern (glob relativo) y destination (solo si action=mv).
Ejemplos:
- "lista mis pdf": {"action":"ls","pattern":"**/*.pdf"}
- "borra los .tmp": {"action":"rm","pattern":"**/*.tmp"}
- "mueve todos los pdf de redes a revisados": {"action":"mv","pattern":"redes/**/*.pdf","destination":"revisados/"}
Responde SOLO el JSON, sin texto extra.
"""

def interpret(nl_query: str) -> ParsedCommand:
    # debug
    logging.debug("🔑 API_KEY present? %s", bool(openai.api_key))
    logging.debug("🌐 API_BASE = %s", openai.base_url)
    logging.debug("🤖 MODEL = %s", MODEL_NAME)

    resp = openai.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": nl_query}
        ],
        temperature=0.2,
        response_format="json"       # ← fuerza respuesta JSON
    )
    content = resp.choices[0].message.content.strip()
    logging.debug("LLM raw → %s", content)
    try:
        data = json.loads(content)
        return ParsedCommand(**data)
    except (json.JSONDecodeError, ValidationError) as e:
        raise RuntimeError(f"Respuesta LLM inválida: {e}")

# ─────────────── Paso 2: ejecutar la acción ─────────────────────────
def execute(cmd: ParsedCommand) -> dict:
    files = resolve_path(cmd.pattern)
    logs  = []
    match cmd.action:
        case "ls":
            logs = [str(p.relative_to(BASE_DIR)) for p in files]
        case "rm":
            for p in files:
                p.unlink(missing_ok=True)
                logs.append(f"Deleted {p.relative_to(BASE_DIR)}")
        case "mv":
            if not cmd.destination:
                raise ValueError("destination requerido para mv")
            dest_dir = safe_dest_path(cmd.destination)
            for p in files:
                target = dest_dir / p.name
                target.write_bytes(p.read_bytes())
                p.unlink()
                logs.append(f"Moved {p.relative_to(BASE_DIR)} → {target.relative_to(BASE_DIR)}")
        case _:
            raise ValueError(f"Acción no soportada: {cmd.action}")
    return {"affected": len(logs), "logs": logs}

# ─────────────── Paso 3: worker RabbitMQ ────────────────────────────
def process_message(ch, method, properties, body):
    try:
        task   = json.loads(body)
        job_id = task["job_id"]
        query  = task["query"]

        redis_cli.hset(f"job:{job_id}", "status", "PROCESSING")
        logging.info("🛠️  %s — \"%s\"", job_id, query)

        cmd  = interpret(query)
        result = execute(cmd)

        redis_cli.hset(f"job:{job_id}", mapping={
            "status": "FINISHED",
            "result": json.dumps(result, ensure_ascii=False)
        })
        logging.info("✅  %s — %s archivos", job_id, result["affected"])
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logging.error("❌  Error procesando mensaje: %s", traceback.format_exc())
        job_id = locals().get("job_id", str(uuid.uuid4()))
        redis_cli.hset(f"job:{job_id}", mapping={
            "status": "FAILED",
            "result": str(e)
        })
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBIT_URL))
            ch   = conn.channel()
            ch.queue_declare(queue=QUEUE, durable=True)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=QUEUE, on_message_callback=process_message)
            logging.info("👂 Esperando tareas…")
            ch.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logging.warning("RabbitMQ no disponible. Reintentando en 5 s…")
            time.sleep(5)

if __name__ == "__main__":
    main()

