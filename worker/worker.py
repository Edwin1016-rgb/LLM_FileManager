import os, json, time, pathlib, shlex, logging, traceback, uuid, re
import pika, redis, requests
from pydantic import BaseModel, Field, ValidationError
import ast

# ───────────────────────── Configuración ────────────────────────────
RABBIT_URL  = os.getenv("RABBIT_URL",  "amqp://guest:guest@rabbitmq:5672/")
REDIS_HOST  = os.getenv("REDIS_HOST",  "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", 6379))
QUEUE       = os.getenv("QUEUE", "tasks_queue")
BASE_DIR    = pathlib.Path(os.getenv("BASE_DIR", "/data")).resolve()
MODEL_NAME  = os.getenv("OPENAI_MODEL", "deepseek/deepseek-r1-zero:free")
API_KEY     = os.getenv("OPENAI_API_KEY")
API_BASE="https://openrouter.ai/api/v1"

redis_cli = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ────────────────────────── Esquemas ────────────────────────────────
class ParsedCommand(BaseModel):
    action: str
    pattern: str
    destination: str | None = None

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
SYSTEM_PROMPT ="""
Eres un asistente que SOLO responde con JSON VÁLIDO. 

FORMATO REQUERIDO:
```json
{
  "action": "ls|mv|rm",
  "pattern": "patrón_glob",
  "destination": "solo_para_mv" 
}
INSTRUCCIONES ABSOLUTAS:

NUNCA incluyas texto fuera del JSON

NO uses markdown (```json)

Los valores DEBEN ser strings con comillas dobles

Si hay error: {"action":"error","pattern":"descripción"}

EJEMPLO PARA 'mueve PDFs de redes a revisados':

json
{"action":"mv","pattern":"redes/**/*.pdf","destination":"revisados"}
RESPONDERÁS ÚNICAMENTE CON EL JSON VÁLIDO, SIN EXCEPCIONES.
"""

def interpret(nl_query: str) -> ParsedCommand:
    logging.info(f"🔍 Procesando consulta: '{nl_query}'")
    
    url = f"{API_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "HTTP-Referer": "http://localhost",
        "X-Title": "File Manager Assistant",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": nl_query}
        ],
        "temperature": 0.1,
        "max_tokens": 200
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Registrar la respuesta completa para verificar la estructura
        logging.debug(f"📨 Respuesta completa: {json.dumps(data, indent=2)}")
        
        # Extraemos el contenido de la respuesta
        content = None
        
        # Intentamos primero con la estructura estándar de OpenAI
        if "choices" in data and len(data["choices"]) > 0:
            if "message" in data["choices"][0] and "content" in data["choices"][0]["message"]:
                content = data["choices"][0]["message"]["content"]
        
        # Si no encontramos contenido en la estructura de OpenAI, intentamos con Fireworks/OpenRouter
        if not content and "choices" in data and len(data["choices"]) > 0:
            if "text" in data["choices"][0]:
                content = data["choices"][0]["text"]
        
        # Si aún no encontramos contenido, buscar en otros campos posibles
        if not content:
            for key in ["reasoning", "data", "output", "response"]:
                if key in data:
                    content = data[key]
                    break
        
        if not content:
            raise RuntimeError("No se encontró contenido en choices[0].message.content ni en choices[0].text ni en otros campos posibles.")
        
        # Limpieza del contenido para eliminar cualquier envoltorio LaTeX (por ejemplo, \boxed{})
        cleaned_content = content.strip()
        
        # Eliminar cualquier texto LaTeX como \boxed{}
        cleaned_content = re.sub(r'\\boxed{(.*)}', r'\1', cleaned_content)

        # Eliminar cualquier bloque de código Markdown (```json {...}`)
        cleaned_content = re.sub(r'```json(.*?)```', r'\1', cleaned_content, flags=re.DOTALL)

        # Verifica si después de la limpieza, el contenido está vacío
        if not cleaned_content:
            raise RuntimeError("El contenido está vacío después de limpieza")
        
        logging.debug(f"🧹 Contenido limpio: {cleaned_content}")
        
        # Parseo final
        try:
            parsed_data = json.loads(cleaned_content)
            return ParsedCommand(**parsed_data)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"JSON inválido extraído: {cleaned_content}. Error: {str(e)}")
        except ValidationError as e:
            raise RuntimeError(f"Validación fallida para: {parsed_data}. Error: {str(e)}")
            
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error de conexión: {str(e)}")
    except Exception as e:
        logging.error(f"Error completo: {traceback.format_exc()}")
        raise RuntimeError(f"Error procesando la respuesta: {str(e)}")


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