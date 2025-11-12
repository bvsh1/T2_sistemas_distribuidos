import os
import json
import threading
import time
import logging
import uuid # <-- 1. Necesario para nuevos IDs de mensajes
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse # <-- 2. Para devolver status 202
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from sqlalchemy.exc import OperationalError
from kafka import KafkaConsumer, KafkaProducer # <-- 3. Importar Productor
from kafka.errors import NoBrokersAvailable
from pydantic import BaseModel # <-- 4. Para validar el request
from contextlib import asynccontextmanager

# =================================================================
# Configuración
# ================================================================
logging.basicConfig(level=logging.INFO)
# Logger principal de este servicio
logger = logging.getLogger("BDD_GATEWAY")

DB_URL = os.getenv("DB_URL", "sqlite:///./data/data.db")
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")

# Tópicos
TOPIC_CONSUME_VALIDATED = "questions.validated" # De Flink
TOPIC_PRODUCE_NEW = "questions.llm"         # Hacia el LLM

# Variable global para el productor
kafka_producer: KafkaProducer = None

# =================================================================
# SECCIÓN 1: Base de Datos (Sin cambios)
# =================================================================
os.makedirs(os.path.dirname(DB_URL.split("///")[-1]), exist_ok=True)
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class QuestionAnswer(Base):
    __tablename__ = "processed_questions"
    id = Column(Integer, primary_key=True, index=True)
    question = Column(String, unique=True, index=True)
    answer = Column(String)
    score = Column(Float, index=True)

def create_db_and_tables():
    logger.info("BDD: Verificando y creando tablas si es necesario.")
    Base.metadata.create_all(bind=engine)
    logger.info("BDD: Tablas listas y operativas.")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# SECCIÓN 2: Consumidor de Kafka (Sin cambios)
# =================================================================
def consume_results_from_kafka():
    logger.info("CONSUMIDOR_BDD: Iniciando hilo consumidor de Kafka.")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC_CONSUME_VALIDATED,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id='storage_consumer_group',
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )
            logger.info(f"CONSUMIDOR_BDD: Conectado. Escuchando {TOPIC_CONSUME_VALIDATED}.")
            # ... (el resto de tu lógica de consumidor no cambia) ...
            for message in consumer:
                try:
                    data = message.value
                    if not all(k in data for k in ['question', 'answer', 'score']):
                        continue
                    db = SessionLocal()
                    try:
                        existing_qa = db.query(QuestionAnswer).filter(QuestionAnswer.question == data['question']).first()
                        if existing_qa:
                            existing_qa.answer = data['answer']
                            existing_qa.score = data['score']
                            logger.info(f"CONSUMIDOR_BDD: Actualizando entrada existente: {data['question'][:20]}...")
                        else:
                            new_qa = QuestionAnswer(
                                question=data['question'],
                                answer=data['answer'],
                                score=data['score']
                            )
                            db.add(new_qa)
                            logger.info(f"CONSUMIDOR_BDD: Guardando nueva Q&A: {data['question'][:20]}...")
                        db.commit()
                    except Exception as e:
                        db.rollback()
                    finally:
                        db.close()
                except Exception as e:
                    logger.error(f"CONSUMIDOR_BDD: Error en bucle principal: {e}")

        except Exception as e:
            logger.error(f"CONSUMIDOR_BDD: Error de conexión a Kafka: {e}. Reintentando en 5s.")
            time.sleep(5)

# =================================================================
# SECCIÓN 3: API Web (¡Aquí están los cambios!)
# =================================================================

def create_kafka_producer():
    """Función para conectar el productor de Kafka con reintentos."""
    global kafka_producer
    while True:
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("PRODUCTOR_BDD: Productor de Kafka conectado.")
            return
        except NoBrokersAvailable:
            logger.warning("PRODUCTOR_BDD: No se pudo conectar. Reintentando en 5s...")
            time.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API: Iniciando lifespan (startup)...")
    # 1. Crea las tablas
    create_db_and_tables()
    # 2. Inicia el consumidor en un hilo
    consumer_thread = threading.Thread(target=consume_results_from_kafka, daemon=True)
    consumer_thread.start()
    # 3. Inicia el productor en un hilo (solo para la conexión)
    producer_thread = threading.Thread(target=create_kafka_producer, daemon=True)
    producer_thread.start()
    
    yield
    
    logger.info("API: Cerrando lifespan (shutdown)...")
    if kafka_producer:
        kafka_producer.close()

app = FastAPI(title="Servicio de Almacenamiento y Gateway", lifespan=lifespan)

# Modelo Pydantic para el POST
class QuestionRequest(BaseModel):
    question: str

@app.post("/process_question")
async def process_question(request: QuestionRequest, db: Session = Depends(get_db)):
    """
    Este es el endpoint principal (gateway).
    1. Revisa si la pregunta está en la BDD (caché).
    2. Si está, la devuelve (200 OK).
    3. Si no está, la envía a Kafka y devuelve (202 Accepted).
    """
    global kafka_producer
    question_text = request.question.strip()

    try:
        # 1. Consultar el caché (BDD)
        result = db.query(QuestionAnswer).filter(QuestionAnswer.question == question_text).first()
        
        if result and result.score is not None:
            # 2. CACHE HIT: Encontrada. Devolver 200 OK.
            logger.info(f"GATEWAY_API: [CACHE HIT] Pregunta encontrada en BDD. {question_text[:30]}...")
            return {
                "status": "cache_hit",
                "processed": True,
                "question": result.question,
                "answer": result.answer,
                "score": result.score
            }
        else:
            # 3. CACHE MISS: No encontrada. Enviar a Kafka.
            logger.info(f"GATEWAY_API: [CACHE MISS] Pregunta no encontrada. Encolando en Kafka. {question_text[:30]}...")
            
            if not kafka_producer:
                logger.error("GATEWAY_API: Productor de Kafka NO está listo. Imposible encolar.")
                raise HTTPException(status_code=503, detail="Servicio de encolado no disponible")

            message = {
                "id": str(uuid.uuid4()),
                "question": question_text,
                "timestamp": time.time(),
                "attempt": 1,
                "regens": 0
            }
            
            # Enviar al tópico del LLM
            kafka_producer.send(TOPIC_PRODUCE_NEW, message)
            kafka_producer.flush()
            
            # Devolver 202 Accepted
            return JSONResponse(
                status_code=202, 
                content={"status": "queued", "id": message["id"]}
            )

    except OperationalError as e:
        logger.error(f"GATEWAY_API: Error de BDD en /process_question: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        logger.error(f"GATEWAY_API: Error inesperado en /process_question: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Dejamos los otros endpoints por si los quieres usar para depurar
@app.get("/qa")
async def get_all_questions(limit: int = 50, db: Session = Depends(get_db)):
    return db.query(QuestionAnswer).limit(limit).all()

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "storage-gateway"}