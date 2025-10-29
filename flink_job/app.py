import os
import re
import json
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import RichMapFunction # <-- 1. Importar RichMapFunction
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy

# --- 2. Importar SentenceTransformers ---
from sentence_transformers import SentenceTransformer, util

# =================================================================
# Configuración
# =================================================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("flink_job_semantic")

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP_ID = "flink_scoring_group"

TOPIC_INPUT = os.getenv("TOPIC_INPUT", "questions.answers")
TOPIC_OUTPUT_VALIDATED = os.getenv("TOPIC_OUTPUT_VALIDATED", "questions.validated")
TOPIC_OUTPUT_REGENERATE = os.getenv("TOPIC_OUTPUT_REGENERATE", "questions.llm")

# NOTA: Este umbral (0.62) estaba pensado para la heurística.
# Para similitud semántica, quizás quieras un valor diferente (ej. 0.5 o 0.7).
SCORE_THRESHOLD = float(os.getenv("SCORE_THRESHOLD", 0.62))
MAX_REGENERATIONS = int(os.getenv("MAX_REGENERATIONS", 3))

logger.info(f"Iniciando Flink Job (Semantic Scoring)...")
logger.info(f"  Consumiendo de: {TOPIC_INPUT}")
logger.info(f"  Publicando (OK) en: {TOPIC_OUTPUT_VALIDATED}")
logger.info(f"  Publicando (Regen) en: {TOPIC_OUTPUT_REGENERATE}")
logger.info(f"  Umbral de Score: {SCORE_THRESHOLD}")
logger.info(f"  Máx. Regeneraciones: {MAX_REGENERATIONS}")

# --- 3. Eliminadas las funciones heurísticas (calculate_score, normalize_text) y listas (STOP_WORDS, BAD_PHRASES) ---


# =================================================================
# Lógica de Flink (Con Scoring Semántico)
# =================================================================

class Scorer(RichMapFunction):
    """
    Parsea el JSON, calcula el score usando similitud semántica y devuelve el dict.
    Usa RichMapFunction para cargar el modelo de ML una sola vez.
    """
    
    def __init__(self, model_name='paraphrase-multilingual-MiniLM-L12-v2'):
        self.model_name = model_name
        self.model = None

    def open(self, runtime_context):
        """
        Método 'open': Se llama una vez cuando se inicializa el task manager.
        Ideal para cargar modelos de ML pesados.
        """
        logger.info(f"Cargando modelo de SentenceTransformer: {self.model_name}...")
        try:
            # El modelo se descargará y cacheará la primera vez
            self.model = SentenceTransformer(self.model_name)
            logger.info("Modelo cargado exitosamente.")
        except Exception as e:
            logger.error(f"Error fatal al cargar el modelo de ML: {e}")
            # Si el modelo no carga, el contenedor fallará, lo cual es correcto.
            raise e

    def map(self, json_string: str) -> dict:
        """
        Procesa cada mensaje del stream de Kafka.
        """
        try:
            message = json.loads(json_string)
            question = message.get("question", "")
            answer = message.get("answer", "")
            
            # Validar que tengamos texto y que el modelo esté cargado
            if not self.model or not question or not answer:
                logger.warning(f"Mensaje (ID: {message.get('id', 'N/A')}) no tiene pregunta/respuesta o el modelo no cargó.")
                message['score'] = 0.0
                return message

            # --- Lógica de Scoring Semántico ---
            # 1. Generar embeddings (vectores)
            q_embedding = self.model.encode(question, convert_to_tensor=True)
            a_embedding = self.model.encode(answer, convert_to_tensor=True)
            
            # 2. Calcular Similitud Coseno
            similarity = util.cos_sim(q_embedding, a_embedding)
            
            # 3. Extraer el score (float)
            score = float(similarity[0][0])
            # ------------------------------------

            message['score'] = score
            logger.info(f"Scored (Semantic) (ID: {message.get('id', 'N/A')}, Score: {score:.2f})")
            return message

        except Exception as e:
            logger.error(f"Error parseando o puntuando (semántico) JSON: {e} - String: {json_string[:100]}")
            return {"error": str(e), "original_string": json_string}


class PrepareForPersistence(object):
    """
    Formatea el dict para guardarlo en la BDD y lo serializa a JSON string.
    (Sin cambios)
    """
    def __call__(self, message: dict) -> str:
        if "error" in message:
            return json.dumps({})
        output = {
            "id": message.get("id"),
            "question": message.get("question"),
            "answer": message.get("answer"),
            "score": message.get("score")
        }
        return json.dumps(output, ensure_ascii=False)

class PrepareForRegeneration(object):
    """
    Formatea el dict para enviarlo de vuelta al LLM y lo serializa a JSON string.
    (Sin cambios)
    """
    def __call__(self, message: dict) -> str:
        if "error" in message:
             return json.dumps({})

        original_msg = message.get("original_message", {})
        
        regens = original_msg.get("regens", 0) + 1
        original_msg["regens"] = regens
        original_msg["last_score"] = message.get("score")
        
        logger.warning(f"Regenerando (ID: {message.get('id')}, Score: {message.get('score'):.2f}, Intento: {regens})")
        return json.dumps(original_msg, ensure_ascii=False)

# =================================================================
# Job Principal de PyFlink (Sin cambios en la lógica del job)
# =================================================================

def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_topics(TOPIC_INPUT) \
        .set_group_id(CONSUMER_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    wm_strategy = WatermarkStrategy.no_watermarks()
    stream = env.from_source(kafka_source, wm_strategy, "Kafka Source")

    # --- Aquí se usa la nueva clase Scorer ---
    # Flink instanciará Scorer() y llamará a .open() y luego a .map()
    # Usamos un modelo multilingüe ligero
    scored_stream = stream.map(Scorer(model_name='paraphrase-multilingual-MiniLM-L12-v2'), 
                               output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTE_ARRAY()))

    # 5. Dividir el Stream (Routing)
    high_score_stream = scored_stream.filter(lambda msg: "error" not in msg and msg.get("score", 0.0) >= SCORE_THRESHOLD)
    low_score_stream = scored_stream.filter(
        lambda msg: "error" not in msg and \
                    msg.get("score", 0.0) < SCORE_THRESHOLD and \
                    msg.get("original_message", {}).get("regens", 0) < MAX_REGENERATIONS
    )

    # 6. Formatear los streams de salida
    persistence_stream = high_score_stream.map(PrepareForPersistence(), output_type=Types.STRING())
    feedback_stream = low_score_stream.map(PrepareForRegeneration(), output_type=Types.STRING())

    # 7. Definir los Sinks
    sink_persist = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUTPUT_VALIDATED)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    sink_feedback = KafkaSink.builder() \
        .set_bootstrap_servers(BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(TOPIC_OUTPUT_REGENERATE)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # 8. Conectar streams a sinks
    persistence_stream.filter(lambda s: s != '{}').sink_to(sink_persist).name("Sink to BDD")
    feedback_stream.filter(lambda s: s != '{}').sink_to(sink_feedback).name("Sink to Regenerate")

    # 9. Ejecutar
    env.execute("Flink Semantic Scoring and Feedback Job")

if __name__ == "__main__":
    run_flink_job()