import os
import time
import subprocess

def configurar_hadoop_client():
    """
    Sobrescribe el archivo core-site.xml para asegurar que el cliente
    apunte al puerto correcto (9000) del namenode.
    """
    print("Configurando cliente Hadoop (core-site.xml)...")
    config_content = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
"""
    config_path = "/opt/hadoop/etc/hadoop/core-site.xml"
    try:
        with open(config_path, "w") as f:
            f.write(config_content)
        print(f"Configuración escrita exitosamente en {config_path}")
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo escribir la configuración automática: {e}")

def check_hdfs_ready():
    """Espera a que HDFS salga del modo seguro."""
    print("Verificando estado de HDFS...")
    hdfs_cmd = ["hdfs", "dfsadmin", "-safemode", "get"]
    
    safemode_on = True
    while safemode_on:
        try:
            result = subprocess.run(hdfs_cmd, capture_output=True, text=True, check=True, timeout=30)
            if "Safe mode is OFF" in result.stdout:
                safemode_on = False
                print("HDFS está listo.")
            else:
                print(f"Estado HDFS: {result.stdout.strip()}. Esperando 10s...")
                time.sleep(10)
        except subprocess.CalledProcessError as e:
            print(f"Error al verificar HDFS (reintentando).")
            time.sleep(10)
        except subprocess.TimeoutExpired:
            print("Timeout verificando HDFS. Reintentando...")
            time.sleep(10)

def ejecutar_comando(comando):
    """Ejecuta un comando en el shell e imprime su salida."""
    print(f"\n[EJECUTANDO]: {' '.join(comando)}")
    try:
        result = subprocess.run(comando, capture_output=True, text=True, check=True)
        print("[STDOUT]:", result.stdout)
        if result.stderr:
            print("[STDERR (Info/Warn)]:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error FATAL ejecutando el comando: {' '.join(comando)}")
        print(f"[STDERR]: {e.stderr}")
        raise

def main():
    print("--- Iniciando script de análisis batch (Versión Final) ---")
    
    configurar_hadoop_client()

    wait_time = 30
    print(f"Dando {wait_time} segundos de gracia a los servicios de Hadoop...")
    time.sleep(wait_time)
    
    check_hdfs_ready()
    
    print("Subiendo archivos de datos a HDFS...")
    try:
        ejecutar_comando(["hdfs", "dfs", "-mkdir", "-p", "/wordcount/input"])
    except:
        pass

    # Subimos los datos de texto
    ejecutar_comando(["hdfs", "dfs", "-put", "-f", "/app/data/human_answers.txt", "/wordcount/input/human_answers.txt"])
    ejecutar_comando(["hdfs", "dfs", "-put", "-f", "/app/data/llm_answers.txt", "/wordcount/input/llm_answers.txt"])
    
    # --- ¡NUEVO! Subimos también el archivo stopwords.txt ---
    # El Dockerfile ya copió este archivo a /app/stopwords.txt
    print("Subiendo stopwords a HDFS...")
    ejecutar_comando(["hdfs", "dfs", "-put", "-f", "/app/stopwords.txt", "/wordcount/stopwords.txt"])
    
    # Ejecutar análisis Pig (Humanas)
    print("\n--- Iniciando Análisis Pig para Respuestas Humanas ---")
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/wordcount/output/humanas"], capture_output=True)
    ejecutar_comando([
        "pig",
        "-param", "INPUT_PATH=/wordcount/input/human_answers.txt",
        "-param", "OUTPUT_PATH=/wordcount/output/humanas",
        "wordcount.pig"
    ])
    
    # Ejecutar análisis Pig (LLM)
    print("\n--- Iniciando Análisis Pig para Respuestas del LLM ---")
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/wordcount/output/llm"], capture_output=True)
    ejecutar_comando([
        "pig",
        "-param", "INPUT_PATH=/wordcount/input/llm_answers.txt",
        "-param", "OUTPUT_PATH=/wordcount/output/llm",
        "wordcount.pig"
    ])

    # Descargar resultados
    print("\nDescargando resultados desde HDFS...")
    os.makedirs("results", exist_ok=True)
    if os.path.exists("results/humanas_wordcount.txt"): os.remove("results/humanas_wordcount.txt")
    if os.path.exists("results/llm_wordcount.txt"): os.remove("results/llm_wordcount.txt")

    ejecutar_comando(["hdfs", "dfs", "-get", "/wordcount/output/humanas/part-r-00000", "results/humanas_wordcount.txt"])
    ejecutar_comando(["hdfs", "dfs", "-get", "/wordcount/output/llm/part-r-00000", "results/llm_wordcount.txt"])

    print("\n--- ANÁLISIS COMPARATIVO COMPLETADO ---")
    print("Resultados descargados a la carpeta 'results/'.")

if __name__ == "__main__":
    main()