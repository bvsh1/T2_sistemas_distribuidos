import os
import time
import subprocess

def check_hdfs_ready():
    """Espera a que HDFS salga del modo seguro."""
    print("Verificando estado de HDFS...")
    hdfs_cmd = ["hdfs", "dfsadmin", "-safemode", "get"]
    
    safemode_on = True
    while safemode_on:
        try:
            # Damos un timeout al comando por si se queda pegado
            result = subprocess.run(hdfs_cmd, capture_output=True, text=True, check=True, timeout=30)
            if "Safe mode is OFF" in result.stdout:
                safemode_on = False
                print("HDFS está listo.")
            else:
                print(f"Estado HDFS: {result.stdout.strip()}. Esperando 10s...")
                time.sleep(10)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            print(f"Error al verificar HDFS (probablemente aún iniciando), reintentando: {e}")
            time.sleep(10)

def ejecutar_comando(comando):
    """Ejecuta un comando en el shell e imprime su salida."""
    print(f"\n[EJECUTANDO]: {' '.join(comando)}")
    try:
        result = subprocess.run(comando, capture_output=True, text=True, check=True)
        print("[STDOUT]:", result.stdout)
        print("[STDERR]:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error ejecutando el comando: {e}\n[STDOUT]: {e.stdout}\n[STDERR]: {e.stderr}")
        raise

def main():
    
    # --- ¡¡AQUÍ ESTÁ LA CORRECCIÓN!! ---
    # Damos 30 segundos de gracia a los servicios de Hadoop para que inicien.
    # El 'namenode' es lento para arrancar y estar listo para recibir conexiones.
    print("--- Iniciando script de análisis batch ---")
    wait_time = 30
    print(f"Dando {wait_time} segundos de gracia a los servicios de Hadoop (namenode/datanode) para que inicien...")
    time.sleep(wait_time)
    print("Tiempo de gracia terminado. Intentando conectar con HDFS...")
    # --- FIN DE LA CORRECCIÓN ---

    # Paso 1: Esperar que HDFS esté listo
    check_hdfs_ready()
    
    # Paso 2: Subir datos a HDFS (desde la ruta donde se montaron)
    print("Subiendo archivos de datos a HDFS...")
    ejecutar_comando(["hdfs", "dfs", "-mkdir", "-p", "/wordcount/input"])
    ejecutar_comando(["hdfs", "dfs", "-put", "-f", "/app/data/human_answers.txt", "/wordcount/input/human_answers.txt"])
    ejecutar_comando(["hdfs", "dfs", "-put", "-f", "/app/data/llm_answers.txt", "/wordcount/input/llm_answers.txt"])
    
    # Paso 3: Ejecutar análisis de Pig para respuestas humanas
    print("\n--- Iniciando Análisis Pig para Respuestas Humanas ---")
    ejecutar_comando([
        "pig",
        "-param", "INPUT_PATH=/wordcount/input/human_answers.txt",
        "-param", "OUTPUT_PATH=/wordcount/output/humanas",
        "wordcount.pig"
    ])
    
    # Paso 4: Ejecutar análisis de Pig para respuestas del LLM
    print("\n--- Iniciando Análisis Pig para Respuestas del LLM ---")
    ejecutar_comando([
        "pig",
        "-param", "INPUT_PATH=/wordcount/input/llm_answers.txt",
        "-param", "OUTPUT_PATH=/wordcount/output/llm",
        "wordcount.pig"
    ])

    # Paso 5: Descargar resultados de HDFS
    print("\nDescargando resultados desde HDFS...")
    os.makedirs("results", exist_ok=True)
    ejecutar_comando(["hdfs", "dfs", "-get", "/wordcount/output/humanas/part-r-00000", "results/humanas_wordcount.txt"])
    ejecutar_comando(["hdfs", "dfs", "-get", "/wordcount/output/llm/part-r-00000", "results/llm_wordcount.txt"])

    print("\n--- ANÁLISIS COMPARATIVO COMPLETADO ---")
    print("Resultados descargados a la carpeta 'results/'.")

if __name__ == "__main__":
    main()