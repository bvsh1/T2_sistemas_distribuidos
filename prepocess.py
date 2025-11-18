import pandas as pd
import os

# --- Configuración ---
# 1. Cambia esto al nombre de tu archivo de dataset
INPUT_FILE = 'data/interactions.csv' 

# 2. Nombres de los archivos de salida (puedes dejarlos así)
OUTPUT_HUMAN_FILE = 'human_answers.txt'
OUTPUT_LLM_FILE = 'llm_answers.txt'

# 3. Columnas que tu dataset DEBE tener
COL_HUMAN = 'reference_answer'
COL_LLM = 'llm_answer'

# 4. Cantidad de muestras a extraer
SAMPLE_SIZE = 10000
# ---------------------

def preprocess_data(input_path, output_human_path, output_llm_path):
    print(f"Cargando dataset desde '{input_path}'...")
    
    # Columnas que necesitamos cargar para cumplir la tarea
    columns_to_load = [COL_HUMAN, COL_LLM]

    try:
        # Cargar solo las columnas que necesitamos para ahorrar memoria
        df = pd.read_csv(input_path, usecols=columns_to_load)
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo '{input_file}'.")
        print("Por favor, pon este script en la misma carpeta que tu dataset o")
        print("actualiza la variable 'INPUT_FILE' en el script.")
        return
    except ValueError as e:
        print(f"Error: ¿Estás seguro que las columnas {columns_to_load} existen?")
        print(f"Detalle: {e}")
        return

    print(f"Dataset cargado. Total de filas: {len(df)}")

    # 1. Limpieza: Eliminar filas donde falte alguna de las dos respuestas
    df_cleaned = df.dropna(subset=columns_to_load)
    print(f"Filas después de eliminar nulos: {len(df_cleaned)}")

    # 2. Muestreo: Tomar 10k filas al azar
    if len(df_cleaned) > SAMPLE_SIZE:
        print(f"Tomando una muestra aleatoria de {SAMPLE_SIZE} filas...")
        # random_state=42 asegura que siempre obtengas la misma muestra
        # (puedes quitarlo si quieres una muestra diferente cada vez)
        df_sample = df_cleaned.sample(n=SAMPLE_SIZE, random_state=42)
    else:
        print(f"El dataset tiene menos de {SAMPLE_SIZE} filas, usando todas.")
        df_sample = df_cleaned

    # 3. Extracción y guardado para 'reference_answer' (Humanas)
    print(f"Guardando respuestas humanas en '{output_human_path}'...")
    with open(output_human_path, 'w', encoding='utf-8') as f:
        for answer in df_sample[COL_HUMAN]:
            # Reemplazamos saltos de línea para que Pig procese una respuesta por línea
            cleaned_answer = str(answer).replace('\n', ' ').replace('\r', ' ')
            f.write(cleaned_answer + '\n')

    # 4. Extracción y guardado para 'llm_answer' (LLM)
    print(f"Guardando respuestas del LLM en '{output_llm_path}'...")
    with open(output_llm_path, 'w', encoding='utf-8') as f:
        for answer in df_sample[COL_LLM]:
            cleaned_answer = str(answer).replace('\n', ' ').replace('\r', ' ')
            f.write(cleaned_answer + '\n')

    print("\n¡Proceso completado!")
    print(f"Archivos listos para HDFS: '{output_human_path}' y '{output_llm_path}'.")

if __name__ == "__main__":
    preprocess_data(INPUT_FILE, OUTPUT_HUMAN_FILE, OUTPUT_LLM_FILE)