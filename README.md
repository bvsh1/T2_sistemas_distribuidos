# Tarea 3 — Sistemas Distribuidos

Este repositorio contiene una solución para el trabajo práctico: procesamiento de texto y conteo de palabras en modo distribuido usando contenedores Docker y herramientas de análisis por lotes.

**Resumen rápido**
- **Preprocesamiento**: `preprocess.py` limpia y prepara los textos.
- **Análisis por lotes**: `batch_analysis/` contiene la imagen y scripts para ejecutar el análisis (por ejemplo, con Hadoop/Pig en contenedores).
- **Datos**: la carpeta `data/` tiene los archivos fuente (`human_answers.txt`, `llm_answers.txt`, `interactions.csv`).
- **Resultados**: la carpeta `results/` incluye salidas de ejemplo (`humanas_wordcount.txt`, `llm_wordcount.txt`).

**Contenido**
- `docker-compose.yml` — orquesta los servicios necesarios.
- `preprocess.py` — script local de preprocesamiento (Python).
- `batch_analysis/` — Dockerfile y scripts para el análisis por lotes.
	- `run_analysis.py` — script que se ejecuta dentro del contenedor.
	- `wordcount.pig` — script Pig para conteo de palabras.
	- `stopwords.txt` — lista de palabras a excluir.
- `data/` — datos de entrada.
- `results/` — resultados generados.

**Requisitos**
- Docker y Docker Compose (v2, plugin integrado). En Debian/Ubuntu/derivados, puede instalarse con `apt` o la guía oficial de Docker.
- Python 3.8+ (solo si se va a ejecutar `preprocess.py` localmente).

Guía de instalación mínima (Debian/Ubuntu):

```bash
# Actualizar paquetes
sudo apt update

# Instalar Docker (si no está instalado)
sudo apt install -y docker.io docker-compose-plugin

# Añadir tu usuario al grupo docker (para usar docker sin sudo)
sudo usermod -aG docker $USER
# Cierra sesión y vuelve a entrar o ejecuta: newgrp docker
```

Nota: si ves errores de permisos con el socket `/var/run/docker.sock`, asegúrate de que el daemon Docker esté corriendo y de que tu usuario pertenezca al grupo `docker`.

**Modo rápido — ejecutar con Docker Compose**

1) Construir imágenes y levantar servicios:

```bash
# Desde la raíz del repo
docker compose up --build
```

2) Levantar en segundo plano:

```bash
docker compose up -d --build
```

3) Ver logs de un servicio (ejemplo `batch_analysis`):

```bash
docker compose logs -f batch_analysis
```

4) Reconstruir una imagen concreta:

```bash
docker compose build batch_analysis
```

5) Ejecutar un comando dentro del contenedor en ejecución:

```bash
docker compose exec batch_analysis /bin/bash
```

6) Detener y eliminar contenedores:

```bash
docker compose down
```

**Ejecutar preprocesamiento localmente**

Si prefieres preprocesar los archivos fuera de Docker:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # si existe un requirements.txt
python3 preprocess.py data/human_answers.txt results/human_preprocessed.txt
```

Modifica los argumentos del script `preprocess.py` según necesites. (El repo puede no incluir `requirements.txt`; instala dependencias manualmente si lo requiere.)

**Descripción del flujo de trabajo**

1. `preprocess.py` limpia y normaliza los textos de `data/` (opcional: ejecutar localmente para debug).
2. `docker compose up --build` levanta el servicio `batch_analysis` que ejecuta `run_analysis.py`.
3. `run_analysis.py` ejecuta el job de conteo (por ejemplo, `wordcount.pig`) y escribe resultados en `results/`.

**Solución de problemas comunes**

- Error de permisos: "permission denied while trying to connect to the Docker daemon socket..."
	- Añade tu usuario al grupo `docker`: `sudo usermod -aG docker $USER` y cierra sesión/vuelve a abrir o usa `newgrp docker`.
	- Alternativa temporal: ejecutar comandos con `sudo` (ej. `sudo docker compose up`).

- Docker daemon no corriendo: `Cannot connect to the Docker daemon at unix:///var/run/docker.sock`
	- Inicia el servicio: `sudo systemctl start docker` y habilítalo: `sudo systemctl enable docker`.
	- Comprueba estado: `sudo systemctl status docker`.

- Conflicto al instalar docker-compose (.deb): error "trying to overwrite ... also in package docker-compose-plugin"
	- Solución: eliminar la versión conflictiva y mantener `docker-compose-plugin` (ej. `sudo apt remove docker-compose docker-compose-plugin` y luego `sudo apt install docker-compose-plugin`).

**Comandos útiles de verificación**

```bash
# Ver versión
docker compose version

# Ver contenedores en ejecución
docker ps

# Ver configuración del daemon y permisos
ls -l /var/run/docker.sock
```

**Dónde mirar los resultados**
- Resultados finales esperados: `results/humanas_wordcount.txt` y `results/llm_wordcount.txt`.

**Contribuir / Extender**
- Añadir nuevos datasets a `data/`.
- Añadir nuevos scripts Pig o modificar `run_analysis.py` según el pipeline de análisis.

Si quieres, puedo:
- Añadir ejemplos concretos de ejecución (con parámetros exactos) según cómo acepte `preprocess.py` y `run_analysis.py`.
- Crear un `requirements.txt` si me das las dependencias de Python que uses.

---
Fecha: 2025-11-18
