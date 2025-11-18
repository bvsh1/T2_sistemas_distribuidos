# 1. Empezamos desde Ubuntu 22.04
FROM ubuntu:22.04

# 2. Evitar preguntas interactivas
ENV DEBIAN_FRONTEND=noninteractive

# 3. Cambiar el mirror de Ubuntu a Chile (¡Esto funcionó!)
RUN sed -i 's/archive.ubuntu.com/cl.archive.ubuntu.com/g' /etc/apt/sources.list

# 4. Instalar dependencias (Ahora es rápido)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 5. Configurar JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# 6. Configurar variables de entorno
ENV PIG_HOME="/opt/pig"
ENV HADOOP_HOME="/opt/hadoop"
ENV PATH="$PIG_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH:$JAVA_HOME/bin"

# --- ¡¡LÍNEA 7 MODIFICADA!! ---
# 7. Instalar Hadoop (Volvemos al servidor 'archive' que SÍ tiene el archivo)
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm /tmp/hadoop.tar.gz

# --- ¡¡LÍNEA 8 MODIFICADA!! ---
# 8. Instalar Pig (Volvemos al servidor 'archive')
RUN wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz -O /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz -C /opt \
    && mv /opt/pig-0.17.0 /opt/pig \
    && rm /tmp/pig.tar.gz

# 9. Copiar el archivo de configuración de Hadoop
COPY hadoop-config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

# 10. Copiar el resto de la aplicación
WORKDIR /app
COPY . .

# 11. Definir el comando a ejecutar
CMD ["python3", "run_analysis.py"]