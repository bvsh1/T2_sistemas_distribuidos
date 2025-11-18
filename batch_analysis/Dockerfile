# 1. Empezamos desde una imagen base de Ubuntu 22.04 (LTS), que siempre existe
FROM ubuntu:22.04

# 2. Evitar que 'apt' haga preguntas interactivas
ENV DEBIAN_FRONTEND=noninteractive

# 3. Instalar todas las dependencias de una vez
#    Aquí instalamos Java (JDK), Python, pip y wget
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    python3 \
    python3-pip \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# 4. Configurar JAVA_HOME (necesario para Hadoop y Pig)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# 5. Configurar las variables de entorno para Pig y Hadoop
ENV PIG_HOME="/opt/pig"
ENV HADOOP_HOME="/opt/hadoop"
# Añadimos JAVA_HOME/bin al PATH también
ENV PATH="$PIG_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH:$JAVA_HOME/bin"

# 6. Instalar Hadoop (esta parte es idéntica a antes)
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz -O /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm /tmp/hadoop.tar.gz

# 7. Instalar Pig (esta parte es idéntica a antes)
RUN wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz -O /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz -C /opt \
    && mv /opt/pig-0.17.0 /opt/pig \
    && rm /tmp/pig.tar.gz

# 8. Copiar el archivo de configuración de Hadoop
COPY hadoop-config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

# 9. Copiar el resto de la aplicación
WORKDIR /app
COPY . .

# 10. Definir el comando a ejecutar
CMD ["python3", "run_analysis.py"]