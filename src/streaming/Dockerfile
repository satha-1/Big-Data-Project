FROM python:3.11-slim

# Install Java 11 (required for Spark)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    curl \
    netcat-openbsd \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Spark 3.4.0
ENV SPARK_VERSION=3.4.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN mkdir -p /opt \
  && cd /opt \
  && curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz \
  && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
  && chmod -R 755 /opt/spark

# Create directory for Spark apps
RUN mkdir -p /opt/spark-apps && chmod 777 /opt/spark-apps

WORKDIR /opt/spark

