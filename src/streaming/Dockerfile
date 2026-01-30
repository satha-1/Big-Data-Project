FROM apache/spark-py:3.4.0

USER root

# Install required tools and ensure Python is available
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    netcat-openbsd \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Create directory for Spark apps
RUN mkdir -p /opt/spark-apps && chmod 777 /opt/spark-apps

USER spark

