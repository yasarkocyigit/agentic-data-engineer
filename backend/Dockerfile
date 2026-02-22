# ─── OpenClaw FastAPI Backend ───
FROM apache/spark:4.0.1 AS spark-runtime
FROM python:3.12-slim

WORKDIR /app

# Keep PySpark driver on same JDK major as Spark master/worker containers.
COPY --from=spark-runtime /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Docker CLI and shell tools are required for infrastructure terminal commands.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker-cli curl bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY routers/ routers/

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
