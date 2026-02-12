#!/bin/bash
set -e

echo "============================================="
echo "  Agentic Data Engineer - Setup Script"
echo "============================================="

# Create jars directory
mkdir -p jars

echo ""
echo "📦 Downloading required JARs..."
echo ""

# Iceberg Spark Runtime
if [ ! -f "jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar" ]; then
    echo "⬇️  Downloading Iceberg Spark Runtime..."
    curl -fSL -o jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
else
    echo "✅ Iceberg Spark Runtime already exists."
fi

# AWS SDK Bundle (for MinIO/S3A)
if [ ! -f "jars/bundle-2.31.1.jar" ]; then
    echo "⬇️  Downloading AWS SDK Bundle (this may take a minute)..."
    curl -fSL -o jars/bundle-2.31.1.jar \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.1/bundle-2.31.1.jar"
else
    echo "✅ AWS SDK Bundle already exists."
fi

# Hadoop AWS
if [ ! -f "jars/hadoop-aws-3.4.1.jar" ]; then
    echo "⬇️  Downloading Hadoop AWS..."
    curl -fSL -o jars/hadoop-aws-3.4.1.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar"
else
    echo "✅ Hadoop AWS already exists."
fi

# PostgreSQL JDBC Driver
if [ ! -f "jars/postgresql-42.7.5.jar" ]; then
    echo "⬇️  Downloading PostgreSQL JDBC Driver..."
    curl -fSL -o jars/postgresql-42.7.5.jar \
        "https://jdbc.postgresql.org/download/postgresql-42.7.5.jar"
else
    echo "✅ PostgreSQL JDBC Driver already exists."
fi

echo ""
echo "============================================="
echo "  ✅ Setup Complete!"
echo "============================================="
echo ""
echo "Next steps:"
echo "  1. docker compose up -d"
echo "  2. Open Airflow: http://localhost:8080 (admin/admin)"
echo "  3. Open MinIO:   http://localhost:9001 (admin/admin123)"
echo "  4. Open Trino:   http://localhost:8090"
echo "  5. Open Marquez: http://localhost:3000"
echo ""
