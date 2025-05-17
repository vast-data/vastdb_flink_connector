#!/bin/bash

# This script helps run the Flink VastDB connector in a Docker-based Flink cluster

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if the JAR file exists
if [ ! -f "target/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar" ]; then
    echo "JAR file not found. Please build the project first with 'mvn clean package'."
    exit 1
fi

# Set up environment variables if not already set
if [ -z "$ENDPOINT" ]; then
  echo "ENDPOINT environment variable not set. Please set it to your VastDB endpoint."
  exit 1
fi

if [ -z "$AWS_ACCESS_KEY_ID" ]; then
  echo "AWS_ACCESS_KEY_ID environment variable not set. Please set it with your VastDB credentials."
  exit 1
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "AWS_SECRET_ACCESS_KEY environment variable not set. Please set it with your VastDB credentials."
  exit 1
fi

# Get schema and table name from command line arguments or use defaults
SCHEMA_NAME=${1:-"csnow-db/flink"}
TABLE_NAME=${2:-"my_table"}
BATCH_SIZE=${3:-"100"}

# Validate schema and table names
if [ -z "$SCHEMA_NAME" ]; then
  echo "Schema name is empty. Please provide a valid schema name."
  exit 1
fi

if [ -z "$TABLE_NAME" ]; then
  echo "Table name is empty. Please provide a valid table name."
  exit 1
fi

echo "Starting Flink cluster using Docker Compose..."
docker-compose up -d

echo "Waiting for Flink cluster to start up..."
sleep 10

echo "Submitting Flink job to the cluster..."
echo "Using schema: $SCHEMA_NAME"
echo "Using table: $TABLE_NAME"
docker exec -it $(docker ps -q -f name=jobmanager) flink run \
  -c com.example.FlinkVastDbJobExample \
  /opt/flink/usrlib/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
  --vastDbEndpoint="$ENDPOINT" \
  --vastDbSchemaName="$SCHEMA_NAME" \
  --vastDbTableName="$TABLE_NAME" \
  --vastDbAwsAccessKeyId="$AWS_ACCESS_KEY_ID" \
  --vastDbAwsSecretAccessKey="$AWS_SECRET_ACCESS_KEY" \
  --vastDbBatchSize="$BATCH_SIZE"

echo "Job submitted. You can view the Flink Dashboard at http://localhost:8081"
echo "When you're done, you can stop the cluster with: docker-compose down"