#!/usr/bin/env bash

# This script is for running the Flink VastDB example application in a local Flink mini cluster

# Check if the required JVM module option is already in FLINK_JVM_OPTIONS
if [[ ! "$FLINK_JVM_OPTIONS" =~ "--add-opens=java.base/java.nio=ALL-UNNAMED" ]]; then
  export FLINK_JVM_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED $FLINK_JVM_OPTIONS"
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

# Run the Flink application with the required Arrow memory access options
echo "Running Flink VastDB example with parameters:"
echo "Endpoint: $ENDPOINT"
echo "Schema: $SCHEMA_NAME"
echo "Table: $TABLE_NAME"
echo "Batch Size: $BATCH_SIZE"
echo "JVM Options: $FLINK_JVM_OPTIONS"

java $FLINK_JVM_OPTIONS \
     -jar target/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
     --vastDbEndpoint="$ENDPOINT" \
     --vastDbSchemaName="$SCHEMA_NAME" \
     --vastDbTableName="$TABLE_NAME" \
     --vastDbBatchSize="$BATCH_SIZE"
