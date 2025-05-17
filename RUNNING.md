# Running the VastDB Flink Connector

This document provides detailed instructions for running the VastDB Flink Connector in different Flink environments.

## Running in Different Flink Environments

### 1. Local Development (Standalone Mode)

For development and testing, use the provided script:

```bash
# Set environment variables
export ENDPOINT="http://172.200.204.2:80"
export DB_SCHEMA="csnow-db/flink_schema"
export DB_TABLE="flink_table"
export AWS_ACCESS_KEY_ID="Y5101AQQTB1PUAEKQXN5"
export AWS_SECRET_ACCESS_KEY="bsqwYOcsvfXxsvtTYruCT24c3w1E1Y8iBpmyoLGr"

# Run the example
./run-flink-local.sh
```

This uses the standalone example implementation that doesn't require a Flink cluster.

### 2. Flink Standalone Cluster

For a Flink standalone cluster, follow these steps:

1. Start your Flink cluster with the required JVM options for Arrow:

   Edit `conf/flink-conf.yaml` to add:
   ```yaml
   env.java.opts: "--add-opens=java.base/java.nio=ALL-UNNAMED"
   ```

2. Submit the job:
   ```bash
   $FLINK_HOME/bin/flink run \
     -c com.example.FlinkVastDbJobExample \
     target/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
     --vastDbEndpoint="$ENDPOINT" \
     --vastDbSchemaName="$DB_SCHEMA" \
     --vastDbTableName="$DB_SCHEMA" \
     --vastDbBatchSize=100
   ```

#### Using Docker for Flink Standalone Cluster

You can also use Docker to run a Flink standalone cluster:

1. Start the Flink cluster:

```bash
docker-compose up -d
```

2. Submit the job:

```bash
docker exec -it $(docker ps -q -f name=jobmanager) flink run \
  -c com.example.FlinkVastDbJobExample \
  /opt/flink/usrlib/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
  --vastDbEndpoint="$ENDPOINT" \
  --vastDbSchemaName="$DB_SCHEMA" \
  --vastDbTableName="$DB_TABLE" \
  --vastDbAwsAccessKeyId="$AWS_ACCESS_KEY_ID" \
  --vastDbAwsSecretAccessKey="$AWS_SECRET_ACCESS_KEY" \
  --vastDbBatchSize=100
```

4. Access the Flink Dashboard at http://localhost:8081

5. When done, stop the cluster:

```bash
docker-compose down
```

### 3. Flink on YARN

For YARN deployments:

```bash
$FLINK_HOME/bin/flink run -yjm 1024m -ytm 4096m \
  -yD env.java.opts="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  -c com.example.FlinkVastDbJobExample \
  target/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
  --vastDbEndpoint="$ENDPOINT" \
  --vastDbSchemaName="your-schema" \
  --vastDbTableName="your-table" \
  --vastDbBatchSize=100
```

### 4. Flink on Kubernetes

For Kubernetes deployments:

```bash
$FLINK_HOME/bin/flink run-application \
  -t kubernetes-application \
  -Dkubernetes.container.image.ref=flink:latest \
  -Dkubernetes.jobmanager.env.JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  -Dkubernetes.taskmanager.env.JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
  -c com.example.FlinkVastDbJobExample \
  target/flink-vastdb-connector-1.0.0-jar-with-dependencies.jar \
  --vastDbEndpoint="$ENDPOINT" \
  --vastDbSchemaName="your-schema" \
  --vastDbTableName="your-table" \
  --vastDbBatchSize=100
```

## Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `vastDbEndpoint` | VastDB endpoint URL | - |
| `vastDbSchemaName` | VastDB schema name | - |
| `vastDbTableName` | VastDB table name | - |
| `vastDbBatchSize` | Number of records to batch before writing | 100 |
| `vastDbAwsAccessKeyId` | AWS access key ID | From environment variable `AWS_ACCESS_KEY_ID` |
| `vastDbAwsSecretAccessKey` | AWS secret access key | From environment variable `AWS_SECRET_ACCESS_KEY` |

## Monitoring and Troubleshooting

### Monitoring

When running in a Flink cluster, you can monitor the connector's performance through Flink's monitoring interfaces:

1. **Flink Dashboard**: Check the job status, parallelism, and performance metrics
2. **Metrics**: The connector exposes metrics like number of records written, batch size, and write latency
3. **Logs**: Look for log messages with the prefix `com.example.flink.vastdb`

### Troubleshooting

#### Arrow Memory Initialization Error

If you encounter an error like:

```
Failed to initialize MemoryUtil. Was Java started with `--add-opens=java.base/java.nio=ALL-UNNAMED`?
```

Make sure you've added the required JVM option to your Flink configuration.

#### Connection Issues

If you have issues connecting to VastDB:

1. Check that the endpoint URL is correct
2. Verify your AWS credentials are valid
3. Ensure the VastDB schema and table names are correct
4. Check for network connectivity issues or firewalls

#### Memory Warnings

Arrow memory warnings like "Memory was leaked by query" are expected and can be ignored.

## Advanced Usage

### Custom Schema Conversion

To implement your own schema conversion logic, implement the `VastDbSchemaConverter` interface:

```java
public class CustomSchemaConverter implements VastDbSchemaConverter {
    @Override
    public Schema toArrowSchema(DataType dataType) {
        // Custom conversion logic
    }

    @Override
    public Schema withRowId(Schema schema) {
        // Add row ID to schema
    }
}
```

### Performance Tuning

For optimal performance:

1. **Batch Size**: Adjust the batch size based on your data volume and record size
2. **Parallelism**: For large datasets, increase the parallelism of the Flink job
3. **Memory**: Allocate sufficient memory for Flink task managers
4. **Checkpointing**: Configure appropriate checkpointing to balance durability and performance
