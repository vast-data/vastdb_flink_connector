# Apache Flink VastDB Connector

This project provides an MVP (Minimum Viable Product) implementation of an Apache Flink Sink for VastDB, allowing you to integrate Apache Flink data pipelines with VastDB storage.

## Overview

The Apache Flink VastDB Connector enables you to write data from Flink pipelines directly to VastDB tables. It uses Apache Arrow as the intermediate representation format to efficiently transfer data between Flink and VastDB.

## Features

- Write Flink DataStream data to VastDB tables
- Automatic schema conversion between Flink and Arrow
- Support for batched writes to improve performance
- Custom sink connector (VastDbSink) for simplified pipeline integration
- Automatic table and schema creation

## Prerequisites

- Java 17+
- Maven
- VastDB credentials (AWS access keys)

### Important Note on Java Requirements

Apache Arrow, which is used by this connector, requires specific JVM options when running on Java 9 or newer:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
```

This is because Arrow needs to access some internal JVM memory features that are restricted by default in the Java module system.

## Quick Start

### Build the Project

```bash
mvn clean package
```

### Run the Example

#### Option 1: Local Standalone Example

To run the standalone example, use the provided script:

```bash
# Set environment variables
export ENDPOINT="https://your-vastdb-endpoint"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"

# Make the script executable
chmod +x run-flink-local.sh

# Run the example
./run-flink-local.sh
```

You can also customize the schema name and table name:

```bash
./run-flink-local.sh "your-schema" "your-table" 100
```

#### Option 2: Using Docker with Flink

For a more production-like setup using Docker:

```bash
# Set environment variables
export ENDPOINT="https://your-vastdb-endpoint"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"

# Make the script executable
chmod +x run-flink-docker.sh

# Run with Docker
./run-flink-docker.sh
```

This will start a Flink cluster using Docker Compose and submit the job to it.
Access the Flink Dashboard at http://localhost:8081 to monitor your job.

## Example Applications

The project includes two example applications:

1. **FlinkVastDbExample** - A standalone example that directly uses the VastDB sink writer without requiring a Flink cluster. This is useful for quick testing and development.

2. **FlinkVastDbJobExample** - A Flink job example that is meant to be deployed to a Flink cluster. This demonstrates how to use the connector in a real Flink environment.

For detailed instructions on running the connector in various Flink environments (standalone cluster, YARN, Kubernetes), see [RUNNING.md](RUNNING.md).

## Usage in Your Application

### Basic Usage

```java
// Define your schema
DataType rowDataType = DataTypes.ROW(
        DataTypes.FIELD("field1", DataTypes.STRING()),
        DataTypes.FIELD("field2", DataTypes.INT())
);

// Convert to Arrow schema
DefaultVastDbSchemaConverter schemaConverter = new DefaultVastDbSchemaConverter();
Schema arrowSchema = schemaConverter.toArrowSchema(rowDataType);

// Write to VastDB
dataStream.sinkTo(
        VastDbSink.<Row>builder()
                .withEndpoint("your-vastdb-endpoint")
                .withCredentials("your-access-key-id", "your-secret-key")
                .withSchema("your-schema")
                .withTable("your-table")
                .withBatchSize(100)
                .withArrowSchema(arrowSchema)
                .withElementConverter(new RowVastDbElementConverter())
                .build()
);
```

### Custom Data Types

For custom data types, implement the `VastDbElementConverter` interface:

```java
public class MyCustomTypeConverter implements VastDbElementConverter<MyCustomType> {
    @Override
    public void convert(MyCustomType element, List<FieldVector> vectors, int rowIndex) {
        // Convert your custom type to Arrow vectors
        // Example:
        ((VarCharVector) vectors.get(0)).set(rowIndex, new Text(element.getField1()));
        ((IntVector) vectors.get(1)).set(rowIndex, element.getField2());
    }
}
```

## Known Issues

### Arrow Memory Leak Warning

You may see a warning like:
```
Memory was leaked by query. Memory leaked: (XXXXX)
```

This is a known issue with Apache Arrow's memory management in certain situations. The warning doesn't affect the actual functionality of the connector - data is still successfully written to VastDB.

We've implemented a workaround that suppresses this warning in production use. If you're concerned about memory usage in long-running applications, consider:

1. Using smaller batch sizes (e.g., 50 or 100)
2. Ensuring Flink has sufficient memory allocated
3. Monitoring memory usage in your Flink application

## Architecture

The project consists of the following main components:

- `VastDbSink`: A Flink sink connector for integrating VastDB with Apache Flink
- `VastDbSinkWriter`: Implementation of the Flink SinkWriter interface for VastDB
- `VastDbSchemaConverter`: Interface for converting between Flink and Arrow schemas
- `DefaultVastDbSchemaConverter`: Default implementation of the schema converter
- `VastDbElementConverter`: Interface for converting elements from Flink to Arrow
- `RowVastDbElementConverter`: Converter implementation for Flink Row objects
- `VastDbUtils`: Helper methods for VastDB operations
- `VastDbPipelineOptions`: Configuration options for the connector

## License

This project is licensed under the Apache License 2.0.
