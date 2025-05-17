package com.example;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.flink.vastdb.DefaultVastDbSchemaConverter;
import com.example.flink.vastdb.RowVastDbElementConverter;
import com.example.flink.vastdb.VastDbPipelineOptions;
import com.example.flink.vastdb.VastDbSink;

/**
 * Example Flink application for the VastDB connector.
 * This is meant to be run in a real Flink environment.
 */
public class FlinkVastDbJobExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkVastDbJobExample.class);
    
    public static void main(String[] args) throws Exception {
        // Parse options
        VastDbPipelineOptions options = VastDbPipelineOptions.fromArgs(args);
        
        // Explicitly check for empty strings
        if (options.getVastDbSchemaName() == null || options.getVastDbSchemaName().trim().isEmpty()) {
            throw new IllegalArgumentException("VastDB schema name must not be empty. Please provide a valid schema name with --vastDbSchemaName.");
        }
        
        if (options.getVastDbTableName() == null || options.getVastDbTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("VastDB table name must not be empty. Please provide a valid table name with --vastDbTableName.");
        }
        
        if (options.getVastDbEndpoint() == null || options.getVastDbEndpoint().trim().isEmpty()) {
            throw new IllegalArgumentException("VastDB endpoint must not be empty. Please provide a valid endpoint with --vastDbEndpoint.");
        }
        
        if (options.getVastDbAwsAccessKeyId() == null || options.getVastDbAwsAccessKeyId().trim().isEmpty()) {
            throw new IllegalArgumentException("AWS access key ID must not be empty. Please provide with --vastDbAwsAccessKeyId.");
        }
        
        if (options.getVastDbAwsSecretAccessKey() == null || options.getVastDbAwsSecretAccessKey().trim().isEmpty()) {
            throw new IllegalArgumentException("AWS secret access key must not be empty. Please provide with --vastDbAwsSecretAccessKey.");
        }
        
        // Run the standard validation
        options.validate();
        
        // Print parsed options for debugging
        System.out.println("Running with configuration:");
        System.out.println("Endpoint: " + options.getVastDbEndpoint());
        System.out.println("Schema: " + options.getVastDbSchemaName());
        System.out.println("Table: " + options.getVastDbTableName());
        System.out.println("AWS Access Key ID: " + (options.getVastDbAwsAccessKeyId() != null ? "***" : "null"));
        System.out.println("AWS Secret Key: " + (options.getVastDbAwsSecretAccessKey() != null ? "***" : "null"));
        System.out.println("Batch Size: " + options.getVastDbBatchSize());
        
        // Setup execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Define schema for our data
        DataType rowDataType = DataTypes.ROW(
                DataTypes.FIELD("x", DataTypes.STRING()),
                DataTypes.FIELD("y", DataTypes.STRING())
        );
        
        // Convert to Arrow schema
        DefaultVastDbSchemaConverter schemaConverter = new DefaultVastDbSchemaConverter();
        Schema arrowSchema = schemaConverter.toArrowSchema(rowDataType);
        
        // Define row type info for Flink
        TypeInformation<?>[] types = new TypeInformation[]{Types.STRING, Types.STRING};
        String[] names = new String[]{"x", "y"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, names);
        
        // Generate some sample data
        DataStream<Row> dataStream = env.fromElements(
                Row.of("value1", "value2"),
                Row.of("value3", "value4"),
                Row.of("value5", "value6")
        ).returns(rowTypeInfo);
        
        // Write to VastDB
        dataStream.sinkTo(
                VastDbSink.<Row>builder()
                        .withEndpoint(options.getVastDbEndpoint())
                        .withCredentials(
                                options.getVastDbAwsAccessKeyId(),
                                options.getVastDbAwsSecretAccessKey())
                        .withSchema(options.getVastDbSchemaName())
                        .withTable(options.getVastDbTableName())
                        .withBatchSize(options.getVastDbBatchSize())
                        .withArrowSchema(arrowSchema)
                        .withElementConverter(new RowVastDbElementConverter())
                        .build()
        );
        
        // Execute the job
        System.out.println("Executing Flink job...");
        env.execute("Flink VastDB Job");
    }
}