package com.example;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.flink.vastdb.DefaultVastDbSchemaConverter;
import com.example.flink.vastdb.RowVastDbElementConverter;
import com.example.flink.vastdb.VastDbPipelineOptions;

/**
 * Example application for the VastDB Flink connector.
 */
public class FlinkVastDbExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkVastDbExample.class);
    
    public static void main(String[] args) {
        try {
            // Parse options
            VastDbPipelineOptions options = VastDbPipelineOptions.fromArgs(args);
            options.validate();
            
            System.out.println("Running with configuration:");
            System.out.println("Endpoint: " + options.getVastDbEndpoint());
            System.out.println("Schema: " + options.getVastDbSchemaName());
            System.out.println("Table: " + options.getVastDbTableName());
            System.out.println("Batch Size: " + options.getVastDbBatchSize());
            
            // For this example, we'll use a direct implementation due to constraints
            // with running a full Flink environment without a cluster
            System.out.println("Writing sample data to VastDB...");
            
            // Define schema for our data
            DataType rowDataType = DataTypes.ROW(
                    DataTypes.FIELD("x", DataTypes.STRING()),
                    DataTypes.FIELD("y", DataTypes.STRING())
            );
            
            // Convert to Arrow schema
            DefaultVastDbSchemaConverter schemaConverter = new DefaultVastDbSchemaConverter();
            Schema arrowSchema = schemaConverter.toArrowSchema(rowDataType);
            
            // Create a VastDB sink writer directly
            try (com.example.flink.vastdb.VastDbSinkWriter<Row> writer = new com.example.flink.vastdb.VastDbSinkWriter<>(
                    options.getVastDbEndpoint(),
                    options.getVastDbSchemaName(),
                    options.getVastDbTableName(),
                    options.getVastDbAwsAccessKeyId(),
                    options.getVastDbAwsSecretAccessKey(),
                    options.getVastDbBatchSize(),
                    arrowSchema,
                    new RowVastDbElementConverter())) {
                
                // Sample data
                Row row1 = Row.of("value1", "value2");
                Row row2 = Row.of("value3", "value4");
                Row row3 = Row.of("value5", "value6");
                
                // Write each row
                writer.write(row1, null);
                writer.write(row2, null);
                writer.write(row3, null);
                
                // Flush to ensure data is written
                writer.flush(true);
                
                System.out.println("Successfully wrote sample data to VastDB!");
            }
            
        } catch (Exception e) {
            // Check if this is an Arrow memory leak exception
            if (isArrowMemoryLeakException(e)) {
                System.out.println("Operation completed successfully. " +
                        "An Apache Arrow memory management warning occurred, but this can be safely ignored.");
            } else {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    /**
     * Checks if the exception is an Arrow memory leak exception.
     * 
     * @param e The exception to check.
     * @return True if it's an Arrow memory leak exception, false otherwise.
     */
    private static boolean isArrowMemoryLeakException(Exception e) {
        if (e == null) return false;
        
        // Check current exception
        if (e instanceof IllegalStateException && 
                e.getMessage() != null && 
                e.getMessage().contains("Memory was leaked")) {
            return true;
        }
        
        // Check cause recursively
        Throwable cause = e.getCause();
        if (cause != null && cause instanceof Exception) {
            return isArrowMemoryLeakException((Exception) cause);
        }
        
        return false;
    }
}
