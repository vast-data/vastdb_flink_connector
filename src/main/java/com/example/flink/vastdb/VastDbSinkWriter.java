package com.example.flink.vastdb;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.VastSdkConfig;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;

/**
 * VastDbSinkWriter - A Flink SinkWriter implementation for VastDB.
 * 
 * This writer batches records and writes them to VastDB when the batch is full
 * or when flush/close is called.
 * 
 * @param <IN> The input type to the writer.
 */
public class VastDbSinkWriter<IN> implements SinkWriter<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbSinkWriter.class);
    
    private final String endpoint;
    private final String schemaName;
    private final String tableName;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final int batchSize;
    private final Schema arrowSchema;
    private final VastDbElementConverter<IN> elementConverter;
    
    private final Table table;
    private final VastSdk vastSdk;
    private final RootAllocator allocator;
    
    private List<IN> batch;
    private boolean tableSchemaLoaded = false;
    
    /**
     * Creates a VastDbSinkWriter.
     * 
     * @param endpoint The VastDB endpoint.
     * @param schemaName The schema name.
     * @param tableName The table name.
     * @param awsAccessKeyId The AWS access key ID.
     * @param awsSecretAccessKey The AWS secret access key.
     * @param batchSize The batch size.
     * @param arrowSchema The Arrow schema.
     * @param elementConverter The element converter.
     * @throws IOException If an error occurs during initialization.
     */
    public VastDbSinkWriter(
            String endpoint,
            String schemaName,
            String tableName,
            String awsAccessKeyId,
            String awsSecretAccessKey,
            int batchSize,
            Schema arrowSchema,
            VastDbElementConverter<IN> elementConverter) throws IOException {
        this.endpoint = endpoint;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
        this.batchSize = batchSize;
        this.arrowSchema = arrowSchema;
        this.elementConverter = elementConverter;
        
        this.batch = new ArrayList<>(batchSize);
        this.allocator = new RootAllocator();
        
        // Initialize VastSdk
        URI uri = URI.create(endpoint);
        VastSdkConfig config = new VastSdkConfig(
                uri,
                uri.toString(),
                awsAccessKeyId,
                awsSecretAccessKey);
        
        HttpClient httpClient = new JettyHttpClient();
        this.vastSdk = new VastSdk(httpClient, config);
        
        try {
            // Try to get the table and load its schema
            this.table = vastSdk.getTable(schemaName, tableName);
            
            // Initialize the table and schema if they don't exist yet
            try {
                this.table.loadSchema();
                this.tableSchemaLoaded = true;
                LOG.info("Connected to existing VastDB table: {}/{}", schemaName, tableName);
            } catch (Exception e) {
                LOG.info("Table {}/{} doesn't exist or schema couldn't be loaded: {}", 
                        schemaName, tableName, e.getMessage());
                // Table will be created on first write
            }
        } catch (Exception e) {
            throw new IOException("Failed to initialize VastDB connection", e);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        batch.add(element);
        
        if (batch.size() >= batchSize) {
            flush(false);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (batch.isEmpty()) {
            return;
        }
        
        try {
            // Create the table if it doesn't exist
            if (!tableSchemaLoaded) {
                createTableIfNotExists();
            }
            
            // Convert batch to Arrow VectorSchemaRoot
            VectorSchemaRoot root = convertBatchToArrow();
            
            // Write to VastDB
            table.put(root);
            
            // Clear the batch
            batch.clear();
            
            LOG.info("Successfully wrote batch to VastDB table: {}/{}", schemaName, tableName);
        } catch (Exception e) {
            throw new IOException("Failed to write batch to VastDB", e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (!batch.isEmpty()) {
                flush(true);
            }
        } catch (Exception e) {
            LOG.error("Error flushing batch data on close: {}", e.getMessage());
            throw e;
        } finally {
            // Attempt to close the allocator
            if (allocator != null) {
                try {
                    allocator.close();
                } catch (IllegalStateException e) {
                    // Only suppress the specific Arrow memory leak warnings
                    if (e.getMessage() != null && e.getMessage().contains("Memory was leaked")) {
                        LOG.debug("Suppressed Arrow memory leak warning: {}", e.getMessage());
                    } else {
                        throw e;
                    }
                }
            }
        }
    }
    
    private void createTableIfNotExists() {
        try {
            LOG.info("Creating table {}/{} with schema if it doesn't exist", schemaName, tableName);
            
            // Use the VastDbUtils for table creation
            VastDbUtils.createTableIfNotExists(
                    vastSdk.getVastClient(),
                    schemaName, 
                    tableName, 
                    arrowSchema);
            
            // Reload the table schema
            table.loadSchema();
            tableSchemaLoaded = true;
        } catch (Exception e) {
            LOG.error("Failed to create table: {}", e.getMessage());
            throw new RuntimeException("Failed to create table", e);
        }
    }
    
    private VectorSchemaRoot convertBatchToArrow() {
        try {
            List<FieldVector> vectors = new ArrayList<>();
            
            // Initialize vectors based on the schema
            for (Field field : arrowSchema.getFields()) {
                FieldVector vector = field.createVector(allocator);
                vectors.add(vector);
                vector.allocateNew();
            }
            
            // Set the values
            for (int rowIdx = 0; rowIdx < batch.size(); rowIdx++) {
                IN element = batch.get(rowIdx);
                elementConverter.convert(element, vectors, rowIdx);
            }
            
            // Set the value count for all vectors
            for (FieldVector vector : vectors) {
                vector.setValueCount(batch.size());
            }
            
            return new VectorSchemaRoot(arrowSchema, vectors, batch.size());
        } catch (Exception e) {
            LOG.error("Failed to convert batch to Arrow format: {}", e.getMessage());
            throw new RuntimeException("Failed to convert batch to Arrow format", e);
        }
    }
}
