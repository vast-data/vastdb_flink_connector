package com.example.flink.vastdb;

import com.vastdata.vdb.sdk.VastSdk;
import com.vastdata.vdb.sdk.Table;
import com.vastdata.vdb.sdk.VastSdkConfig;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * VastDbSink - A Flink sink for writing data to VastDB.
 * 
 * This sink uses Apache Arrow as the intermediate representation format to efficiently
 * transfer data between Flink and VastDB.
 * 
 * @param <IN> The input type to the sink.
 */
public class VastDbSink<IN> implements Sink<IN>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(VastDbSink.class);
    
    // These fields need to be serializable
    private final String endpoint;
    private final String schemaName;
    private final String tableName;
    private final String awsAccessKeyId;
    private final String awsSecretAccessKey;
    private final int batchSize;
    
    // Store serializable schema definition instead of Arrow schema objects
    private final SerializableSchema serializableSchema;
    
    // Use class names instead of actual instances for non-serializable components
    private final String elementConverterClassName;

    private VastDbSink(
            String endpoint,
            String schemaName,
            String tableName,
            String awsAccessKeyId,
            String awsSecretAccessKey,
            int batchSize,
            SerializableSchema serializableSchema,
            String elementConverterClassName) {
        this.endpoint = endpoint;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.awsAccessKeyId = awsAccessKeyId;
        this.awsSecretAccessKey = awsSecretAccessKey;
        this.batchSize = batchSize;
        this.serializableSchema = serializableSchema;
        this.elementConverterClassName = elementConverterClassName;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext context) throws IOException {
        LOG.info("Creating VastDbSinkWriter for table: {}/{}", schemaName, tableName);
        
        try {
            // Recreate the Schema from serializable definition
            Schema arrowSchema = serializableSchema.toArrowSchema();
            
            // Create element converter from class name
            @SuppressWarnings("unchecked")
            VastDbElementConverter<IN> elementConverter = 
                    (VastDbElementConverter<IN>) Class.forName(elementConverterClassName).newInstance();
            
            return new VastDbSinkWriter<>(
                    endpoint,
                    schemaName,
                    tableName,
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    batchSize,
                    arrowSchema,
                    elementConverter);
        } catch (Exception e) {
            LOG.error("Failed to create VastDbSinkWriter", e);
            throw new IOException("Failed to create VastDbSinkWriter", e);
        }
    }

    /**
     * A builder for creating VastDbSink instances.
     * @param <IN> The input type to the sink.
     */
    public static class Builder<IN> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private String endpoint;
        private String schemaName;
        private String tableName;
        private String awsAccessKeyId;
        private String awsSecretAccessKey;
        private int batchSize = 100; // Default batch size
        private Schema arrowSchema;
        private VastDbElementConverter<IN> elementConverter;

        public Builder<IN> withEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<IN> withCredentials(String awsAccessKeyId, String awsSecretAccessKey) {
            this.awsAccessKeyId = awsAccessKeyId;
            this.awsSecretAccessKey = awsSecretAccessKey;
            return this;
        }

        public Builder<IN> withSchema(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder<IN> withTable(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<IN> withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder<IN> withArrowSchema(Schema arrowSchema) {
            this.arrowSchema = arrowSchema;
            return this;
        }

        public Builder<IN> withElementConverter(VastDbElementConverter<IN> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }

        public VastDbSink<IN> build() {
            if (endpoint == null) {
                throw new IllegalArgumentException("VastDB endpoint must be provided");
            }
            if (schemaName == null) {
                throw new IllegalArgumentException("VastDB schema name must be provided");
            }
            if (tableName == null) {
                throw new IllegalArgumentException("VastDB table name must be provided");
            }
            if (awsAccessKeyId == null || awsSecretAccessKey == null) {
                throw new IllegalArgumentException("AWS credentials must be provided");
            }
            if (arrowSchema == null) {
                throw new IllegalArgumentException("Arrow schema must be provided");
            }
            if (elementConverter == null) {
                throw new IllegalArgumentException("Element converter must be provided");
            }

            // Convert Arrow schema to serializable representation
            SerializableSchema serializableSchema = SerializableSchema.fromArrowSchema(arrowSchema);
            
            // Store class name instead of actual instance
            String elementConverterClassName = elementConverter.getClass().getName();

            return new VastDbSink<>(
                    endpoint,
                    schemaName,
                    tableName,
                    awsAccessKeyId,
                    awsSecretAccessKey,
                    batchSize,
                    serializableSchema,
                    elementConverterClassName
            );
        }
    }

    /**
     * Creates a builder for the VastDbSink.
     * @param <IN> The input type to the sink.
     * @return A new builder instance.
     */
    public static <IN> Builder<IN> builder() {
        return new Builder<>();
    }
}
