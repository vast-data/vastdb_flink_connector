package com.example.flink.vastdb;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Pipeline options for VastDB connector.
 */
public class VastDbPipelineOptions {
    
    public static final String VASTDB_ENDPOINT = "vastDbEndpoint";
    public static final String VASTDB_SCHEMA_NAME = "vastDbSchemaName";
    public static final String VASTDB_TABLE_NAME = "vastDbTableName";
    public static final String VASTDB_AWS_ACCESS_KEY_ID = "vastDbAwsAccessKeyId";
    public static final String VASTDB_AWS_SECRET_ACCESS_KEY = "vastDbAwsSecretAccessKey";
    public static final String VASTDB_BATCH_SIZE = "vastDbBatchSize";
    
    private final Map<String, String> options;
    
    /**
     * Creates a new VastDbPipelineOptions instance.
     * 
     * @param options The options map.
     */
    public VastDbPipelineOptions(Map<String, String> options) {
        this.options = options;
    }
    
    /**
     * Creates VastDbPipelineOptions from command line arguments.
     * 
     * @param args The command line arguments.
     * @return The VastDbPipelineOptions instance.
     */
    public static VastDbPipelineOptions fromArgs(String[] args) {
        // Process args manually since parameter format seems to be causing issues
        Map<String, String> options = new HashMap<>();
        
        // First parse command line arguments
        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    options.put(parts[0], parts[1]);
                    System.out.println("Parsed argument: " + parts[0] + " = " + parts[1]);
                } else {
                    System.out.println("Warning: Ignoring malformed argument: " + arg);
                }
            }
        }
        
        // Set environment variables as fallbacks if not provided in args
        setEnvIfNotPresent(options, VASTDB_AWS_ACCESS_KEY_ID, "AWS_ACCESS_KEY_ID");
        setEnvIfNotPresent(options, VASTDB_AWS_SECRET_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY");
        setEnvIfNotPresent(options, VASTDB_ENDPOINT, "ENDPOINT");
        
        return new VastDbPipelineOptions(options);
    }
    
    /**
     * Sets a value from environment variable if not present in options.
     * 
     * @param options The options map.
     * @param optionKey The option key.
     * @param envKey The environment variable key.
     */
    private static void setEnvIfNotPresent(Map<String, String> options, String optionKey, String envKey) {
        if (!options.containsKey(optionKey)) {
            String envValue = System.getenv(envKey);
            if (envValue != null && !envValue.trim().isEmpty()) {
                options.put(optionKey, envValue);
                System.out.println("Using environment variable " + envKey + " for " + optionKey);
            } else {
                System.out.println("Warning: Neither argument --" + optionKey + " nor environment variable " + 
                        envKey + " is set");
            }
        }
    }
    
    /**
     * Registers the VastDbPipelineOptions with the execution environment.
     * 
     * @param env The StreamExecutionEnvironment.
     */
    public void registerWithEnvironment(StreamExecutionEnvironment env) {
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(options));
    }
    
    /**
     * Gets the VastDB endpoint.
     * 
     * @return The VastDB endpoint.
     */
    public String getVastDbEndpoint() {
        return options.get(VASTDB_ENDPOINT);
    }
    
    /**
     * Gets the VastDB schema name.
     * 
     * @return The VastDB schema name.
     */
    public String getVastDbSchemaName() {
        return options.get(VASTDB_SCHEMA_NAME);
    }
    
    /**
     * Gets the VastDB table name.
     * 
     * @return The VastDB table name.
     */
    public String getVastDbTableName() {
        return options.get(VASTDB_TABLE_NAME);
    }
    
    /**
     * Gets the AWS access key ID.
     * 
     * @return The AWS access key ID.
     */
    public String getVastDbAwsAccessKeyId() {
        return options.get(VASTDB_AWS_ACCESS_KEY_ID);
    }
    
    /**
     * Gets the AWS secret access key.
     * 
     * @return The AWS secret access key.
     */
    public String getVastDbAwsSecretAccessKey() {
        return options.get(VASTDB_AWS_SECRET_ACCESS_KEY);
    }
    
    /**
     * Gets the batch size.
     * 
     * @return The batch size.
     */
    public int getVastDbBatchSize() {
        String batchSizeStr = options.get(VASTDB_BATCH_SIZE);
        return batchSizeStr != null ? Integer.parseInt(batchSizeStr) : 100;
    }
    
    /**
     * Validates that all required options are present.
     */
    public void validate() {
        if (getVastDbEndpoint() == null) {
            throw new IllegalArgumentException(
                    "VastDB endpoint must be provided using --" + VASTDB_ENDPOINT + " or ENDPOINT environment variable");
        }
        
        if (getVastDbSchemaName() == null) {
            throw new IllegalArgumentException(
                    "VastDB schema name must be provided using --" + VASTDB_SCHEMA_NAME);
        }
        
        if (getVastDbTableName() == null) {
            throw new IllegalArgumentException(
                    "VastDB table name must be provided using --" + VASTDB_TABLE_NAME);
        }
        
        if (getVastDbAwsAccessKeyId() == null) {
            throw new IllegalArgumentException(
                    "AWS access key ID must be provided using --" + VASTDB_AWS_ACCESS_KEY_ID + 
                    " or AWS_ACCESS_KEY_ID environment variable");
        }
        
        if (getVastDbAwsSecretAccessKey() == null) {
            throw new IllegalArgumentException(
                    "AWS secret access key must be provided using --" + VASTDB_AWS_SECRET_ACCESS_KEY + 
                    " or AWS_SECRET_ACCESS_KEY environment variable");
        }
    }
}