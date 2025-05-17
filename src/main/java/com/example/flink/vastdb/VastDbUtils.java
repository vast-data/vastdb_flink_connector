package com.example.flink.vastdb;

import com.vastdata.client.VastClient;
import com.vastdata.client.error.VastException;
import com.vastdata.client.schema.CreateTableContext;
import com.vastdata.client.schema.StartTransactionContext;
import com.vastdata.client.schema.VastMetadataUtils;
import com.vastdata.client.tx.SimpleVastTransaction;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for VastDB operations.
 */
public class VastDbUtils {
    private static final Logger LOG = LoggerFactory.getLogger(VastDbUtils.class);
    
    /**
     * Creates a table in VastDB if it doesn't exist.
     * 
     * @param client The VastClient instance.
     * @param schemaName The schema name.
     * @param tableName The table name.
     * @param arrowSchema The Arrow schema.
     * @throws VastException If an error occurs while creating the table.
     */
    public static void createTableIfNotExists(
            VastClient client, 
            String schemaName, 
            String tableName,
            Schema arrowSchema) throws VastException {
        
        // Start a transaction
        SimpleTransactionFactory transactionFactory = new SimpleTransactionFactory();
        TransactionManager transactionManager = new TransactionManager(client, transactionFactory);
        SimpleVastTransaction tx = transactionManager
                .startTransaction(new StartTransactionContext(false, false));
        
        try {
            // Create schema if it doesn't exist
            if (!client.schemaExists(tx, schemaName)) {
                LOG.info("Creating schema: {}", schemaName);
                client.createSchema(tx, schemaName,
                        new VastMetadataUtils().getPropertiesString(Collections.emptyMap()));
            }
            
            // Check if table exists
            boolean tableExists = client.listTables(tx, schemaName, 100)
                    .anyMatch(t -> t.equals(tableName));
            
            if (!tableExists) {
                LOG.info("Creating table: {}/{}", schemaName, tableName);
                
                // Get fields from the schema
                List<Field> fieldsWithRowId = new ArrayList<>();
                fieldsWithRowId.add(com.vastdata.client.schema.ArrowSchemaUtils.VASTDB_ROW_ID_FIELD);
                fieldsWithRowId.addAll(arrowSchema.getFields());
                
                // Create the table
                client.createTable(tx, new CreateTableContext(
                        schemaName, 
                        tableName, 
                        fieldsWithRowId, 
                        null, 
                        null));
            } else {
                LOG.info("Table already exists: {}/{}", schemaName, tableName);
            }
            
            // Commit the transaction
            transactionManager.commit(tx);
        } catch (Exception e) {
            // Rollback the transaction
            transactionManager.rollback(tx);
            throw e;
        }
    }
}
