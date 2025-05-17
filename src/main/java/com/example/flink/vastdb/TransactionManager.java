package com.example.flink.vastdb;

import com.vastdata.client.VastClient;
import com.vastdata.client.tx.SimpleVastTransaction;
import com.vastdata.client.tx.VastTransactionHandleManager;

/**
 * Manager for VastDB transactions.
 */
public class TransactionManager
        extends VastTransactionHandleManager<SimpleVastTransaction> {
    
    private static TransactionManager instance;
    
    /**
     * Creates a new TransactionManager instance.
     * 
     * @param client The VastClient instance.
     * @param transactionInstantiationFunction The transaction factory.
     */
    TransactionManager(
            VastClient client, 
            SimpleTransactionFactory transactionInstantiationFunction) {
        
        super(client, transactionInstantiationFunction);
    }
    
    /**
     * Gets the singleton instance of the TransactionManager.
     * 
     * @param client The VastClient instance.
     * @param transactionInstantiationFunction The transaction factory.
     * @return The TransactionManager instance.
     */
    public static TransactionManager getInstance(
            VastClient client, 
            SimpleTransactionFactory transactionInstantiationFunction) {
        
        if (instance == null) {
            initInstance(client, transactionInstantiationFunction);
        }
        return instance;
    }
    
    private static synchronized void initInstance(
            VastClient client, 
            SimpleTransactionFactory transactionInstantiationFunction) {
        
        if (instance == null) {
            instance = new TransactionManager(client, transactionInstantiationFunction);
        }
    }
}
