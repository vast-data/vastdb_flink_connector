package com.example.flink.vastdb;

import java.io.Serializable;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.DataType;

/**
 * Interface for converting between Flink and Arrow schemas.
 */
public interface VastDbSchemaConverter extends Serializable {
    
    /**
     * Converts a Flink DataType to an Apache Arrow Schema.
     * 
     * @param dataType The Flink DataType to convert.
     * @return The equivalent Arrow Schema.
     */
    Schema toArrowSchema(DataType dataType);
    
    /**
     * Gets the Arrow Schema with VastDB row ID field included.
     * 
     * @param schema The original Arrow Schema.
     * @return The Arrow Schema with VastDB row ID field added.
     */
    Schema withRowId(Schema schema);
}

