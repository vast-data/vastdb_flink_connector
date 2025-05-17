package com.example.flink.vastdb;

import java.io.Serializable;
import java.util.List;

import org.apache.arrow.vector.FieldVector;

/**
 * Interface for converting elements from Flink to Apache Arrow vectors.
 * 
 * @param <T> The type of elements to convert.
 */
public interface VastDbElementConverter<T> extends Serializable {
    
    /**
     * Converts an element to Arrow format, writing values to the provided vectors.
     * 
     * @param element The element to convert.
     * @param vectors The list of Arrow vectors to populate.
     * @param rowIndex The row index to write values to.
     */
    void convert(T element, List<FieldVector> vectors, int rowIndex);
}

