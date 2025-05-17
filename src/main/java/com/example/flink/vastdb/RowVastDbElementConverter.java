package com.example.flink.vastdb;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter for Flink Row objects to Apache Arrow vectors.
 */
public class RowVastDbElementConverter implements VastDbElementConverter<Row> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RowVastDbElementConverter.class);

    @Override
    public void convert(Row element, List<FieldVector> vectors, int rowIndex) {
        for (int i = 0; i < element.getArity(); i++) {
            Object value = element.getField(i);
            if (value == null) {
                vectors.get(i).setNull(rowIndex);
                continue;
            }
            
            setVectorValue(vectors.get(i), rowIndex, value);
        }
    }
    
    private void setVectorValue(FieldVector vector, int rowIndex, Object value) {
        try {
            if (vector instanceof BitVector) {
                BitVector bitVector = (BitVector) vector;
                bitVector.set(rowIndex, (Boolean) value ? 1 : 0);
            } else if (vector instanceof TinyIntVector) {
                TinyIntVector tinyIntVector = (TinyIntVector) vector;
                tinyIntVector.set(rowIndex, ((Number) value).byteValue());
            } else if (vector instanceof SmallIntVector) {
                SmallIntVector smallIntVector = (SmallIntVector) vector;
                smallIntVector.set(rowIndex, ((Number) value).shortValue());
            } else if (vector instanceof IntVector) {
                IntVector intVector = (IntVector) vector;
                intVector.set(rowIndex, ((Number) value).intValue());
            } else if (vector instanceof BigIntVector) {
                BigIntVector bigIntVector = (BigIntVector) vector;
                bigIntVector.set(rowIndex, ((Number) value).longValue());
            } else if (vector instanceof Float4Vector) {
                Float4Vector float4Vector = (Float4Vector) vector;
                float4Vector.set(rowIndex, ((Number) value).floatValue());
            } else if (vector instanceof Float8Vector) {
                Float8Vector float8Vector = (Float8Vector) vector;
                float8Vector.set(rowIndex, ((Number) value).doubleValue());
            } else if (vector instanceof DecimalVector) {
                DecimalVector decimalVector = (DecimalVector) vector;
                decimalVector.set(rowIndex, (BigDecimal) value);
            } else if (vector instanceof VarCharVector) {
                VarCharVector varCharVector = (VarCharVector) vector;
                if (value instanceof String) {
                    varCharVector.set(rowIndex, new Text((String) value));
                } else {
                    varCharVector.set(rowIndex, new Text(value.toString()));
                }
            } else if (vector instanceof DateDayVector) {
                DateDayVector dateDayVector = (DateDayVector) vector;
                LocalDate date = (LocalDate) value;
                dateDayVector.set(rowIndex, Math.toIntExact(date.toEpochDay()));
            } else if (vector instanceof TimeStampVector) {
                TimeStampVector timeStampVector = (TimeStampVector) vector;
                if (value instanceof LocalDateTime) {
                    LocalDateTime dateTime = (LocalDateTime) value;
                    timeStampVector.set(rowIndex, dateTime.atZone(java.time.ZoneId.systemDefault())
                            .toInstant().toEpochMilli() * 1000); // Convert to microseconds
                } else {
                    timeStampVector.set(rowIndex, ((Number) value).longValue());
                }
            } else {
                LOG.warn("Unsupported vector type: {}", vector.getClass().getName());
                vector.setNull(rowIndex);
            }
        } catch (ClassCastException e) {
            LOG.error("Type mismatch when setting value for vector {}: {}", 
                    vector.getClass().getName(), e.getMessage());
            vector.setNull(rowIndex);
        } catch (Exception e) {
            LOG.error("Error setting value for vector {}: {}", 
                    vector.getClass().getName(), e.getMessage());
            vector.setNull(rowIndex);
        }
    }
}
