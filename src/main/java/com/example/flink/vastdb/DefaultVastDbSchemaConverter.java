package com.example.flink.vastdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vastdata.client.schema.ArrowSchemaUtils;

/**
 * Default implementation of the VastDbSchemaConverter interface.
 */
public class DefaultVastDbSchemaConverter implements VastDbSchemaConverter {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DefaultVastDbSchemaConverter.class);

    @Override
    public Schema toArrowSchema(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        
        if (!(logicalType instanceof RowType)) {
            throw new IllegalArgumentException("Expected RowType for schema conversion, got: " + logicalType.getClass().getSimpleName());
        }
        
        RowType rowType = (RowType) logicalType;
        List<Field> fields = new ArrayList<>();
        
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            LogicalType fieldType = rowType.getTypeAt(i);
            
            Field field = convertField(fieldName, fieldType);
            fields.add(field);
        }
        
        return new Schema(fields);
    }

    @Override
    public Schema withRowId(Schema schema) {
        List<Field> fieldsWithRowId = new ArrayList<>();
        fieldsWithRowId.add(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD);
        fieldsWithRowId.addAll(schema.getFields());
        
        return new Schema(fieldsWithRowId);
    }
    
    private Field convertField(String name, LogicalType logicalType) {
        FieldType fieldType;
        List<Field> children = null;
        
        if (logicalType instanceof BooleanType) {
            fieldType = FieldType.nullable(new ArrowType.Bool());
        } else if (logicalType instanceof TinyIntType) {
            fieldType = FieldType.nullable(new ArrowType.Int(8, true));
        } else if (logicalType instanceof SmallIntType) {
            fieldType = FieldType.nullable(new ArrowType.Int(16, true));
        } else if (logicalType instanceof IntType) {
            fieldType = FieldType.nullable(new ArrowType.Int(32, true));
        } else if (logicalType instanceof BigIntType) {
            fieldType = FieldType.nullable(new ArrowType.Int(64, true));
        } else if (logicalType instanceof FloatType) {
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        } else if (logicalType instanceof DoubleType) {
            fieldType = FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            fieldType = FieldType.nullable(new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale()));
        } else if (logicalType instanceof CharType) {
            fieldType = FieldType.nullable(new ArrowType.Utf8());
        } else if (logicalType instanceof VarCharType) {
            fieldType = FieldType.nullable(new ArrowType.Utf8());
        } else if (logicalType instanceof DateType) {
            fieldType = FieldType.nullable(new ArrowType.Date(DateUnit.DAY));
        } else if (logicalType instanceof TimestampType) {
            fieldType = FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            Field childField = convertField("element", arrayType.getElementType());
            children = Collections.singletonList(childField);
            fieldType = FieldType.nullable(new ArrowType.List());
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            
            Field keyField = convertField("key", mapType.getKeyType());
            Field valueField = convertField("value", mapType.getValueType());
            
            // For Arrow's struct representation of map entries
            List<Field> structFields = new ArrayList<>();
            structFields.add(keyField);
            structFields.add(valueField);
            
            Field structField = new Field(
                    "entries",
                    FieldType.notNullable(new ArrowType.Struct()),
                    structFields);
            
            children = Collections.singletonList(structField);
            fieldType = FieldType.nullable(new ArrowType.Map(false));
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            
            children = new ArrayList<>();
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String fieldName = rowType.getFieldNames().get(i);
                LogicalType fieldType1 = rowType.getTypeAt(i);
                
                Field childField = convertField(fieldName, fieldType1);
                children.add(childField);
            }
            
            fieldType = FieldType.nullable(new ArrowType.Struct());
        } else {
            LOG.warn("Unsupported type: {}. Defaulting to String", logicalType);
            fieldType = FieldType.nullable(new ArrowType.Utf8());
        }
        
        return new Field(name, fieldType, children);
    }
}
