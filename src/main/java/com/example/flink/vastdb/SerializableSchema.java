package com.example.flink.vastdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import com.vastdata.client.schema.ArrowSchemaUtils;

/**
 * A serializable representation of an Arrow schema.
 * This class allows Flink to serialize schema information across the cluster.
 */
public class SerializableSchema implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final List<SerializableField> fields;
    private final boolean includeRowId;
    
    /**
     * Creates a new SerializableSchema.
     * 
     * @param fields The list of serializable fields.
     * @param includeRowId Whether to include the row ID field.
     */
    public SerializableSchema(List<SerializableField> fields, boolean includeRowId) {
        this.fields = fields;
        this.includeRowId = includeRowId;
    }
    
    /**
     * Converts this serializable schema to an Arrow schema.
     * 
     * @return The Arrow schema.
     */
    public Schema toArrowSchema() {
        List<Field> arrowFields = fields.stream()
                .map(SerializableField::toArrowField)
                .collect(Collectors.toList());
        
        if (includeRowId) {
            List<Field> fieldsWithRowId = new ArrayList<>();
            fieldsWithRowId.add(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD);
            fieldsWithRowId.addAll(arrowFields);
            return new Schema(fieldsWithRowId);
        } else {
            return new Schema(arrowFields);
        }
    }
    
    /**
     * Creates a serializable schema from an Arrow schema.
     * 
     * @param arrowSchema The Arrow schema.
     * @return The serializable schema.
     */
    public static SerializableSchema fromArrowSchema(Schema arrowSchema) {
        List<SerializableField> fields = arrowSchema.getFields().stream()
                .map(SerializableField::fromArrowField)
                .collect(Collectors.toList());
        
        // Check if the schema already has a row ID field
        boolean includeRowId = arrowSchema.getFields().stream()
                .anyMatch(f -> f.getName().equals(ArrowSchemaUtils.VASTDB_ROW_ID_FIELD.getName()));
        
        return new SerializableSchema(fields, includeRowId);
    }
    
    /**
     * A serializable representation of an Arrow field.
     */
    public static class SerializableField implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String name;
        private final boolean nullable;
        private final SerializableType type;
        private final List<SerializableField> children;
        
        /**
         * Creates a new SerializableField.
         * 
         * @param name The field name.
         * @param nullable Whether the field is nullable.
         * @param type The field type.
         * @param children The child fields (for complex types).
         */
        public SerializableField(String name, boolean nullable, SerializableType type, 
                List<SerializableField> children) {
            this.name = name;
            this.nullable = nullable;
            this.type = type;
            this.children = children;
        }
        
        /**
         * Converts this serializable field to an Arrow field.
         * 
         * @return The Arrow field.
         */
        public Field toArrowField() {
            FieldType fieldType = new FieldType(nullable, type.toArrowType(), null);
            
            if (children != null && !children.isEmpty()) {
                List<Field> childFields = children.stream()
                        .map(SerializableField::toArrowField)
                        .collect(Collectors.toList());
                return new Field(name, fieldType, childFields);
            } else {
                return new Field(name, fieldType, null);
            }
        }
        
        /**
         * Creates a serializable field from an Arrow field.
         * 
         * @param field The Arrow field.
         * @return The serializable field.
         */
        public static SerializableField fromArrowField(Field field) {
            SerializableType type = SerializableType.fromArrowType(field.getType());
            
            List<SerializableField> children = null;
            if (field.getChildren() != null && !field.getChildren().isEmpty()) {
                children = field.getChildren().stream()
                        .map(SerializableField::fromArrowField)
                        .collect(Collectors.toList());
            }
            
            return new SerializableField(
                    field.getName(), 
                    field.isNullable(), 
                    type, 
                    children);
        }
    }
    
    /**
     * A serializable representation of an Arrow type.
     */
    public static class SerializableType implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public enum TypeCode {
            BOOL,
            INT,
            FLOATING_POINT,
            UTF8,
            DECIMAL,
            DATE,
            TIMESTAMP,
            LIST,
            MAP,
            STRUCT
        }
        
        private final TypeCode typeCode;
        private final Integer bitWidth;
        private final Boolean signed;
        private final FloatingPointPrecision precision;
        private final Integer scale;
        private final DateUnit dateUnit;
        private final TimeUnit timeUnit;
        private final String timezone;
        private final Boolean keysSorted;
        
        /**
         * Creates a new SerializableType.
         */
        public SerializableType(TypeCode typeCode, Integer bitWidth, Boolean signed, 
                FloatingPointPrecision precision, Integer scale, DateUnit dateUnit, 
                TimeUnit timeUnit, String timezone, Boolean keysSorted) {
            this.typeCode = typeCode;
            this.bitWidth = bitWidth;
            this.signed = signed;
            this.precision = precision;
            this.scale = scale;
            this.dateUnit = dateUnit;
            this.timeUnit = timeUnit;
            this.timezone = timezone;
            this.keysSorted = keysSorted;
        }
        
        /**
         * Converts this serializable type to an Arrow type.
         * 
         * @return The Arrow type.
         */
        public ArrowType toArrowType() {
            switch (typeCode) {
                case BOOL:
                    return new ArrowType.Bool();
                case INT:
                    return new ArrowType.Int(bitWidth, signed);
                case FLOATING_POINT:
                    return new ArrowType.FloatingPoint(precision);
                case UTF8:
                    return new ArrowType.Utf8();
                case DECIMAL:
                    return new ArrowType.Decimal(bitWidth, scale);
                case DATE:
                    return new ArrowType.Date(dateUnit);
                case TIMESTAMP:
                    return new ArrowType.Timestamp(timeUnit, timezone);
                case LIST:
                    return new ArrowType.List();
                case MAP:
                    return new ArrowType.Map(keysSorted);
                case STRUCT:
                    return new ArrowType.Struct();
                default:
                    throw new IllegalStateException("Unsupported type code: " + typeCode);
            }
        }
        
        /**
         * Creates a serializable type from an Arrow type.
         * 
         * @param arrowType The Arrow type.
         * @return The serializable type.
         */
        public static SerializableType fromArrowType(ArrowType arrowType) {
            if (arrowType instanceof ArrowType.Bool) {
                return new SerializableType(TypeCode.BOOL, null, null, null, null, null, null, null, null);
            } else if (arrowType instanceof ArrowType.Int) {
                ArrowType.Int intType = (ArrowType.Int) arrowType;
                return new SerializableType(TypeCode.INT, intType.getBitWidth(), intType.getIsSigned(), 
                        null, null, null, null, null, null);
            } else if (arrowType instanceof ArrowType.FloatingPoint) {
                ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
                return new SerializableType(TypeCode.FLOATING_POINT, null, null, fpType.getPrecision(), 
                        null, null, null, null, null);
            } else if (arrowType instanceof ArrowType.Utf8) {
                return new SerializableType(TypeCode.UTF8, null, null, null, null, null, null, null, null);
            } else if (arrowType instanceof ArrowType.Decimal) {
                ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
                return new SerializableType(TypeCode.DECIMAL, decimalType.getPrecision(), null, null, 
                        decimalType.getScale(), null, null, null, null);
            } else if (arrowType instanceof ArrowType.Date) {
                ArrowType.Date dateType = (ArrowType.Date) arrowType;
                return new SerializableType(TypeCode.DATE, null, null, null, null, dateType.getUnit(), 
                        null, null, null);
            } else if (arrowType instanceof ArrowType.Timestamp) {
                ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                return new SerializableType(TypeCode.TIMESTAMP, null, null, null, null, null, 
                        timestampType.getUnit(), timestampType.getTimezone(), null);
            } else if (arrowType instanceof ArrowType.List) {
                return new SerializableType(TypeCode.LIST, null, null, null, null, null, null, null, null);
            } else if (arrowType instanceof ArrowType.Map) {
                ArrowType.Map mapType = (ArrowType.Map) arrowType;
                return new SerializableType(TypeCode.MAP, null, null, null, null, null, null, null, 
                        mapType.getKeysSorted());
            } else if (arrowType instanceof ArrowType.Struct) {
                return new SerializableType(TypeCode.STRUCT, null, null, null, null, null, null, null, null);
            } else {
                throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType);
            }
        }
    }
}
