package org.apache.beam.io.iceberg.util;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

@SuppressWarnings({"dereference.of.nullable"})
public class SchemaHelper {

    private SchemaHelper() { }

    public static String ICEBERG_TYPE_OPTION_NAME = "icebergTypeID";

    public static Schema.FieldType fieldTypeForType(final Type type) {
        switch(type.typeId()) {
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case INTEGER:
                return FieldType.INT32;
            case LONG:
                return FieldType.INT64;
            case FLOAT:
                return FieldType.FLOAT;
            case DOUBLE:
                return FieldType.DOUBLE;
            case DATE: case TIME: case TIMESTAMP: //TODO: Logical types?
                return FieldType.DATETIME;
            case STRING:
                return FieldType.STRING;
            case UUID:
            case BINARY:
                return FieldType.BYTES;
            case FIXED:case DECIMAL:
                return FieldType.DECIMAL;
            case STRUCT:
                return FieldType.row(convert(type.asStructType()));
            case LIST:
                return FieldType.iterable(fieldTypeForType(type.asListType().elementType()));
            case MAP:
                return FieldType.map(fieldTypeForType(type.asMapType().keyType()),
                    fieldTypeForType(type.asMapType().valueType()));
        }
        throw new RuntimeException("Unrecognized Iceberg Type");
    }

    public static Schema.Field convert(final Types.NestedField field) {
        return Schema.Field.of(field.name(),fieldTypeForType(field.type()))
                .withOptions(Schema.Options.builder()
                        .setOption(ICEBERG_TYPE_OPTION_NAME,
                            Schema.FieldType.STRING,field.type().typeId().name())
                .build())
                .withNullable(field.isOptional());
    }
    public static Schema convert(final org.apache.iceberg.Schema schema) {
        Schema.Builder builder = Schema.builder();
        for(Types.NestedField f : schema.columns()) {
            builder.addField(convert(f));
        }
        return builder.build();
    }

    public static Schema convert(final Types.StructType struct) {
        Schema.Builder builder = Schema.builder();
        for(Types.NestedField f : struct.fields()) {
            builder.addField(convert(f));
        }
        return builder.build();
    }

    public static Types.NestedField convert(int fieldId,final Schema.Field field) {
        String typeId = field.getOptions().getValue(ICEBERG_TYPE_OPTION_NAME,String.class);
        if(typeId != null) {
            return Types.NestedField.of(
                    fieldId,
                    field.getType().getNullable(),
                    field.getName(),
                    Types.fromPrimitiveString(typeId));
        } else {
            return Types.NestedField.of(fieldId,
                    field.getType().getNullable(),
                    field.getName(),
                    Types.StringType.get());
        }
    }

    public static org.apache.iceberg.Schema convert(final Schema schema) {
        Types.NestedField[] fields = new Types.NestedField[schema.getFieldCount()];
        int fieldId = 0;
        for(Schema.Field f : schema.getFields()) {
            fields[fieldId++] = convert(fieldId,f);
        }
        return new org.apache.iceberg.Schema(fields);
    }
}
