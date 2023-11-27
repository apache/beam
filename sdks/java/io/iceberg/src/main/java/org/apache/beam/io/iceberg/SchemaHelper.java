package org.apache.beam.io.iceberg;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class SchemaHelper {

    public static Schema.Field convert(final Types.NestedField field) {

        Schema.Field f = null;
        switch(field.type().typeId()) {
            case BOOLEAN:
                f = Schema.Field.of(field.name(), Schema.FieldType.BOOLEAN);
                break;
            case INTEGER:
                f = Schema.Field.of(field.name(), Schema.FieldType.INT32);
                break;
            case LONG:
                f = Schema.Field.of(field.name(), Schema.FieldType.INT64);
                break;
            case FLOAT:
                f = Schema.Field.of(field.name(), Schema.FieldType.FLOAT);
                break;
            case DOUBLE:
                f = Schema.Field.of(field.name(), Schema.FieldType.DOUBLE);
                break;
            case DATE:
                f = Schema.Field.of(field.name(), Schema.FieldType.DATETIME);
                break;
            case TIME:
                f = Schema.Field.of(field.name(), Schema.FieldType.DATETIME);
                break;
            case TIMESTAMP:
                f = Schema.Field.of(field.name(), Schema.FieldType.DATETIME);
                break;
            case STRING:
                f = Schema.Field.of(field.name(), Schema.FieldType.STRING);
                break;
            case UUID:
                f = Schema.Field.of(field.name(), Schema.FieldType.BYTES);
                break;
            case FIXED:
                f = Schema.Field.of(field.name(), Schema.FieldType.DECIMAL);
                break;
            case BINARY:
                f = Schema.Field.of(field.name(), Schema.FieldType.BYTES);
                break;
            case DECIMAL:
                f = Schema.Field.of(field.name(), Schema.FieldType.DECIMAL);
                break;
            case STRUCT:
                f = Schema.Field.of(field.name(),
                        Schema.FieldType.row(convert(field.type().asStructType())));
                break;
            case LIST:
                break;
            case MAP:
                break;
        }
        f = f.withOptions(Schema.Options.builder()
                        .setOption("icebergTypeID", Schema.FieldType.STRING,field.type().typeId().name())
                .build());
        return f.withNullable(field.isOptional());

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
        String typeId = field.getOptions().getValue("icebergTypeID",String.class);
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
    }
}
