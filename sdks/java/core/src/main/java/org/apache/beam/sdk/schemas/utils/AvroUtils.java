/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/** Utils to convert AVRO records to Beam rows. */
@Experimental(Experimental.Kind.SCHEMAS)
public class AvroUtils {
  // Unwrap an AVRO schema into the base type an whether it is nullable.
  static class TypeWithNullability {
    public final org.apache.avro.Schema type;
    public final boolean nullable;

    TypeWithNullability(org.apache.avro.Schema avroSchema) {
      if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
        List<org.apache.avro.Schema> types = avroSchema.getTypes();

        // optional fields in AVRO have form of:
        // {"name": "foo", "type": ["null", "something"]}

        // don't need recursion because nested unions aren't supported in AVRO
        List<org.apache.avro.Schema> nonNullTypes =
            types
                .stream()
                .filter(x -> x.getType() != org.apache.avro.Schema.Type.NULL)
                .collect(Collectors.toList());

        if (nonNullTypes.size() == types.size() || nonNullTypes.isEmpty()) {
          // union without `null` or all 'null' union, keep as is.
          type = avroSchema;
          nullable = false;
        } else if (nonNullTypes.size() > 1) {
          type = org.apache.avro.Schema.createUnion(nonNullTypes);
          nullable = true;
        } else {
          // One non-null type.
          type = nonNullTypes.get(0);
          nullable = true;
        }
      } else {
        type = avroSchema;
        nullable = false;
      }
    }
  }

  private AvroUtils() {}

  /**
   * Converts AVRO schema to Beam row schema.
   *
   * @param schema schema of type RECORD
   */
  public static Schema toBeamSchema(org.apache.avro.Schema schema) {
    Schema.Builder builder = Schema.builder();

    for (org.apache.avro.Schema.Field field : schema.getFields()) {
      TypeWithNullability nullableType = new TypeWithNullability(field.schema());
      Field beamField = Field.of(field.name(), toFieldType(nullableType));
      if (field.doc() != null) {
        beamField = beamField.withDescription(field.doc());
      }
      builder.addField(beamField);
    }

    return builder.build();
  }

  /** Converts a Beam Schema into an AVRO schema. */
  public static org.apache.avro.Schema toAvroSchema(Schema beamSchema) {
    List<org.apache.avro.Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : beamSchema.getFields()) {
      org.apache.avro.Schema fieldSchema = getFieldSchema(field.getType());
      org.apache.avro.Schema.Field recordField =
          new org.apache.avro.Schema.Field(
              field.getName(), fieldSchema, field.getDescription(), (Object) null);
      fields.add(recordField);
    }
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord(fields);
    return avroSchema;
  }

  /**
   * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
   * conversion. If Schema is not provided, one is inferred from the AVRO schema.
   */
  public static Row toBeamRowStrict(GenericRecord record, @Nullable Schema schema) {
    if (schema == null) {
      schema = toBeamSchema(record.getSchema());
    }

    Row.Builder builder = Row.withSchema(schema);
    org.apache.avro.Schema avroSchema = record.getSchema();

    for (Schema.Field field : schema.getFields()) {
      Object value = record.get(field.getName());
      org.apache.avro.Schema fieldAvroSchema = avroSchema.getField(field.getName()).schema();

      if (value == null) {
        builder.addValue(null);
      } else {
        builder.addValue(convertAvroFieldStrict(value, fieldAvroSchema, field.getType()));
      }
    }

    return builder.build();
  }

  /**
   * Convert from a Beam Row to an AVRO GenericRecord. If a Schema is not provided, one is inferred
   * from the Beam schema on the orw.
   */
  public static GenericRecord toGenericRecord(
      Row row, @Nullable org.apache.avro.Schema avroSchema) {
    Schema beamSchema = row.getSchema();
    // Use the provided AVRO schema if present, otherwise infer an AVRO schema from the row
    // schema.
    if (avroSchema != null && avroSchema.getFields().size() != beamSchema.getFieldCount()) {
      throw new IllegalArgumentException(
          "AVRO schema doesn't match row schema. Row schema "
              + beamSchema
              + ". AVRO schema + "
              + avroSchema);
    }
    if (avroSchema == null) {
      avroSchema = toAvroSchema(beamSchema);
    }

    GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
    for (int i = 0; i < beamSchema.getFieldCount(); ++i) {
      Schema.Field field = beamSchema.getField(i);
      builder.set(
          field.getName(),
          genericFromBeamField(
              field.getType(), avroSchema.getField(field.getName()).schema(), row.getValue(i)));
    }
    return builder.build();
  }

  /** Converts AVRO schema to Beam field. */
  private static Schema.FieldType toFieldType(TypeWithNullability type) {
    Schema.FieldType fieldType = null;
    org.apache.avro.Schema avroSchema = type.type;

    LogicalType logicalType = LogicalTypes.fromSchema(avroSchema);
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        fieldType = FieldType.DECIMAL;
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        // TODO: There is a desire to move Beam schema DATETIME to a micros representation. When
        // this is done, this logical type needs to be changed.
        fieldType = FieldType.DATETIME;
      }
    }

    if (fieldType == null) {
      switch (type.type.getType()) {
        case RECORD:
          fieldType = Schema.FieldType.row(toBeamSchema(avroSchema));
          break;

        case ENUM:
          fieldType = Schema.FieldType.STRING;
          break;

        case ARRAY:
          Schema.FieldType elementType =
              toFieldType(new TypeWithNullability(avroSchema.getElementType()));
          fieldType = Schema.FieldType.array(elementType);
          break;

        case MAP:
          fieldType =
              Schema.FieldType.map(
                  Schema.FieldType.STRING,
                  toFieldType(new TypeWithNullability(avroSchema.getValueType())));
          break;

        case FIXED:
          fieldType = Schema.FieldType.BYTES;
          break;

        case STRING:
          fieldType = Schema.FieldType.STRING;
          break;

        case BYTES:
          fieldType = Schema.FieldType.BYTES;
          break;

        case INT:
          fieldType = Schema.FieldType.INT32;
          break;

        case LONG:
          fieldType = Schema.FieldType.INT64;
          break;

        case FLOAT:
          fieldType = Schema.FieldType.FLOAT;
          break;

        case DOUBLE:
          fieldType = Schema.FieldType.DOUBLE;
          break;

        case BOOLEAN:
          fieldType = Schema.FieldType.BOOLEAN;
          break;

        case UNION:
          throw new RuntimeException("Can't convert 'union' to FieldType");

        case NULL:
          throw new RuntimeException("Can't convert 'null' to FieldType");

        default:
          throw new AssertionError("Unexpected AVRO Schema.Type: " + avroSchema.getType());
      }
    }
    fieldType = fieldType.withNullable(type.nullable);
    return fieldType;
  }

  private static org.apache.avro.Schema getFieldSchema(Schema.FieldType fieldType) {
    org.apache.avro.Schema baseType;
    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
        baseType = org.apache.avro.Schema.create(Type.INT);
        break;

      case INT64:
        baseType = org.apache.avro.Schema.create(Type.LONG);
        break;

      case DECIMAL:
        baseType =
            LogicalTypes.decimal(Integer.MAX_VALUE)
                .addToSchema(org.apache.avro.Schema.create(Type.BYTES));
        break;

      case FLOAT:
        baseType = org.apache.avro.Schema.create(Type.FLOAT);
        break;

      case DOUBLE:
        baseType = org.apache.avro.Schema.create(Type.DOUBLE);
        break;

      case STRING:
        baseType = org.apache.avro.Schema.create(Type.STRING);
        break;

      case DATETIME:
        // TODO: There is a desire to move Beam schema DATETIME to a micros representation. When
        // this is done, this logical type needs to be changed.
        baseType =
            LogicalTypes.timestampMillis().addToSchema(org.apache.avro.Schema.create(Type.LONG));
        break;

      case BOOLEAN:
        baseType = org.apache.avro.Schema.create(Type.BOOLEAN);
        break;

      case BYTES:
        baseType = org.apache.avro.Schema.create(Type.BYTES);
        break;

      case ARRAY:
        baseType =
            org.apache.avro.Schema.createArray(
                getFieldSchema(fieldType.getCollectionElementType()));
        break;

      case MAP:
        if (fieldType.getMapKeyType().getTypeName().isStringType()) {
          // Avro only supports string keys in maps.
          baseType = org.apache.avro.Schema.createMap(getFieldSchema(fieldType.getMapValueType()));
        } else {
          throw new IllegalArgumentException("Avro only supports maps with string keys");
        }
        break;

      case ROW:
        baseType = toAvroSchema(fieldType.getRowSchema());
        break;

      default:
        throw new IllegalArgumentException("Unexpected type " + fieldType);
    }
    return fieldType.getNullable() ? ReflectData.makeNullable(baseType) : baseType;
  }

  private static Object genericFromBeamField(
      Schema.FieldType fieldType, org.apache.avro.Schema avroSchema, Object value) {
    org.apache.avro.Schema expectedSchema = getFieldSchema(fieldType);
    switch (fieldType.getTypeName()) {
      case BYTE:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return checkValueType(avroSchema, value, fieldType, expectedSchema);

      case STRING:
        return new Utf8((String) value);

      case DECIMAL:
        BigDecimal decimal = (BigDecimal) value;
        LogicalType logicalType = avroSchema.getLogicalType();
        ByteBuffer byteBuffer =
            new Conversions.DecimalConversion().toBytes(decimal, null, logicalType);
        return checkValueType(avroSchema, byteBuffer, fieldType, expectedSchema);

      case DATETIME:
        ReadableInstant instant = (ReadableInstant) value;
        return checkValueType(avroSchema, instant.getMillis(), fieldType, expectedSchema);

      case BYTES:
        return checkValueType(
            avroSchema, ByteBuffer.wrap((byte[]) value), fieldType, expectedSchema);

      case ARRAY:
        List array = (List) checkValueType(avroSchema, value, fieldType, expectedSchema);
        List<Object> translatedArray = Lists.newArrayListWithExpectedSize(array.size());
        org.apache.avro.Schema avroArrayType = new TypeWithNullability(avroSchema).type;

        for (Object arrayElement : array) {
          translatedArray.add(
              genericFromBeamField(
                  fieldType.getCollectionElementType(),
                  avroArrayType.getElementType(),
                  arrayElement));
        }
        return checkValueType(avroSchema, translatedArray, fieldType, expectedSchema);

      case MAP:
        ImmutableMap.Builder builder = ImmutableMap.builder();
        Map<Object, Object> valueMap =
            (Map<Object, Object>) checkValueType(avroSchema, value, fieldType, expectedSchema);
        org.apache.avro.Schema avroMapType = new TypeWithNullability(avroSchema).type;

        for (Map.Entry entry : valueMap.entrySet()) {
          Utf8 key = new Utf8((String) entry.getKey());
          builder.put(
              key,
              genericFromBeamField(
                  fieldType.getMapValueType(), avroMapType.getValueType(), entry.getValue()));
        }
        return checkValueType(avroSchema, builder.build(), fieldType, expectedSchema);

      case ROW:
        return checkValueType(
            avroSchema, toGenericRecord((Row) value, avroSchema), fieldType, expectedSchema);

      default:
        throw new IllegalArgumentException("Unsupported type " + fieldType);
    }
  }

  private static Object checkValueType(
      org.apache.avro.Schema avroSchema,
      Object o,
      FieldType fieldType,
      org.apache.avro.Schema expectedType) {
    TypeWithNullability typeWithNullability = new TypeWithNullability(avroSchema);
    if (!fieldType.getNullable().equals(typeWithNullability.nullable)) {
      throw new IllegalArgumentException(
          "FieldType "
              + fieldType
              + " and AVRO schema "
              + avroSchema
              + " don't have matching nullability");
    }
    return o;
  }

  /**
   * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
   * conversion.
   *
   * @param value {@link GenericRecord} or any nested value
   * @param avroSchema schema for value
   * @param fieldType target beam field type
   * @return value converted for {@link Row}
   */
  @SuppressWarnings("unchecked")
  public static Object convertAvroFieldStrict(
      @Nonnull Object value,
      @Nonnull org.apache.avro.Schema avroSchema,
      @Nonnull Schema.FieldType fieldType) {

    TypeWithNullability type = new TypeWithNullability(avroSchema);
    LogicalType logicalType = LogicalTypes.fromSchema(type.type);
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        ByteBuffer byteBuffer = (ByteBuffer) value;
        BigDecimal bigDecimal =
            new Conversions.DecimalConversion()
                .fromBytes(byteBuffer.duplicate(), type.type, logicalType);
        return convertDecimal(bigDecimal, fieldType);
      } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
        return convertDateTimeStrict((Long) value, fieldType);
      }
    }

    switch (type.type.getType()) {
      case FIXED:
        return convertFixedStrict((GenericFixed) value, fieldType);

      case BYTES:
        return convertBytesStrict((ByteBuffer) value, fieldType);

      case STRING:
        return convertStringStrict((CharSequence) value, fieldType);

      case INT:
        return convertIntStrict((Integer) value, fieldType);

      case LONG:
        return convertLongStrict((Long) value, fieldType);

      case FLOAT:
        return convertFloatStrict((Float) value, fieldType);

      case DOUBLE:
        return convertDoubleStrict((Double) value, fieldType);

      case BOOLEAN:
        return convertBooleanStrict((Boolean) value, fieldType);

      case RECORD:
        return convertRecordStrict((GenericRecord) value, fieldType);

      case ENUM:
        return convertEnumStrict((GenericEnumSymbol) value, fieldType);

      case ARRAY:
        return convertArrayStrict((List<Object>) value, type.type.getElementType(), fieldType);

      case MAP:
        return convertMapStrict(
            (Map<CharSequence, Object>) value, type.type.getValueType(), fieldType);

      case UNION:
        throw new IllegalArgumentException(
            "Can't convert 'union', only nullable fields are supported");

      case NULL:
        throw new IllegalArgumentException("Can't convert 'null' to non-nullable field");

      default:
        throw new AssertionError("Unexpected AVRO Schema.Type: " + type.type.getType());
    }
  }

  private static Object convertRecordStrict(GenericRecord record, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.ROW, "record");
    return toBeamRowStrict(record, fieldType.getRowSchema());
  }

  private static Object convertBytesStrict(ByteBuffer bb, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.BYTES, "bytes");

    byte[] bytes = new byte[bb.remaining()];
    bb.duplicate().get(bytes);
    return bytes;
  }

  private static Object convertFixedStrict(GenericFixed fixed, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.BYTES, "fixed");
    return fixed.bytes().clone(); // clone because GenericFixed is mutable
  }

  private static Object convertStringStrict(CharSequence value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.STRING, "string");
    return value.toString();
  }

  private static Object convertIntStrict(Integer value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.INT32, "int");
    return value;
  }

  private static Object convertLongStrict(Long value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.INT64, "long");
    return value;
  }

  private static Object convertDecimal(BigDecimal value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DECIMAL, "decimal");
    return value;
  }

  private static Object convertDateTimeStrict(Long value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), TypeName.DATETIME, "dateTime");
    return new Instant(value);
  }

  private static Object convertFloatStrict(Float value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.FLOAT, "float");
    return value;
  }

  private static Object convertDoubleStrict(Double value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.DOUBLE, "double");
    return value;
  }

  private static Object convertBooleanStrict(Boolean value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.BOOLEAN, "boolean");
    return value;
  }

  private static Object convertEnumStrict(GenericEnumSymbol value, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.STRING, "enum");
    return value.toString();
  }

  private static Object convertArrayStrict(
      List<Object> values, org.apache.avro.Schema elemAvroSchema, Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.ARRAY, "array");

    List<Object> ret = new ArrayList<>(values.size());
    Schema.FieldType elemFieldType = fieldType.getCollectionElementType();

    for (Object value : values) {
      ret.add(convertAvroFieldStrict(value, elemAvroSchema, elemFieldType));
    }

    return ret;
  }

  private static Object convertMapStrict(
      Map<CharSequence, Object> values,
      org.apache.avro.Schema valueAvroSchema,
      Schema.FieldType fieldType) {
    checkTypeName(fieldType.getTypeName(), Schema.TypeName.MAP, "map");
    checkNotNull(fieldType.getMapKeyType());
    checkNotNull(fieldType.getMapValueType());

    if (!fieldType.getMapKeyType().equals(Schema.FieldType.STRING)) {
      throw new IllegalArgumentException(
          "Can't convert 'string' map keys to " + fieldType.getMapKeyType());
    }

    Map<Object, Object> ret = new HashMap<>();

    for (Map.Entry<CharSequence, Object> value : values.entrySet()) {
      ret.put(
          convertStringStrict(value.getKey(), fieldType.getMapKeyType()),
          convertAvroFieldStrict(value.getValue(), valueAvroSchema, fieldType.getMapValueType()));
    }

    return ret;
  }

  private static void checkTypeName(Schema.TypeName got, Schema.TypeName expected, String label) {
    checkArgument(
        got.equals(expected),
        "Can't convert '" + label + "' to " + got + ", expected: " + expected);
  }
}
