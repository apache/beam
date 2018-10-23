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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/** Utils to convert AVRO records to Beam rows. */
@Experimental(Experimental.Kind.SCHEMAS)
public class AvroUtils {
  private AvroUtils() {}

  /**
   * Converts AVRO schema to Beam row schema.
   *
   * @param schema schema of type RECORD
   */
  public static Schema toSchema(@Nonnull org.apache.avro.Schema schema) {
    Schema.Builder builder = Schema.builder();

    for (org.apache.avro.Schema.Field field : schema.getFields()) {
      org.apache.avro.Schema unwrapped = unwrapNullableSchema(field.schema());

      if (!unwrapped.equals(field.schema())) {
        builder.addNullableField(field.name(), toFieldType(unwrapped));
      } else {
        builder.addField(field.name(), toFieldType(unwrapped));
      }
    }

    return builder.build();
  }

  /** Converts AVRO schema to Beam field. */
  public static Schema.FieldType toFieldType(@Nonnull org.apache.avro.Schema avroSchema) {
    switch (avroSchema.getType()) {
      case RECORD:
        return Schema.FieldType.row(toSchema(avroSchema));

      case ENUM:
        return Schema.FieldType.STRING;

      case ARRAY:
        Schema.FieldType elementType = toFieldType(avroSchema.getElementType());
        return Schema.FieldType.array(elementType);

      case MAP:
        return Schema.FieldType.map(
            Schema.FieldType.STRING, toFieldType(avroSchema.getValueType()));

      case FIXED:
        return Schema.FieldType.BYTES;

      case STRING:
        return Schema.FieldType.STRING;

      case BYTES:
        return Schema.FieldType.BYTES;

      case INT:
        return Schema.FieldType.INT32;

      case LONG:
        return Schema.FieldType.INT64;

      case FLOAT:
        return Schema.FieldType.FLOAT;

      case DOUBLE:
        return Schema.FieldType.DOUBLE;

      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;

      case UNION:
        throw new RuntimeException("Can't convert 'union' to FieldType");

      case NULL:
        throw new RuntimeException("Can't convert 'null' to FieldType");

      default:
        throw new AssertionError("Unexpected AVRO Schema.Type: " + avroSchema.getType());
    }
  }

  /**
   * Strict conversion from AVRO to Beam, strict because it doesn't do widening or narrowing during
   * conversion.
   */
  public static Row toRowStrict(@Nonnull GenericRecord record, @Nonnull Schema schema) {
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

    org.apache.avro.Schema unwrapped = unwrapNullableSchema(avroSchema);

    switch (unwrapped.getType()) {
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
        return convertArrayStrict((List<Object>) value, unwrapped.getElementType(), fieldType);

      case MAP:
        return convertMapStrict(
            (Map<CharSequence, Object>) value, unwrapped.getValueType(), fieldType);

      case UNION:
        throw new RuntimeException("Can't convert 'union', only nullable fields are supported");

      case NULL:
        throw new RuntimeException("Can't convert 'null' to non-nullable field");

      default:
        throw new AssertionError("Unexpected AVRO Schema.Type: " + unwrapped.getType());
    }
  }

  @VisibleForTesting
  static org.apache.avro.Schema unwrapNullableSchema(org.apache.avro.Schema avroSchema) {
    if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
      List<org.apache.avro.Schema> types = avroSchema.getTypes();

      // optional fields in AVRO have form of:
      // {"name": "foo", "type": ["null", "something"]}

      // don't need recursion because nested unions aren't supported in AVRO
      List<org.apache.avro.Schema> nonNullTypes = types.stream()
          .filter(x -> x.getType() != org.apache.avro.Schema.Type.NULL)
          .collect(Collectors.toList());

      if (nonNullTypes.size() == types.size()) {
        // union without `null`, keep as is
        return avroSchema;
      } else if (nonNullTypes.size() > 1) {
        return org.apache.avro.Schema.createUnion(nonNullTypes);
      } else if (nonNullTypes.size() == 1) {
        return nonNullTypes.get(0);
      } else { // nonNullTypes.size() == 0
        return avroSchema;
      }
    }

    return avroSchema;
  }

  private static Object convertRecordStrict(GenericRecord record, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ROW:
        return toRowStrict(record, fieldType.getRowSchema());
      default:
        throw new RuntimeException("Can't convert 'record' to " + fieldType.getTypeName());
    }
  }

  private static Object convertBytesStrict(ByteBuffer bb, Schema.FieldType fieldType) {
    // array of byte shouldn't be there because it's encoded as List
    switch (fieldType.getTypeName()) {
      case BYTES:
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
      default:
        throw new RuntimeException("Can't convert 'fixed' to " + fieldType.getTypeName());
    }
  }

  private static Object convertFixedStrict(GenericFixed fixed, Schema.FieldType fieldType) {
    // array of byte shouldn't be there because it's encoded as List
    switch (fieldType.getTypeName()) {
      case BYTES:
        return fixed.bytes().clone(); // clone because GenericFixed is mutable
      default:
        throw new RuntimeException("Can't convert 'fixed' to " + fieldType.getTypeName());
    }
  }

  private static Object convertStringStrict(CharSequence value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case STRING:
        return value.toString();
      default:
        throw new RuntimeException("Can't convert 'string' to " + fieldType.getTypeName());
    }
  }

  private static Object convertIntStrict(Integer value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
        return value;

      default:
        throw new RuntimeException("Can't convert 'int' to " + fieldType.getTypeName());
    }
  }

  private static Object convertLongStrict(Long value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT64:
        return value;

      default:
        throw new RuntimeException("Can't convert 'long' to " + fieldType.getTypeName());
    }
  }

  private static Object convertFloatStrict(Float value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case FLOAT:
        return value;

      default:
        throw new RuntimeException("Can't convert 'float' to " + fieldType.getTypeName());
    }
  }

  private static Object convertDoubleStrict(Double value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case DOUBLE:
        return value;

      default:
        throw new RuntimeException("Can't convert 'double' to " + fieldType.getTypeName());
    }
  }

  private static Object convertBooleanStrict(Boolean value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case BOOLEAN:
        return value;

      default:
        throw new RuntimeException("Can't convert 'boolean' to " + fieldType.getTypeName());
    }
  }

  private static Object convertEnumStrict(GenericEnumSymbol value, Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case STRING:
        return value.toString();

      default:
        throw new RuntimeException("Can't convert 'enum' to " + fieldType.getTypeName());
    }
  }

  private static Object convertArrayStrict(
      List<Object> values, org.apache.avro.Schema elemAvroSchema, Schema.FieldType fieldType) {

    switch (fieldType.getTypeName()) {
      case ARRAY:
        List<Object> ret = new ArrayList<>(values.size());
        Schema.FieldType elemFieldType = fieldType.getCollectionElementType();

        for (Object value : values) {
          ret.add(convertAvroFieldStrict(value, elemAvroSchema, elemFieldType));
        }

        return ret;

      default:
        throw new RuntimeException("Can't convert 'array' to " + fieldType.getTypeName());
    }
  }

  private static Object convertMapStrict(
      Map<CharSequence, Object> values,
      org.apache.avro.Schema valueAvroSchema,
      Schema.FieldType fieldType) {

    switch (fieldType.getTypeName()) {
      case MAP:
        checkNotNull(fieldType.getMapKeyType());
        checkNotNull(fieldType.getMapValueType());

        if (!fieldType.getMapKeyType().equals(Schema.FieldType.STRING)) {
          throw new RuntimeException(
              "Can't convert 'string' map keys to " + fieldType.getMapKeyType());
        }

        Map<Object, Object> ret = new HashMap<>();

        for (Map.Entry<CharSequence, Object> value : values.entrySet()) {
          ret.put(
              convertStringStrict(value.getKey(), fieldType.getMapKeyType()),
              convertAvroFieldStrict(
                  value.getValue(), valueAvroSchema, fieldType.getMapValueType()));
        }

        return ret;

      default:
        throw new RuntimeException("Can't convert 'array' to " + fieldType.getTypeName());
    }
  }
}
