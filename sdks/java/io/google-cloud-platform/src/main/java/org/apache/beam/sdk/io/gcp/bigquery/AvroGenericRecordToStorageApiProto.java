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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils.TypeWithNullability;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;

/**
 * Utility methods for converting Avro {@link GenericRecord} objects to dynamic protocol message,
 * for use with the Storage write API.
 */
public class AvroGenericRecordToStorageApiProto {

  private static final org.joda.time.LocalDate EPOCH_DATE = new org.joda.time.LocalDate(1970, 1, 1);

  static final Map<Schema.Type, TableFieldSchema.Type> PRIMITIVE_TYPES =
      ImmutableMap.<Schema.Type, TableFieldSchema.Type>builder()
          .put(Schema.Type.INT, TableFieldSchema.Type.INT64)
          .put(Schema.Type.FIXED, TableFieldSchema.Type.BYTES)
          .put(Schema.Type.LONG, TableFieldSchema.Type.INT64)
          .put(Schema.Type.FLOAT, TableFieldSchema.Type.DOUBLE)
          .put(Schema.Type.DOUBLE, TableFieldSchema.Type.DOUBLE)
          .put(Schema.Type.STRING, TableFieldSchema.Type.STRING)
          .put(Schema.Type.BOOLEAN, TableFieldSchema.Type.BOOL)
          .put(Schema.Type.ENUM, TableFieldSchema.Type.STRING)
          .put(Schema.Type.BYTES, TableFieldSchema.Type.BYTES)
          .build();

  // A map of supported logical types to the protobuf field type.
  static Optional<TableFieldSchema.Type> logicalTypes(LogicalType logicalType) {
    switch (logicalType.getName()) {
      case "date":
        return Optional.of(TableFieldSchema.Type.DATE);
      case "time-micros":
        return Optional.of(TableFieldSchema.Type.TIME);
      case "time-millis":
        return Optional.of(TableFieldSchema.Type.TIME);
      case "decimal":
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
        int scale = decimal.getScale();
        int precision = decimal.getPrecision();
        if (scale > 9 || precision - scale > 29) {
          return Optional.of(TableFieldSchema.Type.BIGNUMERIC);
        } else {
          return Optional.of(TableFieldSchema.Type.NUMERIC);
        }
      case "timestamp-micros":
        return Optional.of(TableFieldSchema.Type.TIMESTAMP);
      case "timestamp-millis":
        return Optional.of(TableFieldSchema.Type.TIMESTAMP);
      case "local-timestamp-micros":
        return Optional.of(TableFieldSchema.Type.DATETIME);
      case "local-timestamp-millis":
        return Optional.of(TableFieldSchema.Type.DATETIME);
      case "uuid":
        return Optional.of(TableFieldSchema.Type.STRING);
      default:
        return Optional.empty();
    }
  }

  static final Map<Schema.Type, Function<Object, Object>> PRIMITIVE_ENCODERS =
      ImmutableMap.<Schema.Type, Function<Object, Object>>builder()
          .put(Schema.Type.INT, o -> Long.valueOf((int) o))
          .put(Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()))
          .put(Schema.Type.LONG, Functions.identity())
          .put(Schema.Type.FLOAT, o -> Double.parseDouble(Float.valueOf((float) o).toString()))
          .put(Schema.Type.DOUBLE, Function.identity())
          .put(Schema.Type.STRING, Object::toString)
          .put(Schema.Type.BOOLEAN, Function.identity())
          .put(Schema.Type.ENUM, o -> o.toString())
          .put(Schema.Type.BYTES, AvroGenericRecordToStorageApiProto::convertBytes)
          .build();

  // A map of supported logical types to their encoding functions.
  static final Map<String, BiFunction<LogicalType, Object, Object>> LOGICAL_TYPE_ENCODERS =
      ImmutableMap.<String, BiFunction<LogicalType, Object, Object>>builder()
          .put("date", (logicalType, value) -> convertDate(value))
          .put("time-micros", (logicalType, value) -> convertTime(value, true))
          .put("time-millis", (logicalType, value) -> convertTime(value, false))
          .put("decimal", AvroGenericRecordToStorageApiProto::convertDecimal)
          .put("timestamp-micros", (logicalType, value) -> convertTimestamp(value, true))
          .put("timestamp-millis", (logicalType, value) -> convertTimestamp(value, false))
          .put("local-timestamp-micros", (logicalType, value) -> convertDateTime(value, true))
          .put("local-timestamp-millis", (logicalType, value) -> convertDateTime(value, false))
          .put("uuid", (logicalType, value) -> convertUUID(value))
          .build();

  static String convertUUID(Object value) {
    if (value instanceof UUID) {
      return ((UUID) value).toString();
    } else {
      Preconditions.checkArgument(value instanceof String, "Expecting a value as String type.");
      UUID.fromString((String) value);
      return (String) value;
    }
  }

  static Long convertTimestamp(Object value, boolean micros) {
    if (value instanceof org.joda.time.ReadableInstant) {
      return ((org.joda.time.ReadableInstant) value).getMillis() * 1_000L;
    } else if (value instanceof java.time.Instant) {
      java.time.Instant instant = (java.time.Instant) value;
      long seconds = instant.getEpochSecond();
      int nanos = instant.getNano();

      if (seconds < 0 && nanos > 0) {
        long ms = Math.multiplyExact(seconds + 1, 1_000_000L);
        long adjustment = (nanos / 1_000L) - 1_000_000L;
        return Math.addExact(ms, adjustment);
      } else {
        long ms = Math.multiplyExact(seconds, 1_000_000L);
        return Math.addExact(ms, nanos / 1_000L);
      }
    } else {
      Preconditions.checkArgument(
          value instanceof Long, "Expecting a value as Long type (timestamp).");
      return (micros ? 1 : 1_000L) * ((Long) value);
    }
  }

  static Integer convertDate(Object value) {
    if (value instanceof org.joda.time.LocalDate) {
      return org.joda.time.Days.daysBetween(EPOCH_DATE, (org.joda.time.LocalDate) value).getDays();
    } else if (value instanceof java.time.LocalDate) {
      return (int) ((java.time.LocalDate) value).toEpochDay();
    } else {
      Preconditions.checkArgument(
          value instanceof Integer, "Expecting a value as Integer type (days).");
      return (Integer) value;
    }
  }

  static Long convertTime(Object value, boolean micros) {
    if (value instanceof org.joda.time.LocalTime) {
      return CivilTimeEncoder.encodePacked64TimeMicros((org.joda.time.LocalTime) value);
    } else if (value instanceof java.time.LocalTime) {
      return CivilTimeEncoder.encodePacked64TimeMicros((java.time.LocalTime) value);
    } else {
      if (micros) {
        Preconditions.checkArgument(
            value instanceof Long, "Expecting a value as Long type (time).");
        return CivilTimeEncoder.encodePacked64TimeMicros(
            java.time.LocalTime.ofNanoOfDay((TimeUnit.MICROSECONDS.toNanos((long) value))));
      } else {
        Preconditions.checkArgument(
            value instanceof Integer, "Expecting a value as Integer type (time).");
        return CivilTimeEncoder.encodePacked64TimeMicros(
            java.time.LocalTime.ofNanoOfDay(
                (TimeUnit.MILLISECONDS).toNanos(((Integer) value).longValue())));
      }
    }
  }

  static Long convertDateTime(Object value, boolean micros) {
    if (value instanceof org.joda.time.LocalDateTime) {
      // we should never come here as local-timestamp has been added after joda deprecation
      // implement nonetheless for consistency
      org.joda.time.DateTime dateTime =
          ((org.joda.time.LocalDateTime) value).toDateTime(org.joda.time.DateTimeZone.UTC);
      return 1_000L * dateTime.getMillis();
    } else if (value instanceof java.time.LocalDateTime) {
      java.time.Instant instant =
          ((java.time.LocalDateTime) value).toInstant(java.time.ZoneOffset.UTC);
      return convertTimestamp(instant, micros);
    } else {
      Preconditions.checkArgument(
          value instanceof Long, "Expecting a value as Long type (local-timestamp).");
      return (micros ? 1 : 1_000L) * ((Long) value);
    }
  }

  static ByteString convertDecimal(LogicalType logicalType, Object value) {
    ByteBuffer byteBuffer;
    if (value instanceof BigDecimal) {
      // BigDecimalByteStringEncoder does not support parametrized NUMERIC/BIGNUMERIC
      byteBuffer =
          new Conversions.DecimalConversion()
              .toBytes(
                  (BigDecimal) value,
                  Schema.create(Schema.Type.NULL), // dummy schema, not used
                  logicalType);
    } else {
      Preconditions.checkArgument(
          value instanceof ByteBuffer, "Expecting a value as ByteBuffer type (decimal).");
      byteBuffer = (ByteBuffer) value;
    }
    byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(bytes);
    Bytes.reverse(bytes);
    return ByteString.copyFrom(bytes);
  }

  static ByteString convertBytes(Object value) {
    if (value instanceof byte[]) {
      // for backward compatibility
      // this is not accepted by the avro spec, but users may have abused it
      return ByteString.copyFrom((byte[]) value);
    } else {
      return ByteString.copyFrom(((ByteBuffer) value).duplicate());
    }
  }

  /**
   * Given an Avro Schema, returns a protocol-buffer TableSchema that can be used to write data
   * through BigQuery Storage API.
   *
   * @param schema An Avro Schema
   * @return Returns the TableSchema created from the provided Schema
   */
  public static TableSchema protoTableSchemaFromAvroSchema(Schema schema) {
    Preconditions.checkState(!schema.getFields().isEmpty());

    TableSchema.Builder builder = TableSchema.newBuilder();
    for (Schema.Field field : schema.getFields()) {
      builder.addFields(fieldDescriptorFromAvroField(field));
    }
    return builder.build();
  }

  /**
   * Forwards {@param changeSequenceNum} to {@link #messageFromGenericRecord(Descriptor,
   * GenericRecord, String, String)} via {@link Long#toHexString}.
   */
  public static DynamicMessage messageFromGenericRecord(
      Descriptor descriptor,
      GenericRecord record,
      @Nullable String changeType,
      long changeSequenceNum) {
    return messageFromGenericRecord(
        descriptor, record, changeType, Long.toHexString(changeSequenceNum));
  }

  /**
   * Given an Avro {@link GenericRecord} object, returns a protocol-buffer message that can be used
   * to write data using the BigQuery Storage streaming API.
   *
   * @param descriptor The Descriptor for the DynamicMessage result
   * @param record An Avro GenericRecord
   * @return A dynamic message representation of a Proto payload to be used for StorageWrite API
   */
  public static DynamicMessage messageFromGenericRecord(
      Descriptor descriptor,
      GenericRecord record,
      @Nullable String changeType,
      @Nullable String changeSequenceNum) {
    Schema schema = record.getSchema();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Schema.Field field : schema.getFields()) {
      FieldDescriptor fieldDescriptor =
          Preconditions.checkNotNull(descriptor.findFieldByName(field.name().toLowerCase()));
      @Nullable
      Object value =
          messageValueFromGenericRecordValue(fieldDescriptor, field, field.name(), record);
      if (value != null) {
        builder.setField(fieldDescriptor, value);
      }
    }
    if (changeType != null) {
      builder.setField(
          org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
              descriptor.findFieldByName(StorageApiCDC.CHANGE_TYPE_COLUMN)),
          changeType);
      builder.setField(
          org.apache.beam.sdk.util.Preconditions.checkStateNotNull(
              descriptor.findFieldByName(StorageApiCDC.CHANGE_SQN_COLUMN)),
          org.apache.beam.sdk.util.Preconditions.checkStateNotNull(changeSequenceNum));
    }
    return builder.build();
  }

  @SuppressWarnings("nullness")
  private static TableFieldSchema fieldDescriptorFromAvroField(org.apache.avro.Schema.Field field) {
    @Nullable Schema schema = field.schema();
    Preconditions.checkNotNull(schema, "Unexpected null schema!");
    if (StorageApiCDC.COLUMNS.contains(field.name())) {
      throw new RuntimeException("Reserved field name " + field.name() + " in user schema.");
    }
    TableFieldSchema.Builder builder =
        TableFieldSchema.newBuilder().setName(field.name().toLowerCase());
    Schema elementType = null;
    switch (schema.getType()) {
      case RECORD:
        Preconditions.checkState(!schema.getFields().isEmpty());
        builder = builder.setType(TableFieldSchema.Type.STRUCT);
        for (Schema.Field recordField : schema.getFields()) {
          builder = builder.addFields(fieldDescriptorFromAvroField(recordField));
        }
        break;
      case ARRAY:
        elementType = TypeWithNullability.create(schema.getElementType()).getType();
        if (elementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        Preconditions.checkState(
            elementType.getType() != Schema.Type.ARRAY, "Nested arrays not supported by BigQuery.");

        TableFieldSchema elementFieldSchema =
            fieldDescriptorFromAvroField(
                new Schema.Field(field.name(), elementType, field.doc(), field.defaultVal()));
        builder = builder.setType(elementFieldSchema.getType());
        builder.addAllFields(elementFieldSchema.getFieldsList());
        builder = builder.setMode(TableFieldSchema.Mode.REPEATED);
        break;
      case MAP:
        Schema keyType = Schema.create(Schema.Type.STRING);
        Schema valueType = Schema.create(schema.getValueType().getType());
        if (valueType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        TableFieldSchema keyFieldSchema =
            fieldDescriptorFromAvroField(
                new Schema.Field("key", keyType, "key of the map entry", null));
        TableFieldSchema valueFieldSchema =
            fieldDescriptorFromAvroField(
                new Schema.Field("value", valueType, "value of the map entry", null));
        builder =
            builder
                .setType(TableFieldSchema.Type.STRUCT)
                .addFields(keyFieldSchema)
                .addFields(valueFieldSchema)
                .setMode(TableFieldSchema.Mode.REPEATED);
        break;
      case UNION:
        elementType = TypeWithNullability.create(schema).getType();
        if (elementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        // check to see if more than one non-null type is defined in the union
        Preconditions.checkState(
            elementType.getType() != Schema.Type.UNION,
            "Multiple non-null union types are not supported.");
        TableFieldSchema unionFieldSchema =
            fieldDescriptorFromAvroField(
                new Schema.Field(field.name(), elementType, field.doc(), null));
        builder =
            builder
                .setType(unionFieldSchema.getType())
                .setMode(unionFieldSchema.getMode())
                .addAllFields(unionFieldSchema.getFieldsList());
        break;
      default:
        elementType = TypeWithNullability.create(schema).getType();
        Optional<LogicalType> logicalType =
            Optional.ofNullable(LogicalTypes.fromSchema(elementType));
        @Nullable
        TableFieldSchema.Type primitiveType =
            logicalType
                .flatMap(AvroGenericRecordToStorageApiProto::logicalTypes)
                .orElse(PRIMITIVE_TYPES.get(elementType.getType()));
        if (primitiveType == null) {
          throw new RuntimeException("Unsupported type " + elementType.getType());
        }
        // a scalar will be required by default, if defined as part of union then
        // caller will set nullability requirements
        builder = builder.setType(primitiveType);
        // parametrized types
        if (logicalType.isPresent() && logicalType.get().getName().equals("decimal")) {
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType.get();
          int precision = decimal.getPrecision();
          int scale = decimal.getScale();
          if (!(precision == 38 && scale == 9) // NUMERIC
              && !(precision == 77 && scale == 38) // BIGNUMERIC
          ) {
            // parametrized type
            builder = builder.setPrecision(precision);
            if (scale != 0) {
              builder = builder.setScale(scale);
            }
          }
        }
    }
    if (builder.getMode() != TableFieldSchema.Mode.REPEATED) {
      if (TypeWithNullability.create(schema).isNullable()) {
        builder = builder.setMode(TableFieldSchema.Mode.NULLABLE);
      } else {
        builder = builder.setMode(TableFieldSchema.Mode.REQUIRED);
      }
    }
    if (field.doc() != null) {
      builder = builder.setDescription(field.doc());
    }
    return builder.build();
  }

  @Nullable
  private static Object messageValueFromGenericRecordValue(
      FieldDescriptor fieldDescriptor, Schema.Field avroField, String name, GenericRecord record) {
    @Nullable Object value = record.get(name);
    if (value == null) {
      if (fieldDescriptor.isOptional()
          || avroField.schema().getTypes().stream()
              .anyMatch(t -> t.getType() == Schema.Type.NULL)) {
        return null;
      } else {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field " + fieldDescriptor.getName());
      }
    }
    return toProtoValue(fieldDescriptor, avroField.schema(), value);
  }

  private static Object toProtoValue(
      FieldDescriptor fieldDescriptor, Schema avroSchema, Object value) {
    switch (avroSchema.getType()) {
      case RECORD:
        return messageFromGenericRecord(
            fieldDescriptor.getMessageType(), (GenericRecord) value, null, -1);
      case ARRAY:
        Iterable<Object> iterable = (Iterable<Object>) value;
        @Nullable Schema arrayElementType = avroSchema.getElementType();
        if (arrayElementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        return StreamSupport.stream(iterable.spliterator(), false)
            .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
            .collect(Collectors.toList());
      case UNION:
        TypeWithNullability type = TypeWithNullability.create(avroSchema);
        Preconditions.checkState(
            type.getType().getType() != Schema.Type.UNION,
            "Multiple non-null union types are not supported.");
        return toProtoValue(fieldDescriptor, type.getType(), value);
      case MAP:
        Map<CharSequence, Object> map = (Map<CharSequence, Object>) value;
        Schema valueType = Schema.create(avroSchema.getValueType().getType());
        if (valueType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }

        return map.entrySet().stream()
            .map(
                (Map.Entry<CharSequence, Object> entry) ->
                    mapEntryToProtoValue(fieldDescriptor.getMessageType(), valueType, entry))
            .collect(Collectors.toList());
      default:
        return scalarToProtoValue(avroSchema, value);
    }
  }

  static Object mapEntryToProtoValue(
      Descriptor descriptor, Schema valueFieldType, Map.Entry<CharSequence, Object> entryValue) {

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    FieldDescriptor keyFieldDescriptor =
        Preconditions.checkNotNull(descriptor.findFieldByName("key"));
    @Nullable
    Object key =
        toProtoValue(keyFieldDescriptor, Schema.create(Schema.Type.STRING), entryValue.getKey());
    if (key != null) {
      builder.setField(keyFieldDescriptor, key);
    }
    FieldDescriptor valueFieldDescriptor =
        Preconditions.checkNotNull(descriptor.findFieldByName("value"));
    @Nullable
    Object value = toProtoValue(valueFieldDescriptor, valueFieldType, entryValue.getValue());
    if (value != null) {
      builder.setField(valueFieldDescriptor, value);
    }
    return builder.build();
  }

  @VisibleForTesting
  static Object scalarToProtoValue(Schema fieldSchema, Object value) {
    TypeWithNullability type = TypeWithNullability.create(fieldSchema);
    LogicalType logicalType = LogicalTypes.fromSchema(type.getType());
    if (logicalType != null) {
      @Nullable
      BiFunction<LogicalType, Object, Object> logicalTypeEncoder =
          LOGICAL_TYPE_ENCODERS.get(logicalType.getName());
      if (logicalTypeEncoder == null) {
        throw new IllegalArgumentException("Unsupported logical type " + logicalType.getName());
      }
      return logicalTypeEncoder.apply(logicalType, value);
    } else {
      @Nullable Function<Object, Object> encoder = PRIMITIVE_ENCODERS.get(type.getType().getType());
      if (encoder == null) {
        throw new RuntimeException("Unexpected beam type " + fieldSchema);
      }
      return encoder.apply(value);
    }
  }
}
