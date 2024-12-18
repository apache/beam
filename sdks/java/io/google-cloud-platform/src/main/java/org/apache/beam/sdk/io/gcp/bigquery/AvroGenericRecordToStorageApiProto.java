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
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/**
 * Utility methods for converting Avro {@link GenericRecord} objects to dynamic protocol message,
 * for use with the Storage write API.
 */
public class AvroGenericRecordToStorageApiProto {
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
  static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
      ImmutableMap.<String, TableFieldSchema.Type>builder()
          .put(LogicalTypes.date().getName(), TableFieldSchema.Type.DATE)
          .put(LogicalTypes.decimal(1).getName(), TableFieldSchema.Type.BIGNUMERIC)
          .put(LogicalTypes.timestampMicros().getName(), TableFieldSchema.Type.TIMESTAMP)
          .put(LogicalTypes.timestampMillis().getName(), TableFieldSchema.Type.TIMESTAMP)
          .put(LogicalTypes.uuid().getName(), TableFieldSchema.Type.STRING)
          .build();

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
          .put(Schema.Type.BYTES, o -> ByteString.copyFrom(((ByteBuffer) o).duplicate()))
          .build();

  // A map of supported logical types to their encoding functions.
  static final Map<String, BiFunction<LogicalType, Object, Object>> LOGICAL_TYPE_ENCODERS =
      ImmutableMap.<String, BiFunction<LogicalType, Object, Object>>builder()
          .put(LogicalTypes.date().getName(), (logicalType, value) -> convertDate(value))
          .put(
              LogicalTypes.decimal(1).getName(), AvroGenericRecordToStorageApiProto::convertDecimal)
          .put(
              LogicalTypes.timestampMicros().getName(),
              (logicalType, value) -> convertTimestamp(value, true))
          .put(
              LogicalTypes.timestampMillis().getName(),
              (logicalType, value) -> convertTimestamp(value, false))
          .put(LogicalTypes.uuid().getName(), (logicalType, value) -> convertUUID(value))
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
    if (value instanceof ReadableInstant) {
      return ((ReadableInstant) value).getMillis() * (micros ? 1000 : 1);
    } else {
      Preconditions.checkArgument(
          value instanceof Long, "Expecting a value as Long type (millis).");
      return (Long) value;
    }
  }

  static Integer convertDate(Object value) {
    if (value instanceof ReadableInstant) {
      return Days.daysBetween(Instant.EPOCH, (ReadableInstant) value).getDays();
    } else {
      Preconditions.checkArgument(
          value instanceof Integer, "Expecting a value as Integer type (days).");
      return (Integer) value;
    }
  }

  static ByteString convertDecimal(LogicalType logicalType, Object value) {
    ByteBuffer byteBuffer = (ByteBuffer) value;
    BigDecimal bigDecimal =
        new Conversions.DecimalConversion()
            .fromBytes(
                byteBuffer.duplicate(),
                Schema.create(Schema.Type.NULL), // dummy schema, not used
                logicalType);
    return BeamRowToStorageApiProto.serializeBigDecimalToNumeric(bigDecimal);
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

  private static TableFieldSchema fieldDescriptorFromAvroField(Schema.Field field) {
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
            fieldDescriptorFromAvroField(new Schema.Field("key", keyType, "key of the map entry"));
        TableFieldSchema valueFieldSchema =
            fieldDescriptorFromAvroField(
                new Schema.Field("value", valueType, "value of the map entry"));
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
            fieldDescriptorFromAvroField(new Schema.Field(field.name(), elementType, field.doc()));
        builder =
            builder
                .setType(unionFieldSchema.getType())
                .setMode(unionFieldSchema.getMode())
                .addAllFields(unionFieldSchema.getFieldsList());
        break;
      default:
        elementType = TypeWithNullability.create(schema).getType();
        @Nullable
        TableFieldSchema.Type primitiveType =
            Optional.ofNullable(LogicalTypes.fromSchema(elementType))
                .map(logicalType -> LOGICAL_TYPES.get(logicalType.getName()))
                .orElse(PRIMITIVE_TYPES.get(elementType.getType()));
        if (primitiveType == null) {
          throw new RuntimeException("Unsupported type " + elementType.getType());
        }
        // a scalar will be required by default, if defined as part of union then
        // caller will set nullability requirements
        builder = builder.setType(primitiveType);
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
