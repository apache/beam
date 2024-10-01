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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
import org.joda.time.ReadableInstant;

/**
 * Utility methods for converting Beam {@link Row} objects to dynamic protocol message, for use with
 * the Storage write API.
 */
public class BeamRowToStorageApiProto {
  // Number of digits after the decimal point supported by the NUMERIC data type.
  private static final int NUMERIC_SCALE = 9;
  // Maximum and minimum allowed values for the NUMERIC data type.
  private static final BigDecimal MAX_NUMERIC_VALUE =
      new BigDecimal("99999999999999999999999999999.999999999");
  private static final BigDecimal MIN_NUMERIC_VALUE =
      new BigDecimal("-99999999999999999999999999999.999999999");

  // TODO(reuvenlax): Support BIGNUMERIC and GEOGRAPHY types.
  static final Map<TypeName, TableFieldSchema.Type> PRIMITIVE_TYPES =
      ImmutableMap.<TypeName, TableFieldSchema.Type>builder()
          .put(TypeName.INT16, TableFieldSchema.Type.INT64)
          .put(TypeName.BYTE, TableFieldSchema.Type.INT64)
          .put(TypeName.INT32, TableFieldSchema.Type.INT64)
          .put(TypeName.INT64, TableFieldSchema.Type.INT64)
          .put(TypeName.FLOAT, TableFieldSchema.Type.DOUBLE)
          .put(TypeName.DOUBLE, TableFieldSchema.Type.DOUBLE)
          .put(TypeName.STRING, TableFieldSchema.Type.STRING)
          .put(TypeName.BOOLEAN, TableFieldSchema.Type.BOOL)
          .put(TypeName.DATETIME, TableFieldSchema.Type.DATETIME)
          .put(TypeName.BYTES, TableFieldSchema.Type.BYTES)
          .put(TypeName.DECIMAL, TableFieldSchema.Type.BIGNUMERIC)
          .build();

  // A map of supported logical types to the protobuf field type.
  static final Map<String, TableFieldSchema.Type> LOGICAL_TYPES =
      ImmutableMap.<String, TableFieldSchema.Type>builder()
          .put(SqlTypes.DATE.getIdentifier(), TableFieldSchema.Type.DATE)
          .put(SqlTypes.TIME.getIdentifier(), TableFieldSchema.Type.TIME)
          .put(SqlTypes.DATETIME.getIdentifier(), TableFieldSchema.Type.DATETIME)
          .put(SqlTypes.TIMESTAMP.getIdentifier(), TableFieldSchema.Type.TIMESTAMP)
          .put(EnumerationType.IDENTIFIER, TableFieldSchema.Type.STRING)
          .build();

  static final Map<TypeName, Function<Object, Object>> PRIMITIVE_ENCODERS =
      ImmutableMap.<TypeName, Function<Object, Object>>builder()
          .put(TypeName.INT16, o -> ((Short) o).longValue())
          .put(TypeName.BYTE, o -> ((Byte) o).longValue())
          .put(TypeName.INT32, o -> ((Integer) o).longValue())
          .put(TypeName.INT64, Functions.identity())
          .put(TypeName.FLOAT, o -> Double.valueOf(o.toString()))
          .put(TypeName.DOUBLE, Function.identity())
          .put(TypeName.STRING, Function.identity())
          .put(TypeName.BOOLEAN, Function.identity())
          // A Beam DATETIME is actually a timestamp, not a DateTime.
          .put(TypeName.DATETIME, o -> ((ReadableInstant) o).getMillis() * 1000)
          .put(TypeName.BYTES, BeamRowToStorageApiProto::toProtoByteString)
          .put(TypeName.DECIMAL, o -> serializeBigDecimalToNumeric((BigDecimal) o))
          .build();

  private static ByteString toProtoByteString(Object o) {
    if (o instanceof byte[]) {
      return ByteString.copyFrom((byte[]) o);
    } else if (o instanceof ByteBuffer) {
      return ByteString.copyFrom((ByteBuffer) o);
    } else if (o instanceof String) {
      return ByteString.copyFromUtf8((String) o);
    } else {
      throw new ClassCastException(
          String.format(
              "Cannot cast %s to a compatible object to build ByteString.", o.getClass()));
    }
  }

  // A map of supported logical types to their encoding functions.
  static final Map<String, BiFunction<LogicalType<?, ?>, Object, Object>> LOGICAL_TYPE_ENCODERS =
      ImmutableMap.<String, BiFunction<LogicalType<?, ?>, Object, Object>>builder()
          .put(
              SqlTypes.DATE.getIdentifier(),
              (logicalType, value) -> (int) ((LocalDate) value).toEpochDay())
          .put(
              SqlTypes.TIME.getIdentifier(),
              (logicalType, value) -> CivilTimeEncoder.encodePacked64TimeMicros((LocalTime) value))
          .put(
              SqlTypes.DATETIME.getIdentifier(),
              (logicalType, value) ->
                  CivilTimeEncoder.encodePacked64DatetimeMicros((LocalDateTime) value))
          .put(
              SqlTypes.TIMESTAMP.getIdentifier(),
              (logicalType, value) -> (ChronoUnit.MICROS.between(Instant.EPOCH, (Instant) value)))
          .put(
              EnumerationType.IDENTIFIER,
              (logicalType, value) ->
                  ((EnumerationType) logicalType).toString((EnumerationType.Value) value))
          .build();

  /**
   * Forwards (@param changeSequenceNum) to {@link #messageFromBeamRow(Descriptor, Row, String,
   * String)} via {@link Long#toHexString}.
   */
  public static DynamicMessage messageFromBeamRow(
      Descriptor descriptor, Row row, @Nullable String changeType, long changeSequenceNum) {
    return messageFromBeamRow(descriptor, row, changeType, Long.toHexString(changeSequenceNum));
  }

  /**
   * Given a Beam {@link Row} object, returns a protocol-buffer message that can be used to write
   * data using the BigQuery Storage streaming API.
   */
  public static DynamicMessage messageFromBeamRow(
      Descriptor descriptor,
      Row row,
      @Nullable String changeType,
      @Nullable String changeSequenceNum) {
    Schema beamSchema = row.getSchema();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (int i = 0; i < row.getFieldCount(); ++i) {
      Field beamField = beamSchema.getField(i);
      FieldDescriptor fieldDescriptor =
          Preconditions.checkNotNull(
              descriptor.findFieldByName(beamField.getName().toLowerCase()),
              beamField.getName().toLowerCase());
      @Nullable Object value = messageValueFromRowValue(fieldDescriptor, beamField, i, row);
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

  @VisibleForTesting
  static TableSchema protoTableSchemaFromBeamSchema(Schema schema) {
    Preconditions.checkState(schema.getFieldCount() > 0);

    TableSchema.Builder builder = TableSchema.newBuilder();
    for (Field field : schema.getFields()) {
      builder.addFields(fieldDescriptorFromBeamField(field));
    }
    return builder.build();
  }

  private static TableFieldSchema fieldDescriptorFromBeamField(Field field) {
    TableFieldSchema.Builder builder = TableFieldSchema.newBuilder();
    if (StorageApiCDC.COLUMNS.contains(field.getName())) {
      throw new RuntimeException("Reserved field name " + field.getName() + " in user schema.");
    }
    builder = builder.setName(field.getName().toLowerCase());

    switch (field.getType().getTypeName()) {
      case ROW:
        @Nullable Schema rowSchema = field.getType().getRowSchema();
        if (rowSchema == null) {
          throw new RuntimeException("Unexpected null schema!");
        }
        builder = builder.setType(TableFieldSchema.Type.STRUCT);
        for (Schema.Field nestedField : rowSchema.getFields()) {
          builder = builder.addFields(fieldDescriptorFromBeamField(nestedField));
        }
        break;
      case ARRAY:
      case ITERABLE:
        @Nullable FieldType elementType = field.getType().getCollectionElementType();
        if (elementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        Preconditions.checkState(
            !Preconditions.checkNotNull(elementType.getTypeName()).isCollectionType(),
            "Nested arrays not supported by BigQuery.");
        TableFieldSchema elementFieldSchema =
            fieldDescriptorFromBeamField(Field.of(field.getName(), elementType));
        builder = builder.setType(elementFieldSchema.getType());
        builder.addAllFields(elementFieldSchema.getFieldsList());
        builder = builder.setMode(TableFieldSchema.Mode.REPEATED);
        break;
      case LOGICAL_TYPE:
        @Nullable LogicalType<?, ?> logicalType = field.getType().getLogicalType();
        if (logicalType == null) {
          throw new RuntimeException("Unexpected null logical type " + field.getType());
        }
        @Nullable TableFieldSchema.Type type = LOGICAL_TYPES.get(logicalType.getIdentifier());
        if (type == null) {
          throw new RuntimeException("Unsupported logical type " + field.getType());
        }
        builder = builder.setType(type);
        break;
      case MAP:
        throw new RuntimeException("Map types not supported by BigQuery.");
      default:
        @Nullable
        TableFieldSchema.Type primitiveType = PRIMITIVE_TYPES.get(field.getType().getTypeName());
        if (primitiveType == null) {
          throw new RuntimeException("Unsupported type " + field.getType());
        }
        builder = builder.setType(primitiveType);
    }
    if (builder.getMode() != TableFieldSchema.Mode.REPEATED) {
      if (field.getType().getNullable()) {
        builder = builder.setMode(TableFieldSchema.Mode.NULLABLE);
      } else {
        builder = builder.setMode(TableFieldSchema.Mode.REQUIRED);
      }
    }
    if (field.getDescription() != null) {
      builder = builder.setDescription(field.getDescription());
    }
    return builder.build();
  }

  @Nullable
  private static Object messageValueFromRowValue(
      FieldDescriptor fieldDescriptor, Field beamField, int index, Row row) {
    @Nullable Object value = row.getValue(index);
    if (value == null) {
      if (fieldDescriptor.isOptional()) {
        return null;
      } else if (fieldDescriptor.isRepeated()) {
        return Collections.emptyList();
      } else {
        throw new IllegalArgumentException(
            "Received null value for non-nullable field " + fieldDescriptor.getName());
      }
    }
    return toProtoValue(fieldDescriptor, beamField.getType(), value);
  }

  private static Object toProtoValue(
      FieldDescriptor fieldDescriptor, FieldType beamFieldType, Object value) {
    switch (beamFieldType.getTypeName()) {
      case ROW:
        return messageFromBeamRow(fieldDescriptor.getMessageType(), (Row) value, null, -1);
      case ARRAY:
        List<Object> list = (List<Object>) value;
        @Nullable FieldType arrayElementType = beamFieldType.getCollectionElementType();
        if (arrayElementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        return list.stream()
            .map(v -> toProtoValue(fieldDescriptor, arrayElementType, v))
            .collect(Collectors.toList());
      case ITERABLE:
        Iterable<Object> iterable = (Iterable<Object>) value;
        @Nullable FieldType iterableElementType = beamFieldType.getCollectionElementType();
        if (iterableElementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        return StreamSupport.stream(iterable.spliterator(), false)
            .map(v -> toProtoValue(fieldDescriptor, iterableElementType, v))
            .collect(Collectors.toList());
      case MAP:
        throw new RuntimeException("Map types not supported by BigQuery.");
      default:
        return scalarToProtoValue(beamFieldType, value);
    }
  }

  @VisibleForTesting
  static Object scalarToProtoValue(FieldType beamFieldType, Object value) {
    if (beamFieldType.getTypeName() == TypeName.LOGICAL_TYPE) {
      @Nullable LogicalType<?, ?> logicalType = beamFieldType.getLogicalType();
      if (logicalType == null) {
        throw new RuntimeException("Unexpectedly null logical type " + beamFieldType);
      }
      @Nullable
      BiFunction<LogicalType<?, ?>, Object, Object> logicalTypeEncoder =
          LOGICAL_TYPE_ENCODERS.get(logicalType.getIdentifier());
      if (logicalTypeEncoder == null) {
        throw new RuntimeException("Unsupported logical type " + logicalType.getIdentifier());
      }
      return logicalTypeEncoder.apply(logicalType, value);
    } else {
      @Nullable
      Function<Object, Object> encoder = PRIMITIVE_ENCODERS.get(beamFieldType.getTypeName());
      if (encoder == null) {
        throw new RuntimeException("Unexpected beam type " + beamFieldType);
      }
      return encoder.apply(value);
    }
  }

  static ByteString serializeBigDecimalToNumeric(BigDecimal o) {
    return serializeBigDecimal(o, NUMERIC_SCALE, MAX_NUMERIC_VALUE, MIN_NUMERIC_VALUE, "Numeric");
  }

  private static ByteString serializeBigDecimal(
      BigDecimal v, int scale, BigDecimal maxValue, BigDecimal minValue, String typeName) {
    if (v.scale() > scale) {
      throw new IllegalArgumentException(
          typeName + " scale cannot exceed " + scale + ": " + v.toPlainString());
    }
    if (v.compareTo(maxValue) > 0 || v.compareTo(minValue) < 0) {
      throw new IllegalArgumentException(typeName + " overflow: " + v.toPlainString());
    }

    byte[] bytes = v.setScale(scale).unscaledValue().toByteArray();
    // NUMERIC/BIGNUMERIC values are serialized as scaled integers in two's complement form in
    // little endian
    // order. BigInteger requires the same encoding but in big endian order, therefore we must
    // reverse the bytes that come from the proto.
    Bytes.reverse(bytes);
    return ByteString.copyFrom(bytes);
  }
}
