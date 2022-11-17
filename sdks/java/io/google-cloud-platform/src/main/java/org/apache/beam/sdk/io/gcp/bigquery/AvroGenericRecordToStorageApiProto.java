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

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils.TypeWithNullability;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Functions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.codehaus.jackson.JsonNode;
import org.joda.time.Days;
import org.joda.time.Instant;
import org.joda.time.ReadableInstant;

/**
 * Utility methods for converting Beam {@link Row} objects to dynamic protocol message, for use with
 * the Storage write API.
 */
public class AvroGenericRecordToStorageApiProto {

  static final Map<Schema.Type, FieldDescriptorProto.Type> PRIMITIVE_TYPES
          = ImmutableMap.<Schema.Type, FieldDescriptorProto.Type>builder()
                  .put(Schema.Type.INT, FieldDescriptorProto.Type.TYPE_INT32)
                  .put(Schema.Type.FIXED, FieldDescriptorProto.Type.TYPE_BYTES)
                  .put(Schema.Type.LONG, FieldDescriptorProto.Type.TYPE_INT64)
                  .put(Schema.Type.FLOAT, FieldDescriptorProto.Type.TYPE_FLOAT)
                  .put(Schema.Type.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE)
                  .put(Schema.Type.STRING, FieldDescriptorProto.Type.TYPE_STRING)
                  .put(Schema.Type.BOOLEAN, FieldDescriptorProto.Type.TYPE_BOOL)
                  .put(Schema.Type.ENUM, FieldDescriptorProto.Type.TYPE_STRING)
                  .put(Schema.Type.BYTES, FieldDescriptorProto.Type.TYPE_BYTES)
                  .build();

  // A map of supported logical types to the protobuf field type.
  static final Map<String, FieldDescriptorProto.Type> LOGICAL_TYPES
          = ImmutableMap.<String, FieldDescriptorProto.Type>builder()
                  .put(LogicalTypes.date().getName(), FieldDescriptorProto.Type.TYPE_INT32)
                  .put(LogicalTypes.decimal(1).getName(), FieldDescriptorProto.Type.TYPE_BYTES)
                  .put(LogicalTypes.timeMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64)
                  .put(LogicalTypes.timeMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64)
                  .put(LogicalTypes.timestampMicros().getName(), FieldDescriptorProto.Type.TYPE_INT64)
                  .put(LogicalTypes.timestampMillis().getName(), FieldDescriptorProto.Type.TYPE_INT64)
                  .put(LogicalTypes.uuid().getName(), FieldDescriptorProto.Type.TYPE_STRING)
                  .build();

  static final Map<Schema.Type, Function<Object, Object>> PRIMITIVE_ENCODERS
          = ImmutableMap.<Schema.Type, Function<Object, Object>>builder()
                  .put(Schema.Type.INT, Functions.identity())
                  .put(Schema.Type.FIXED, o -> ByteString.copyFrom(((GenericData.Fixed) o).bytes()))
                  .put(Schema.Type.LONG, Functions.identity())
                  .put(Schema.Type.FLOAT, Function.identity())
                  .put(Schema.Type.DOUBLE, Function.identity())
                  .put(Schema.Type.STRING, Function.identity())
                  .put(Schema.Type.BOOLEAN, Function.identity())
                  .put(Schema.Type.ENUM, o -> o.toString())
                  .put(Schema.Type.BYTES, o -> ByteString.copyFrom((byte[]) o))
                  .build();

  // A map of supported logical types to their encoding functions.
  static final Map<String, BiFunction<LogicalType, Object, Object>> LOGICAL_TYPE_ENCODERS
          = ImmutableMap.<String, BiFunction<LogicalType, Object, Object>>builder()
                  .put(LogicalTypes.date().getName(), (logicalType, value) -> convertDate(value))
                  .put(
                          LogicalTypes.decimal(1).getName(), AvroGenericRecordToStorageApiProto::convertDecimal)
                  .put(
                          LogicalTypes.timestampMicros().getName(),
                          (logicalType, value) -> convertTimestamp(value) * 1000)
                  .put(
                          LogicalTypes.timestampMillis().getName(),
                          (logicalType, value) -> convertTimestamp(value))
                  .put(LogicalTypes.uuid().getName(), (logicalType, value) -> ((UUID) value).toString())
                  .build();

  static Long convertTimestamp(Object value) {
    if (value instanceof ReadableInstant) {
      return ((ReadableInstant) value).getMillis();
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
    BigDecimal bigDecimal
            = new Conversions.DecimalConversion().fromBytes(
                    byteBuffer.duplicate(), 
                    Schema.create(Schema.Type.NULL), // dummy schema, not used
                    logicalType);
    return BeamRowToStorageApiProto.serializeBigDecimalToNumeric(bigDecimal);
  }

  /**
   * Given an Avro Schema, returns a protocol-buffer Descriptor that can be used to write data using
   * the BigQuery Storage API.
   */
  public static Descriptor getDescriptorFromSchema(Schema schema)
          throws DescriptorValidationException {
    DescriptorProto descriptorProto = descriptorSchemaFromAvroSchema(schema);
    FileDescriptorProto fileDescriptorProto
            = FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();
    FileDescriptor fileDescriptor
            = FileDescriptor.buildFrom(fileDescriptorProto, new FileDescriptor[0]);

    return Iterables.getOnlyElement(fileDescriptor.getMessageTypes());
  }

  /**
   * Given a Beam {@link Row} object, returns a protocol-buffer message that can be used to write
   * data using the BigQuery Storage streaming API.
   */
  public static DynamicMessage messageFromGenericRecord(
          Descriptor descriptor, GenericRecord record) {
    Schema schema = record.getSchema();
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    for (Schema.Field field : schema.getFields()) {
      FieldDescriptor fieldDescriptor
              = Preconditions.checkNotNull(descriptor.findFieldByName(field.name().toLowerCase()));
      @Nullable
      Object value
              = messageValueFromGenericRecordValue(fieldDescriptor, field, field.name(), record);
      if (value != null) {
        builder.setField(fieldDescriptor, value);
      }
    }
    return builder.build();
  }

  @VisibleForTesting
  static DescriptorProto descriptorSchemaFromAvroSchema(Schema schema) {
    Preconditions.checkState(!schema.getFields().isEmpty());
    DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
    // Create a unique name for the descriptor ('-' characters cannot be used).
    descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
    int i = 1;
    List<DescriptorProto> nestedTypes = Lists.newArrayList();
    for (Schema.Field field : schema.getFields()) {
      FieldDescriptorProto.Builder fieldDescriptorProtoBuilder
              = fieldDescriptorFromAvroField(field, i++, nestedTypes);
      descriptorBuilder.addField(fieldDescriptorProtoBuilder);
    }
    nestedTypes.forEach(descriptorBuilder::addNestedType);
    return descriptorBuilder.build();
  }

  static DescriptorProto mapDescriptorSchemaFromAvroSchema(
          FieldDescriptorProto.Builder keyFieldDescriptor,
          FieldDescriptorProto.Builder valueFieldDescriptor,
          List<DescriptorProto> nestedTypes) {
    DescriptorProto.Builder descriptorBuilder = DescriptorProto.newBuilder();
    // Create a unique name for the descriptor ('-' characters cannot be used).
    descriptorBuilder.setName("D" + UUID.randomUUID().toString().replace("-", "_"));
    descriptorBuilder.addField(keyFieldDescriptor);
    descriptorBuilder.addField(valueFieldDescriptor);
    nestedTypes.forEach(descriptorBuilder::addNestedType);
    return descriptorBuilder.build();
  }

  private static FieldDescriptorProto.Builder fieldDescriptorFromAvroField(
          Schema.Field field, int fieldNumber, List<DescriptorProto> nestedTypes) {
    @Nullable
    Schema schema = field.schema();
    FieldDescriptorProto.Builder fieldDescriptorBuilder = FieldDescriptorProto.newBuilder();
    fieldDescriptorBuilder = fieldDescriptorBuilder.setName(field.name().toLowerCase());
    fieldDescriptorBuilder = fieldDescriptorBuilder.setNumber(fieldNumber);
    Schema elementType = null;
    switch (schema.getType()) {
      case RECORD:
        if (schema == null) {
          throw new RuntimeException("Unexpected null schema!");
        }
        DescriptorProto nested = descriptorSchemaFromAvroSchema(schema);
        nestedTypes.add(nested);
        fieldDescriptorBuilder
                = fieldDescriptorBuilder
                        .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                        .setTypeName(nested.getName());
        break;
      case ARRAY:
        elementType = TypeWithNullability.create(schema.getElementType()).getType();
        if (elementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        Preconditions.checkState(
                elementType.getType() != Schema.Type.ARRAY, 
                "Nested arrays not supported by BigQuery.");
        return fieldDescriptorFromAvroField(
                new Schema.Field(field.name(), elementType, field.doc(), field.defaultVal()),
                fieldNumber,
                nestedTypes)
                .setLabel(Label.LABEL_REPEATED);
      case MAP:
        Schema keyType = Schema.create(Schema.Type.STRING);
        Schema valueType = TypeWithNullability.create(schema.getElementType()).getType();
        if (valueType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }

        List<DescriptorProto> nestedTypesMap = Lists.newArrayList();

        DescriptorProto nestedMap
                = mapDescriptorSchemaFromAvroSchema(
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        "key", 
                                        keyType, 
                                        "key of the map entry", 
                                        Schema.Field.NULL_VALUE),
                                1,
                                nestedTypesMap),
                        fieldDescriptorFromAvroField(
                                new Schema.Field(
                                        "value", 
                                        valueType, 
                                        "value of the map entry", 
                                        Schema.Field.NULL_VALUE),
                                2,
                                nestedTypesMap),
                        nestedTypesMap);

        nestedTypes.add(nestedMap);
        fieldDescriptorBuilder
                = fieldDescriptorBuilder
                        .setType(Type.TYPE_MESSAGE)
                        .setTypeName(nestedMap.getName());

        return fieldDescriptorBuilder.setLabel(Label.LABEL_REPEATED);
      case UNION:
        elementType = TypeWithNullability.create(schema).getType();
        if (elementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        // check to see if more than one non-null type is defined in the union
        Preconditions.checkState(
                elementType.getType() != Schema.Type.UNION,
                "Multiple non-null union types are not supported.");
        fieldDescriptorBuilder
                = fieldDescriptorFromAvroField(
                        new Schema.Field(field.name(), elementType, field.doc(), field.defaultVal()),
                        fieldNumber,
                        nestedTypes);
        break;
      default:
        elementType = TypeWithNullability.create(schema).getType();
        @Nullable FieldDescriptorProto.Type primitiveType
                = Optional.ofNullable(LogicalTypes.fromSchema(elementType))
                        .map(logicalType -> LOGICAL_TYPES.get(logicalType.getName()))
                        .orElse(PRIMITIVE_TYPES.get(elementType.getType()));
        if (primitiveType == null) {
          throw new RuntimeException("Unsupported type " + elementType.getType());
        }
        // a scalar will be required by default, if defined as part of union then
        // caller will set nullability requirements
        return fieldDescriptorBuilder.setType(primitiveType).setLabel(Label.LABEL_REQUIRED);
    }
    if (TypeWithNullability.create(schema).isNullable()) {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_OPTIONAL);
    } else {
      fieldDescriptorBuilder = fieldDescriptorBuilder.setLabel(Label.LABEL_REQUIRED);
    }
    return fieldDescriptorBuilder;
  }

  @Nullable
  private static Object messageValueFromGenericRecordValue(
          FieldDescriptor fieldDescriptor, Schema.Field avroField, String name, GenericRecord record) {
    @Nullable
    Object value = record.get(name);
    if (value == null) {
      if (fieldDescriptor.isOptional()) {
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
        return messageFromGenericRecord(fieldDescriptor.getMessageType(), (GenericRecord) value);
      case ARRAY:
        List<Object> list = (List<Object>) value;
        @Nullable Schema arrayElementType = avroSchema.getElementType();
        if (arrayElementType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }
        return list.stream()
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
        Schema valueType = TypeWithNullability.create(avroSchema.getElementType()).getType();
        if (valueType == null) {
          throw new RuntimeException("Unexpected null element type!");
        }

        return map.entrySet().stream()
                .map(
                        (Map.Entry<CharSequence, Object> entry)
                        -> mapEntryToProtoValue(
                                fieldDescriptor.getMessageType(), valueType, entry))
                .collect(Collectors.toList());
      default:
        return scalarToProtoValue(avroSchema, value);
    }
  }

  static Object mapEntryToProtoValue(
          Descriptor descriptor,
          Schema valueFieldType,
          Map.Entry<CharSequence, Object> entryValue) {

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
    FieldDescriptor keyFieldDescriptor
            = Preconditions.checkNotNull(descriptor.findFieldByName("key"));
    @Nullable
    Object key = toProtoValue(
            keyFieldDescriptor, Schema.create(Schema.Type.STRING), entryValue.getKey());
    if (key != null) {
      builder.setField(keyFieldDescriptor, key);
    }
    FieldDescriptor valueFieldDescriptor
            = Preconditions.checkNotNull(descriptor.findFieldByName("value"));
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
      BiFunction<LogicalType, Object, Object> logicalTypeEncoder
              = LOGICAL_TYPE_ENCODERS.get(logicalType.getName());
      if (logicalTypeEncoder == null) {
        throw new IllegalArgumentException("Unsupported logical type " + logicalType.getName());
      }
      return logicalTypeEncoder.apply(logicalType, value);
    } else {
      @Nullable
      Function<Object, Object> encoder = PRIMITIVE_ENCODERS.get(type.getType().getType());
      if (encoder == null) {
        throw new RuntimeException("Unexpected beam type " + fieldSchema);
      }
      return encoder.apply(value);
    }
  }
}
