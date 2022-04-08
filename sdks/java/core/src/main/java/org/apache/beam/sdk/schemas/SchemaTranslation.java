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
package org.apache.beam.sdk.schemas;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaApi.ArrayTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.AtomicTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.FieldValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.IterableTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.LogicalTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeEntry;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.SchemaLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.UnknownLogicalType;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p43p2.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility methods for translating schemas. */
@Experimental(Kind.SCHEMAS)
@SuppressWarnings({
  "nullness", // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  "rawtypes"
})
public class SchemaTranslation {

  private static final String URN_BEAM_LOGICAL_DATETIME = "beam:logical_type:datetime:v1";
  private static final String URN_BEAM_LOGICAL_DECIMAL = "beam:logical_type:decimal:v1";
  private static final String URN_BEAM_LOGICAL_JAVASDK = "beam:logical_type:javasdk:v1";

  // TODO(BEAM-7855): Populate this with a LogicalTypeRegistrar, which includes a way to construct
  // the LogicalType given an argument.
  private static final ImmutableMap<String, Class<? extends LogicalType<?, ?>>>
      STANDARD_LOGICAL_TYPES =
          ImmutableMap.<String, Class<? extends LogicalType<?, ?>>>builder()
              .put(MicrosInstant.IDENTIFIER, MicrosInstant.class)
              .put(SchemaLogicalType.IDENTIFIER, SchemaLogicalType.class)
              .build();

  public static SchemaApi.Schema schemaToProto(Schema schema, boolean serializeLogicalType) {
    String uuid = schema.getUUID() != null ? schema.getUUID().toString() : "";
    SchemaApi.Schema.Builder builder = SchemaApi.Schema.newBuilder().setId(uuid);
    for (Field field : schema.getFields()) {
      SchemaApi.Field protoField =
          fieldToProto(
              field,
              schema.indexOf(field.getName()),
              schema.getEncodingPositions().get(field.getName()),
              serializeLogicalType);
      builder.addFields(protoField);
    }
    builder.addAllOptions(optionsToProto(schema.getOptions()));
    return builder.build();
  }

  private static SchemaApi.Field fieldToProto(
      Field field, int fieldId, int position, boolean serializeLogicalType) {
    return SchemaApi.Field.newBuilder()
        .setName(field.getName())
        .setDescription(field.getDescription())
        .setType(fieldTypeToProto(field.getType(), serializeLogicalType))
        .setId(fieldId)
        .setEncodingPosition(position)
        .addAllOptions(optionsToProto(field.getOptions()))
        .build();
  }

  private static SchemaApi.FieldType fieldTypeToProto(
      FieldType fieldType, boolean serializeLogicalType) {
    SchemaApi.FieldType.Builder builder = SchemaApi.FieldType.newBuilder();
    switch (fieldType.getTypeName()) {
      case ROW:
        builder.setRowType(
            SchemaApi.RowType.newBuilder()
                .setSchema(schemaToProto(fieldType.getRowSchema(), serializeLogicalType)));
        break;

      case ARRAY:
        builder.setArrayType(
            SchemaApi.ArrayType.newBuilder()
                .setElementType(
                    fieldTypeToProto(fieldType.getCollectionElementType(), serializeLogicalType)));
        break;

      case ITERABLE:
        builder.setIterableType(
            SchemaApi.IterableType.newBuilder()
                .setElementType(
                    fieldTypeToProto(fieldType.getCollectionElementType(), serializeLogicalType)));
        break;

      case MAP:
        builder.setMapType(
            SchemaApi.MapType.newBuilder()
                .setKeyType(fieldTypeToProto(fieldType.getMapKeyType(), serializeLogicalType))
                .setValueType(fieldTypeToProto(fieldType.getMapValueType(), serializeLogicalType))
                .build());
        break;

      case LOGICAL_TYPE:
        LogicalType logicalType = fieldType.getLogicalType();
        SchemaApi.LogicalType.Builder logicalTypeBuilder;
        if (STANDARD_LOGICAL_TYPES.containsKey(logicalType.getIdentifier())) {
          Preconditions.checkArgument(
              logicalType.getArgumentType() == null,
              "Logical type '%s' cannot be used as a logical type, it has a non-null argument type.",
              logicalType.getIdentifier());
          logicalTypeBuilder =
              SchemaApi.LogicalType.newBuilder()
                  .setRepresentation(
                      fieldTypeToProto(logicalType.getBaseType(), serializeLogicalType))
                  .setUrn(logicalType.getIdentifier());
        } else if (logicalType instanceof UnknownLogicalType) {
          logicalTypeBuilder =
              SchemaApi.LogicalType.newBuilder()
                  .setUrn(logicalType.getIdentifier())
                  .setPayload(ByteString.copyFrom(((UnknownLogicalType) logicalType).getPayload()))
                  .setRepresentation(
                      fieldTypeToProto(logicalType.getBaseType(), serializeLogicalType));

          if (logicalType.getArgumentType() != null) {
            logicalTypeBuilder
                .setArgumentType(
                    fieldTypeToProto(logicalType.getArgumentType(), serializeLogicalType))
                .setArgument(
                    fieldValueToProto(logicalType.getArgumentType(), logicalType.getArgument()));
          }
        } else {
          logicalTypeBuilder =
              SchemaApi.LogicalType.newBuilder()
                  .setRepresentation(
                      fieldTypeToProto(logicalType.getBaseType(), serializeLogicalType))
                  // TODO(BEAM-7855): "javasdk" types should only be a last resort. Types defined in
                  // Beam should have their own URN, and there should be a mechanism for users to
                  // register their own types by URN.
                  .setUrn(URN_BEAM_LOGICAL_JAVASDK);
          if (logicalType.getArgumentType() != null) {
            logicalTypeBuilder =
                logicalTypeBuilder
                    .setArgumentType(
                        fieldTypeToProto(logicalType.getArgumentType(), serializeLogicalType))
                    .setArgument(
                        fieldValueToProto(
                            logicalType.getArgumentType(), logicalType.getArgument()));
          }
          if (serializeLogicalType) {
            logicalTypeBuilder =
                logicalTypeBuilder.setPayload(
                    ByteString.copyFrom(SerializableUtils.serializeToByteArray(logicalType)));
          }
        }
        builder.setLogicalType(logicalTypeBuilder.build());
        break;
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
      case DATETIME:
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DATETIME)
                .setRepresentation(fieldTypeToProto(FieldType.INT64, serializeLogicalType))
                .build());
        break;
      case DECIMAL:
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DECIMAL)
                .setRepresentation(fieldTypeToProto(FieldType.BYTES, serializeLogicalType))
                .build());
        break;
      case BYTE:
        builder.setAtomicType(SchemaApi.AtomicType.BYTE);
        break;
      case INT16:
        builder.setAtomicType(SchemaApi.AtomicType.INT16);
        break;
      case INT32:
        builder.setAtomicType(SchemaApi.AtomicType.INT32);
        break;
      case INT64:
        builder.setAtomicType(SchemaApi.AtomicType.INT64);
        break;
      case FLOAT:
        builder.setAtomicType(SchemaApi.AtomicType.FLOAT);
        break;
      case DOUBLE:
        builder.setAtomicType(SchemaApi.AtomicType.DOUBLE);
        break;
      case STRING:
        builder.setAtomicType(SchemaApi.AtomicType.STRING);
        break;
      case BOOLEAN:
        builder.setAtomicType(SchemaApi.AtomicType.BOOLEAN);
        break;
      case BYTES:
        builder.setAtomicType(SchemaApi.AtomicType.BYTES);
        break;
    }
    builder.setNullable(fieldType.getNullable());
    return builder.build();
  }

  public static Schema schemaFromProto(SchemaApi.Schema protoSchema) {
    Schema.Builder builder = Schema.builder();
    Map<String, Integer> encodingLocationMap = Maps.newHashMap();
    for (SchemaApi.Field protoField : protoSchema.getFieldsList()) {
      Field field;
      try {
        field = fieldFromProto(protoField);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to decode Schema due to an error decoding Field proto:\n\n" + protoField, e);
      }
      builder.addField(field);
      encodingLocationMap.put(protoField.getName(), protoField.getEncodingPosition());
    }
    builder.setOptions(optionsFromProto(protoSchema.getOptionsList()));
    Schema schema = builder.build();

    Preconditions.checkState(encodingLocationMap.size() == schema.getFieldCount());
    long dinstictEncodingPositions = encodingLocationMap.values().stream().distinct().count();
    Preconditions.checkState(dinstictEncodingPositions <= schema.getFieldCount());
    if (dinstictEncodingPositions < schema.getFieldCount() && schema.getFieldCount() > 0) {
      // This means that encoding positions were not specified in the proto. Generally, we don't
      // expect this to happen,
      // but if it does happen, we expect none to be specified - in which case the should all be
      // zero.
      Preconditions.checkState(dinstictEncodingPositions == 1);
    } else if (protoSchema.getEncodingPositionsSet()) {
      schema.setEncodingPositions(encodingLocationMap);
    }
    if (!protoSchema.getId().isEmpty()) {
      schema.setUUID(UUID.fromString(protoSchema.getId()));
    }
    return schema;
  }

  private static Field fieldFromProto(SchemaApi.Field protoField) {
    return Field.of(protoField.getName(), fieldTypeFromProto(protoField.getType()))
        .withOptions(optionsFromProto(protoField.getOptionsList()))
        .withDescription(protoField.getDescription());
  }

  private static FieldType fieldTypeFromProto(SchemaApi.FieldType protoFieldType) {
    FieldType fieldType = fieldTypeFromProtoWithoutNullable(protoFieldType);

    if (protoFieldType.getNullable()) {
      fieldType = fieldType.withNullable(true);
    }

    return fieldType;
  }

  private static FieldType fieldTypeFromProtoWithoutNullable(SchemaApi.FieldType protoFieldType) {
    switch (protoFieldType.getTypeInfoCase()) {
      case ATOMIC_TYPE:
        switch (protoFieldType.getAtomicType()) {
          case BYTE:
            return FieldType.of(TypeName.BYTE);
          case INT16:
            return FieldType.of(TypeName.INT16);
          case INT32:
            return FieldType.of(TypeName.INT32);
          case INT64:
            return FieldType.of(TypeName.INT64);
          case FLOAT:
            return FieldType.of(TypeName.FLOAT);
          case DOUBLE:
            return FieldType.of(TypeName.DOUBLE);
          case STRING:
            return FieldType.of(TypeName.STRING);
          case BOOLEAN:
            return FieldType.of(TypeName.BOOLEAN);
          case BYTES:
            return FieldType.of(TypeName.BYTES);
          case UNSPECIFIED:
            throw new IllegalArgumentException("Encountered UNSPECIFIED AtomicType");
          default:
            throw new IllegalArgumentException(
                "Encountered unknown AtomicType: " + protoFieldType.getAtomicType());
        }
      case ROW_TYPE:
        return FieldType.row(schemaFromProto(protoFieldType.getRowType().getSchema()));
      case ARRAY_TYPE:
        return FieldType.array(fieldTypeFromProto(protoFieldType.getArrayType().getElementType()));
      case ITERABLE_TYPE:
        return FieldType.iterable(
            fieldTypeFromProto(protoFieldType.getIterableType().getElementType()));
      case MAP_TYPE:
        return FieldType.map(
            fieldTypeFromProto(protoFieldType.getMapType().getKeyType()),
            fieldTypeFromProto(protoFieldType.getMapType().getValueType()));
      case LOGICAL_TYPE:
        String urn = protoFieldType.getLogicalType().getUrn();
        SchemaApi.LogicalType logicalType = protoFieldType.getLogicalType();
        Class<? extends LogicalType<?, ?>> logicalTypeClass = STANDARD_LOGICAL_TYPES.get(urn);
        if (logicalTypeClass != null) {
          try {
            return FieldType.logicalType(logicalTypeClass.getConstructor().newInstance());
          } catch (NoSuchMethodException e) {
            throw new RuntimeException(
                String.format(
                    "Standard logical type '%s' does not have a zero-argument constructor.", urn),
                e);
          } catch (IllegalAccessException e) {
            throw new RuntimeException(
                String.format(
                    "Standard logical type '%s' has a zero-argument constructor, but it is not accessible.",
                    urn),
                e);
          } catch (ReflectiveOperationException e) {
            throw new RuntimeException(
                String.format(
                    "Error instantiating logical type '%s' with zero-argument constructor.", urn),
                e);
          }
        }
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
        if (urn.equals(URN_BEAM_LOGICAL_DATETIME)) {
          return FieldType.DATETIME;
        } else if (urn.equals(URN_BEAM_LOGICAL_DECIMAL)) {
          return FieldType.DECIMAL;
        } else if (urn.equals(URN_BEAM_LOGICAL_JAVASDK)) {
          return FieldType.logicalType(
              (LogicalType)
                  SerializableUtils.deserializeFromByteArray(
                      logicalType.getPayload().toByteArray(), "logicalType"));
        } else {
          @Nullable FieldType argumentType = null;
          @Nullable Object argumentValue = null;
          if (logicalType.hasArgumentType()) {
            argumentType = fieldTypeFromProto(logicalType.getArgumentType());
            argumentValue = fieldValueFromProto(argumentType, logicalType.getArgument());
          }
          return FieldType.logicalType(
              new UnknownLogicalType(
                  urn,
                  logicalType.getPayload().toByteArray(),
                  argumentType,
                  argumentValue,
                  fieldTypeFromProto(logicalType.getRepresentation())));
        }
      default:
        throw new IllegalArgumentException(
            "Unexpected type_info: " + protoFieldType.getTypeInfoCase());
    }
  }

  public static SchemaApi.Row rowToProto(Row row) {
    SchemaApi.Row.Builder builder = SchemaApi.Row.newBuilder();
    for (int i = 0; i < row.getFieldCount(); ++i) {
      builder.addValues(fieldValueToProto(row.getSchema().getField(i).getType(), row.getValue(i)));
    }
    return builder.build();
  }

  public static Object rowFromProto(SchemaApi.Row row, FieldType fieldType) {
    Row.Builder builder = Row.withSchema(fieldType.getRowSchema());
    for (int i = 0; i < row.getValuesCount(); ++i) {
      builder.addValue(
          fieldValueFromProto(fieldType.getRowSchema().getField(i).getType(), row.getValues(i)));
    }
    return builder.build();
  }

  static SchemaApi.FieldValue fieldValueToProto(FieldType fieldType, Object value) {
    FieldValue.Builder builder = FieldValue.newBuilder();
    if (value == null) {
      if (fieldType.getNullable()) {
        return builder.build();
      } else {
        throw new RuntimeException("Null value found for field that doesn't support nulls.");
      }
    }

    switch (fieldType.getTypeName()) {
      case ARRAY:
        return builder
            .setArrayValue(
                arrayValueToProto(fieldType.getCollectionElementType(), (Iterable) value))
            .build();
      case ITERABLE:
        return builder
            .setIterableValue(
                iterableValueToProto(fieldType.getCollectionElementType(), (Iterable) value))
            .build();
      case MAP:
        return builder
            .setMapValue(
                mapToProto(fieldType.getMapKeyType(), fieldType.getMapValueType(), (Map) value))
            .build();
      case ROW:
        return builder.setRowValue(rowToProto((Row) value)).build();
      case DATETIME:
        return builder
            .setLogicalTypeValue(logicalTypeToProto(FieldType.INT64, fieldType, value))
            .build();
      case DECIMAL:
        return builder
            .setLogicalTypeValue(logicalTypeToProto(FieldType.BYTES, fieldType, value))
            .build();
      case LOGICAL_TYPE:
        return builder
            .setLogicalTypeValue(logicalTypeToProto(fieldType.getLogicalType(), value))
            .build();
      default:
        return builder.setAtomicValue(primitiveRowFieldToProto(fieldType, value)).build();
    }
  }

  /** Returns if the given field is null and throws exception if it is and can't be. */
  static boolean isNullFieldValueFromProto(FieldType fieldType, boolean hasNonNullValue) {
    if (!hasNonNullValue && !fieldType.getNullable()) {
      throw new RuntimeException("FieldTypeValue has no value but the field cannot be null.");
    }
    return !hasNonNullValue;
  }

  static Object fieldValueFromProto(FieldType fieldType, SchemaApi.FieldValue value) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
        if (isNullFieldValueFromProto(fieldType, value.hasArrayValue())) {
          return null;
        }
        return arrayValueFromProto(fieldType.getCollectionElementType(), value.getArrayValue());
      case ITERABLE:
        if (isNullFieldValueFromProto(fieldType, value.hasIterableValue())) {
          return null;
        }
        return iterableValueFromProto(
            fieldType.getCollectionElementType(), value.getIterableValue());
      case MAP:
        if (isNullFieldValueFromProto(fieldType, value.hasMapValue())) {
          return null;
        }
        return mapFromProto(
            fieldType.getMapKeyType(), fieldType.getMapValueType(), value.getMapValue());
      case ROW:
        if (isNullFieldValueFromProto(fieldType, value.hasRowValue())) {
          return null;
        }
        return rowFromProto(value.getRowValue(), fieldType);
      case LOGICAL_TYPE:
        if (isNullFieldValueFromProto(fieldType, value.hasLogicalTypeValue())) {
          return null;
        }
        return logicalTypeFromProto(fieldType.getLogicalType(), value);
      case DATETIME:
        if (isNullFieldValueFromProto(fieldType, value.hasLogicalTypeValue())) {
          return null;
        }
        return logicalTypeFromProto(FieldType.INT64, fieldType, value.getLogicalTypeValue());
      case DECIMAL:
        if (isNullFieldValueFromProto(fieldType, value.hasLogicalTypeValue())) {
          return null;
        }
        return logicalTypeFromProto(FieldType.BYTES, fieldType, value.getLogicalTypeValue());
      default:
        if (isNullFieldValueFromProto(fieldType, value.hasAtomicValue())) {
          return null;
        }
        return primitiveFromProto(fieldType, value.getAtomicValue());
    }
  }

  private static SchemaApi.ArrayTypeValue arrayValueToProto(
      FieldType elementType, Iterable values) {
    return ArrayTypeValue.newBuilder()
        .addAllElement(Iterables.transform(values, e -> fieldValueToProto(elementType, e)))
        .build();
  }

  private static Iterable arrayValueFromProto(
      FieldType elementType, SchemaApi.ArrayTypeValue values) {
    return values.getElementList().stream()
        .map(e -> fieldValueFromProto(elementType, e))
        .collect(Collectors.toList());
  }

  private static SchemaApi.IterableTypeValue iterableValueToProto(
      FieldType elementType, Iterable values) {
    return IterableTypeValue.newBuilder()
        .addAllElement(Iterables.transform(values, e -> fieldValueToProto(elementType, e)))
        .build();
  }

  private static Object iterableValueFromProto(FieldType elementType, IterableTypeValue values) {
    return values.getElementList().stream()
        .map(e -> fieldValueFromProto(elementType, e))
        .collect(Collectors.toList());
  }

  private static SchemaApi.MapTypeValue mapToProto(
      FieldType keyType, FieldType valueType, Map<Object, Object> map) {
    MapTypeValue.Builder builder = MapTypeValue.newBuilder();
    for (Map.Entry entry : map.entrySet()) {
      MapTypeEntry mapProtoEntry =
          MapTypeEntry.newBuilder()
              .setKey(fieldValueToProto(keyType, entry.getKey()))
              .setValue(fieldValueToProto(valueType, entry.getValue()))
              .build();
      builder.addEntries(mapProtoEntry);
    }
    return builder.build();
  }

  private static Object mapFromProto(
      FieldType mapKeyType, FieldType mapValueType, MapTypeValue mapValue) {
    return mapValue.getEntriesList().stream()
        .collect(
            Collectors.toMap(
                entry -> fieldValueFromProto(mapKeyType, entry.getKey()),
                entry -> fieldValueFromProto(mapValueType, entry.getValue())));
  }

  /** Converts logical type value from proto using a default type coder. */
  private static Object logicalTypeFromProto(
      FieldType baseType, FieldType inputType, LogicalTypeValue value) {
    try {
      PipedInputStream in = new PipedInputStream();
      DataOutputStream stream = new DataOutputStream(new PipedOutputStream(in));
      switch (baseType.getTypeName()) {
        case INT64:
          stream.writeLong(value.getValue().getAtomicValue().getInt64());
          break;
        case BYTES:
          stream.write(value.getValue().getAtomicValue().getBytes().toByteArray());
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported underlying type for parsing logical type via coder.");
      }
      stream.close();
      return SchemaCoderHelpers.coderForFieldType(inputType).decode(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts logical type value to a proto using a default type coder. */
  private static LogicalTypeValue logicalTypeToProto(
      FieldType baseType, FieldType inputType, Object value) {
    try {
      PipedInputStream in = new PipedInputStream();
      PipedOutputStream out = new PipedOutputStream(in);
      SchemaCoderHelpers.coderForFieldType(inputType).encode(value, out);
      out.close(); // Close required for toByteArray.
      Object baseObject;
      switch (baseType.getTypeName()) {
        case INT64:
          baseObject = new DataInputStream(in).readLong();
          break;
        case BYTES:
          baseObject = ByteStreams.toByteArray(in);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported underlying type for producing LogicalType via coder.");
      }
      return LogicalTypeValue.newBuilder()
          .setValue(fieldValueToProto(baseType, baseObject))
          .build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static LogicalTypeValue logicalTypeToProto(LogicalType logicalType, Object value) {
    return LogicalTypeValue.newBuilder()
        .setValue(
            fieldValueToProto(
                logicalType.getBaseType(), SchemaUtils.toLogicalBaseType(logicalType, value)))
        .build();
  }

  private static Object logicalTypeFromProto(
      LogicalType logicalType, SchemaApi.FieldValue logicalValue) {
    return SchemaUtils.toLogicalInputType(
        logicalType,
        fieldValueFromProto(
            logicalType.getBaseType(), logicalValue.getLogicalTypeValue().getValue()));
  }

  private static AtomicTypeValue primitiveRowFieldToProto(FieldType fieldType, Object value) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        return AtomicTypeValue.newBuilder().setByte((byte) value).build();
      case INT16:
        return AtomicTypeValue.newBuilder().setInt16((short) value).build();
      case INT32:
        return AtomicTypeValue.newBuilder().setInt32((int) value).build();
      case INT64:
        return AtomicTypeValue.newBuilder().setInt64((long) value).build();
      case FLOAT:
        return AtomicTypeValue.newBuilder().setFloat((float) value).build();
      case DOUBLE:
        return AtomicTypeValue.newBuilder().setDouble((double) value).build();
      case STRING:
        return AtomicTypeValue.newBuilder().setString((String) value).build();
      case BOOLEAN:
        return AtomicTypeValue.newBuilder().setBoolean((boolean) value).build();
      case BYTES:
        return AtomicTypeValue.newBuilder().setBytes(ByteString.copyFrom((byte[]) value)).build();
      default:
        throw new RuntimeException("FieldType unexpected " + fieldType.getTypeName());
    }
  }

  private static Object primitiveFromProto(FieldType fieldType, AtomicTypeValue value) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        return (byte) value.getByte();
      case INT16:
        return (short) value.getInt16();
      case INT32:
        return value.getInt32();
      case INT64:
        return value.getInt64();
      case FLOAT:
        return value.getFloat();
      case DOUBLE:
        return value.getDouble();
      case STRING:
        return value.getString();
      case BOOLEAN:
        return value.getBoolean();
      case BYTES:
        return value.getBytes().toByteArray();
      default:
        throw new RuntimeException("FieldType unexpected " + fieldType.getTypeName());
    }
  }

  private static List<SchemaApi.Option> optionsToProto(Schema.Options options) {
    List<SchemaApi.Option> protoOptions = new ArrayList<>();
    for (String name : options.getOptionNames()) {
      protoOptions.add(
          SchemaApi.Option.newBuilder()
              .setName(name)
              .setType(fieldTypeToProto(Objects.requireNonNull(options.getType(name)), false))
              .setValue(
                  fieldValueToProto(
                      Objects.requireNonNull(options.getType(name)), options.getValue(name)))
              .build());
    }
    return protoOptions;
  }

  private static Schema.Options optionsFromProto(List<SchemaApi.Option> protoOptions) {
    Schema.Options.Builder optionBuilder = Schema.Options.builder();
    for (SchemaApi.Option protoOption : protoOptions) {
      FieldType fieldType = fieldTypeFromProto(protoOption.getType());
      optionBuilder.setOption(
          protoOption.getName(), fieldType, fieldValueFromProto(fieldType, protoOption.getValue()));
    }
    return optionBuilder.build();
  }
}
