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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaApi.ArrayTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.AtomicTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.FieldValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.IterableTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.LogicalTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeEntry;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeValue;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.FixedString;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.PythonCallable;
import org.apache.beam.sdk.schemas.logicaltypes.SchemaLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.UnknownLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.commons.lang3.ClassUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods for translating schemas. */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public class SchemaTranslation {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaTranslation.class);

  private static final String URN_BEAM_LOGICAL_DECIMAL = FixedPrecisionNumeric.BASE_IDENTIFIER;

  private static String getLogicalTypeUrn(String identifier) {
    if (identifier.startsWith("beam:logical_type:")) {
      return identifier;
    } else {
      String filtered = identifier.replaceAll("[^0-9A-Za-z_]", "").toLowerCase();
      if (!Strings.isNullOrEmpty(filtered)) {
        // urn for non-standard Java SDK logical types are assigned with javasdk_<identifier>
        return String.format("beam:logical_type:javasdk_%s:v1", filtered);
      } else {
        // raw "javasdk" name should only be a last resort. Types defined in Beam should have their
        // own URN.
        return "beam:logical_type:javasdk:v1";
      }
    }
  }

  private static final String URN_BEAM_LOGICAL_MILLIS_INSTANT =
      SchemaApi.LogicalTypes.Enum.MILLIS_INSTANT
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);

  // TODO(https://github.com/apache/beam/issues/19715): Populate this with a LogicalTypeRegistrar,
  // which includes a way to construct
  // the LogicalType given an argument.
  @VisibleForTesting
  static final ImmutableMap<String, Class<? extends LogicalType<?, ?>>> STANDARD_LOGICAL_TYPES =
      ImmutableMap.<String, Class<? extends LogicalType<?, ?>>>builder()
          .put(FixedPrecisionNumeric.IDENTIFIER, FixedPrecisionNumeric.class)
          .put(MicrosInstant.IDENTIFIER, MicrosInstant.class)
          .put(SchemaLogicalType.IDENTIFIER, SchemaLogicalType.class)
          .put(PythonCallable.IDENTIFIER, PythonCallable.class)
          .put(FixedBytes.IDENTIFIER, FixedBytes.class)
          .put(VariableBytes.IDENTIFIER, VariableBytes.class)
          .put(FixedString.IDENTIFIER, FixedString.class)
          .put(VariableString.IDENTIFIER, VariableString.class)
          .build();

  public static SchemaApi.Schema schemaToProto(Schema schema, boolean serializeLogicalType) {
    return schemaToProto(schema, serializeLogicalType, true);
  }

  public static SchemaApi.Schema schemaToProto(
      Schema schema, boolean serializeLogicalType, boolean serializeUUID) {
    String uuid = schema.getUUID() != null && serializeUUID ? schema.getUUID().toString() : "";
    SchemaApi.Schema.Builder builder = SchemaApi.Schema.newBuilder().setId(uuid);
    for (Field field : schema.getFields()) {
      SchemaApi.Field protoField =
          fieldToProto(
              field,
              schema.indexOf(field.getName()),
              schema.getEncodingPositions().get(field.getName()),
              serializeLogicalType,
              serializeUUID);
      builder.addFields(protoField);
    }
    builder.addAllOptions(optionsToProto(schema.getOptions()));
    return builder.build();
  }

  private static SchemaApi.Field fieldToProto(
      Field field, int fieldId, int position, boolean serializeLogicalType, boolean serializeUUID) {
    return SchemaApi.Field.newBuilder()
        .setName(field.getName())
        .setDescription(field.getDescription())
        .setType(fieldTypeToProto(field.getType(), serializeLogicalType, serializeUUID))
        .setId(fieldId)
        .setEncodingPosition(position)
        .addAllOptions(optionsToProto(field.getOptions()))
        .build();
  }

  @VisibleForTesting
  static SchemaApi.FieldType fieldTypeToProto(
      FieldType fieldType, boolean serializeLogicalType, boolean serializeUUID) {
    SchemaApi.FieldType.Builder builder = SchemaApi.FieldType.newBuilder();
    switch (fieldType.getTypeName()) {
      case ROW:
        builder.setRowType(
            SchemaApi.RowType.newBuilder()
                .setSchema(
                    schemaToProto(fieldType.getRowSchema(), serializeLogicalType, serializeUUID)));
        break;

      case ARRAY:
        builder.setArrayType(
            SchemaApi.ArrayType.newBuilder()
                .setElementType(
                    fieldTypeToProto(
                        fieldType.getCollectionElementType(),
                        serializeLogicalType,
                        serializeUUID)));
        break;

      case ITERABLE:
        builder.setIterableType(
            SchemaApi.IterableType.newBuilder()
                .setElementType(
                    fieldTypeToProto(
                        fieldType.getCollectionElementType(),
                        serializeLogicalType,
                        serializeUUID)));
        break;

      case MAP:
        builder.setMapType(
            SchemaApi.MapType.newBuilder()
                .setKeyType(
                    fieldTypeToProto(
                        fieldType.getMapKeyType(), serializeLogicalType, serializeUUID))
                .setValueType(
                    fieldTypeToProto(
                        fieldType.getMapValueType(), serializeLogicalType, serializeUUID))
                .build());
        break;

      case LOGICAL_TYPE:
        LogicalType logicalType = fieldType.getLogicalType();
        SchemaApi.LogicalType.Builder logicalTypeBuilder;
        String identifier = logicalType.getIdentifier();
        boolean isStandard = STANDARD_LOGICAL_TYPES.containsKey(identifier);

        if (!isStandard && logicalType instanceof UnknownLogicalType) {
          logicalTypeBuilder =
              SchemaApi.LogicalType.newBuilder()
                  .setUrn(logicalType.getIdentifier())
                  .setPayload(ByteString.copyFrom(((UnknownLogicalType) logicalType).getPayload()))
                  .setRepresentation(
                      fieldTypeToProto(
                          logicalType.getBaseType(), serializeLogicalType, serializeUUID));

          if (logicalType.getArgumentType() != null) {
            logicalTypeBuilder
                .setArgumentType(
                    fieldTypeToProto(
                        logicalType.getArgumentType(), serializeLogicalType, serializeUUID))
                .setArgument(
                    fieldValueToProto(logicalType.getArgumentType(), logicalType.getArgument()));
          }
        } else {
          String urn = getLogicalTypeUrn(identifier);
          logicalTypeBuilder =
              SchemaApi.LogicalType.newBuilder()
                  .setRepresentation(
                      fieldTypeToProto(
                          logicalType.getBaseType(), serializeLogicalType, serializeUUID))
                  .setUrn(urn);
          if (logicalType.getArgumentType() != null) {
            logicalTypeBuilder =
                logicalTypeBuilder
                    .setArgumentType(
                        fieldTypeToProto(
                            logicalType.getArgumentType(), serializeLogicalType, serializeUUID))
                    .setArgument(
                        fieldValueToProto(
                            logicalType.getArgumentType(), logicalType.getArgument()));
          }
          // No need to embed serialized bytes to payload for standard (known) logical type
          if (!isStandard && serializeLogicalType) {
            logicalTypeBuilder =
                logicalTypeBuilder.setPayload(
                    ByteString.copyFrom(SerializableUtils.serializeToByteArray(logicalType)));
          }
        }
        builder.setLogicalType(logicalTypeBuilder.build());
        break;
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (https://github.com/apache/beam/issues/19817)
      case DATETIME:
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_MILLIS_INSTANT)
                .setRepresentation(
                    fieldTypeToProto(FieldType.INT64, serializeLogicalType, serializeUUID))
                .build());
        break;
      case DECIMAL:
        // DECIMAL without precision specified. Used as the representation type of
        // FixedPrecisionNumeric logical type.
        builder.setLogicalType(
            SchemaApi.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DECIMAL)
                .setRepresentation(
                    fieldTypeToProto(FieldType.BYTES, serializeLogicalType, serializeUUID))
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
    long distinctEncodingPositions = encodingLocationMap.values().stream().distinct().count();
    Preconditions.checkState(distinctEncodingPositions <= schema.getFieldCount());
    if (distinctEncodingPositions < schema.getFieldCount() && schema.getFieldCount() > 0) {
      // This means that encoding positions were not specified in the proto. Generally, we don't
      // expect this to happen,
      // but if it does happen, we expect none to be specified - in which case the should all be
      // zero.
      Preconditions.checkState(distinctEncodingPositions == 1);
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

  @VisibleForTesting
  static FieldType fieldTypeFromProto(SchemaApi.FieldType protoFieldType) {
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
        SchemaApi.LogicalType logicalType = protoFieldType.getLogicalType();
        String urn = logicalType.getUrn();
        Class<? extends LogicalType<?, ?>> logicalTypeClass = STANDARD_LOGICAL_TYPES.get(urn);
        if (logicalTypeClass != null) {
          boolean hasArgument = logicalType.hasArgument();
          if (hasArgument) {
            // Logical type with argument. Construct from compatible of() method with single
            // argument type is either a primitive, List, Map, or Row.
            FieldType fieldType = fieldTypeFromProto(logicalType.getArgumentType());
            Object fieldValue =
                Objects.requireNonNull(fieldValueFromProto(fieldType, logicalType.getArgument()));
            Class clazz = fieldValue.getClass();
            if (ClassUtils.isPrimitiveWrapper(clazz)) {
              // argument is a primitive wrapper type (e.g. Integer)
              clazz = ClassUtils.wrapperToPrimitive(clazz);
            } else if (fieldValue instanceof List) {
              // argument is ArrayValue or iterableValue
              clazz = List.class;
            }
            if (fieldValue instanceof Map) {
              // argument is Map
              clazz = Map.class;
            } else if (fieldValue instanceof Row) {
              // argument is Row
              clazz = Row.class;
            }
            String objectName = clazz.getName();
            try {
              return FieldType.logicalType(
                  logicalTypeClass.cast(
                      logicalTypeClass.getMethod("of", clazz).invoke(null, fieldValue)));
            } catch (NoSuchMethodException e) {
              throw new RuntimeException(
                  String.format(
                      "Standard logical type '%s' does not have a static of('%s') method.",
                      urn, objectName),
                  e);
            } catch (IllegalAccessException e) {
              throw new RuntimeException(
                  String.format(
                      "Standard logical type '%s' has an of('%s') method, but it is not accessible.",
                      urn, objectName),
                  e);
            } catch (InvocationTargetException e) {
              throw new RuntimeException(
                  String.format(
                      "Error instantiating logical type '%s' with of('%s') method.",
                      urn, objectName),
                  e);
            }
          } else {
            // Logical type without argument. Construct from constructor without parameter
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
        }
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (https://github.com/apache/beam/issues/19817)
        if (urn.equals(URN_BEAM_LOGICAL_MILLIS_INSTANT)) {
          return FieldType.DATETIME;
        } else if (urn.equals(URN_BEAM_LOGICAL_DECIMAL)) {
          return FieldType.DECIMAL;
        } else if (urn.startsWith("beam:logical_type:")) {
          if (!logicalType.getPayload().isEmpty()) {
            // logical type has a payload, try to recover the instance by deserialization
            try {
              return FieldType.logicalType(
                  (LogicalType)
                      SerializableUtils.deserializeFromByteArray(
                          logicalType.getPayload().toByteArray(), "logicalType"));
            } catch (IllegalArgumentException e) {
              LOG.warn(
                  "Unable to deserialize the logical type {} from proto. Mark as UnknownLogicalType.",
                  urn);
            }
          } else {
            // logical type does not have a payload. This happens when it is passed xlang.
            // TODO(yathu) it appears this path is called heavily, consider cache the instance
            LOG.debug("Constructing non-standard logical type {} as UnknownLogicalType", urn);
          }
        }
        // assemble an UnknownLogicalType
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
              .setType(
                  fieldTypeToProto(Objects.requireNonNull(options.getType(name)), false, false))
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
