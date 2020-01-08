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

import java.util.Map;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.model.pipeline.v1.SchemaApi.ArrayTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.FieldValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.IterableTypeValue;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeEntry;
import org.apache.beam.model.pipeline.v1.SchemaApi.MapTypeValue;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** Utility methods for translating schemas. */
public class SchemaTranslation {

  private static final String URN_BEAM_LOGICAL_DATETIME = "beam:logical_type:datetime:v1";
  private static final String URN_BEAM_LOGICAL_DECIMAL = "beam:logical_type:decimal:v1";
  private static final String URN_BEAM_LOGICAL_JAVASDK = "beam:logical_type:javasdk:v1";

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
        SchemaApi.LogicalType.Builder logicalTypeBuilder =
            SchemaApi.LogicalType.newBuilder()
                .setArgumentType(
                    fieldTypeToProto(logicalType.getArgumentType(), serializeLogicalType))
                .setArgument(
                    rowFieldToProto(logicalType.getArgumentType(), logicalType.getArgument()))
                .setRepresentation(
                    fieldTypeToProto(logicalType.getBaseType(), serializeLogicalType))
                // TODO(BEAM-7855): "javasdk" types should only be a last resort. Types defined in
                // Beam should have their own URN, and there should be a mechanism for users to
                // register their own types by URN.
                .setUrn(URN_BEAM_LOGICAL_JAVASDK);
        if (serializeLogicalType) {
          logicalTypeBuilder =
              logicalTypeBuilder.setPayload(
                  ByteString.copyFrom(SerializableUtils.serializeToByteArray(logicalType)));
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

  public static Schema fromProto(SchemaApi.Schema protoSchema) {
    Schema.Builder builder = Schema.builder();
    Map<String, Integer> encodingLocationMap = Maps.newHashMap();
    for (SchemaApi.Field protoField : protoSchema.getFieldsList()) {
      Field field = fieldFromProto(protoField);
      builder.addField(field);
      encodingLocationMap.put(protoField.getName(), protoField.getEncodingPosition());
    }
    Schema schema = builder.build();
    schema.setEncodingPositions(encodingLocationMap);
    if (!protoSchema.getId().isEmpty()) {
      schema.setUUID(UUID.fromString(protoSchema.getId()));
    }

    return schema;
  }

  private static Field fieldFromProto(SchemaApi.Field protoField) {
    return Field.of(protoField.getName(), fieldTypeFromProto(protoField.getType()))
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
        return FieldType.row(fromProto(protoFieldType.getRowType().getSchema()));
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
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
        String urn = protoFieldType.getLogicalType().getUrn();
        if (urn.equals(URN_BEAM_LOGICAL_DATETIME)) {
          return FieldType.DATETIME;
        } else if (urn.equals(URN_BEAM_LOGICAL_DECIMAL)) {
          return FieldType.DECIMAL;
        } else if (urn.equals(URN_BEAM_LOGICAL_JAVASDK)) {
          return FieldType.logicalType(
              (LogicalType)
                  SerializableUtils.deserializeFromByteArray(
                      protoFieldType.getLogicalType().getPayload().toByteArray(), "logicalType"));
        } else {
          throw new IllegalArgumentException("Encountered unsupported logical type URN: " + urn);
        }
      default:
        throw new IllegalArgumentException(
            "Unexpected type_info: " + protoFieldType.getTypeInfoCase());
    }
  }

  public static SchemaApi.Row rowToProto(Row row) {
    SchemaApi.Row.Builder builder = SchemaApi.Row.newBuilder();
    for (int i = 0; i < row.getFieldCount(); ++i) {
      builder.addValues(rowFieldToProto(row.getSchema().getField(i).getType(), row.getValue(i)));
    }
    return builder.build();
  }

  private static SchemaApi.FieldValue rowFieldToProto(FieldType fieldType, Object value) {
    FieldValue.Builder builder = FieldValue.newBuilder();
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
      case LOGICAL_TYPE:
      default:
        return builder.setAtomicValue(primitiveRowFieldToProto(fieldType, value)).build();
    }
  }

  private static SchemaApi.ArrayTypeValue arrayValueToProto(
      FieldType elementType, Iterable values) {
    return ArrayTypeValue.newBuilder()
        .addAllElement(Iterables.transform(values, e -> rowFieldToProto(elementType, e)))
        .build();
  }

  private static SchemaApi.IterableTypeValue iterableValueToProto(
      FieldType elementType, Iterable values) {
    return IterableTypeValue.newBuilder()
        .addAllElement(Iterables.transform(values, e -> rowFieldToProto(elementType, e)))
        .build();
  }

  private static SchemaApi.MapTypeValue mapToProto(
      FieldType keyType, FieldType valueType, Map<Object, Object> map) {
    MapTypeValue.Builder builder = MapTypeValue.newBuilder();
    for (Map.Entry entry : map.entrySet()) {
      MapTypeEntry mapProtoEntry =
          MapTypeEntry.newBuilder()
              .setKey(rowFieldToProto(keyType, entry.getKey()))
              .setValue(rowFieldToProto(valueType, entry.getValue()))
              .build();
      builder.addEntries(mapProtoEntry);
    }
    return builder.build();
  }

  private static SchemaApi.AtomicTypeValue primitiveRowFieldToProto(
      FieldType fieldType, Object value) {
    switch (fieldType.getTypeName()) {
      case BYTE:
        return SchemaApi.AtomicTypeValue.newBuilder().setByte((int) value).build();
      case INT16:
        return SchemaApi.AtomicTypeValue.newBuilder().setInt16((int) value).build();
      case INT32:
        return SchemaApi.AtomicTypeValue.newBuilder().setInt32((int) value).build();
      case INT64:
        return SchemaApi.AtomicTypeValue.newBuilder().setInt64((long) value).build();
      case FLOAT:
        return SchemaApi.AtomicTypeValue.newBuilder().setFloat((float) value).build();
      case DOUBLE:
        return SchemaApi.AtomicTypeValue.newBuilder().setDouble((double) value).build();
      case STRING:
        return SchemaApi.AtomicTypeValue.newBuilder().setString((String) value).build();
      case BOOLEAN:
        return SchemaApi.AtomicTypeValue.newBuilder().setBoolean((boolean) value).build();
      case BYTES:
        return SchemaApi.AtomicTypeValue.newBuilder()
            .setBytes(ByteString.copyFrom((byte[]) value))
            .build();
      default:
        throw new RuntimeException("FieldType unexpected " + fieldType.getTypeName());
    }
  }
}
