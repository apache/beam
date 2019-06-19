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
package org.apache.beam.runners.core.construction;

import java.util.Map;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/** Utility methods for translating schemas. */
public class SchemaTranslation {

  private static final BiMap<TypeName, RunnerApi.Schema.AtomicType> ATOMIC_TYPE_MAPPING =
      ImmutableBiMap.<TypeName, RunnerApi.Schema.AtomicType>builder()
          .put(TypeName.BYTE, RunnerApi.Schema.AtomicType.BYTE)
          .put(TypeName.INT16, RunnerApi.Schema.AtomicType.INT16)
          .put(TypeName.INT32, RunnerApi.Schema.AtomicType.INT32)
          .put(TypeName.INT64, RunnerApi.Schema.AtomicType.INT64)
          .put(TypeName.FLOAT, RunnerApi.Schema.AtomicType.FLOAT)
          .put(TypeName.DOUBLE, RunnerApi.Schema.AtomicType.DOUBLE)
          .put(TypeName.STRING, RunnerApi.Schema.AtomicType.STRING)
          .put(TypeName.BOOLEAN, RunnerApi.Schema.AtomicType.BOOLEAN)
          .put(TypeName.BYTES, RunnerApi.Schema.AtomicType.BYTES)
          .build();

  private static final String URN_BEAM_LOGICAL_DATETIME = "beam:fieldtype:datetime";
  private static final String URN_BEAM_LOGICAL_DECIMAL = "beam:fieldtype:decimal";

  public static RunnerApi.Schema toProto(Schema schema) {
    String uuid = schema.getUUID() != null ? schema.getUUID().toString() : "";
    RunnerApi.Schema.Builder builder = RunnerApi.Schema.newBuilder().setId(uuid);
    for (Field field : schema.getFields()) {
      RunnerApi.Schema.Field protoField =
          toProto(
              field,
              schema.indexOf(field.getName()),
              schema.getEncodingPositions().get(field.getName()));
      builder.addFields(protoField);
    }
    return builder.build();
  }

  private static RunnerApi.Schema.Field toProto(Field field, int fieldId, int position) {
    return RunnerApi.Schema.Field.newBuilder()
        .setName(field.getName())
        .setDescription(field.getDescription())
        .setType(toProto(field.getType()))
        .setId(fieldId)
        .setEncodingPosition(position)
        .build();
  }

  private static RunnerApi.Schema.FieldType toProto(FieldType fieldType) {
    RunnerApi.Schema.FieldType.Builder builder = RunnerApi.Schema.FieldType.newBuilder();
    switch (fieldType.getTypeName()) {
      case ROW:
        builder.setRowType(
            RunnerApi.Schema.RowType.newBuilder().setSchema(toProto(fieldType.getRowSchema())));
        break;

      case ARRAY:
        builder.setArrayType(
            RunnerApi.Schema.ArrayType.newBuilder()
                .setElementType(toProto(fieldType.getCollectionElementType())));
        break;

      case MAP:
        builder.setMapType(
            RunnerApi.Schema.MapType.newBuilder()
                .setKeyType(toProto(fieldType.getMapKeyType()))
                .setValueType(toProto(fieldType.getMapValueType()))
                .build());
        break;

      case LOGICAL_TYPE:
        LogicalType logicalType = fieldType.getLogicalType();
        builder.setLogicalType(
            RunnerApi.Schema.LogicalType.newBuilder()
                .setUrn(logicalType.getIdentifier())
                .setArgs(logicalType.getArgument())
                .setRepresentation(toProto(logicalType.getBaseType()))
                .build());
        break;
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
      case DATETIME:
        builder.setLogicalType(
            RunnerApi.Schema.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DATETIME)
                .setRepresentation(toProto(FieldType.INT64))
                .build());
        break;
      case DECIMAL:
        builder.setLogicalType(
            RunnerApi.Schema.LogicalType.newBuilder()
                .setUrn(URN_BEAM_LOGICAL_DECIMAL)
                .setRepresentation(toProto(FieldType.BYTES))
                .build());
        break;
      default:
        builder.setAtomicType(ATOMIC_TYPE_MAPPING.get(fieldType.getTypeName()));
        break;
    }
    builder.setNullable(fieldType.getNullable());
    return builder.build();
  }

  public static Schema fromProto(RunnerApi.Schema protoSchema) {
    Schema.Builder builder = Schema.builder();
    Map<String, Integer> encodingLocationMap = Maps.newHashMap();
    for (RunnerApi.Schema.Field protoField : protoSchema.getFieldsList()) {
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

  private static Field fieldFromProto(RunnerApi.Schema.Field protoField) {
    return Field.of(protoField.getName(), fieldTypeFromProto(protoField.getType()))
        .withDescription(protoField.getDescription());
  }

  private static FieldType fieldTypeFromProto(RunnerApi.Schema.FieldType protoFieldType) {
    FieldType fieldType;
    switch (protoFieldType.getTypeInfoCase()) {
      case ATOMIC_TYPE:
        TypeName typeName = ATOMIC_TYPE_MAPPING.inverse().get(protoFieldType.getAtomicType());
        fieldType = FieldType.of(typeName);
        break;
      case ROW_TYPE:
        fieldType = FieldType.row(fromProto(protoFieldType.getRowType().getSchema()));
        break;
      case ARRAY_TYPE:
        fieldType =
            FieldType.array(fieldTypeFromProto(protoFieldType.getArrayType().getElementType()));
        break;
      case MAP_TYPE:
        fieldType =
            FieldType.map(
                fieldTypeFromProto(protoFieldType.getMapType().getKeyType()),
                fieldTypeFromProto(protoFieldType.getMapType().getValueType()));
        break;
      case LOGICAL_TYPE:
        // Special-case for DATETIME and DECIMAL which are logical types in portable representation,
        // but not yet in Java. (BEAM-7554)
        String urn = protoFieldType.getLogicalType().getUrn();
        if (urn.equals(URN_BEAM_LOGICAL_DATETIME)) {
          fieldType = FieldType.DATETIME;
        } else if (urn.equals(URN_BEAM_LOGICAL_DECIMAL)) {
          fieldType = FieldType.DECIMAL;
        } else {
          // TODO: Look up logical type class by URN.
          throw new IllegalArgumentException("Decoding logical types is not yet supported.");
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected type_info: " + protoFieldType.getTypeInfoCase());
    }

    if (protoFieldType.getNullable()) {
      fieldType = fieldType.withNullable(true);
    }
    return fieldType;
  }
}
