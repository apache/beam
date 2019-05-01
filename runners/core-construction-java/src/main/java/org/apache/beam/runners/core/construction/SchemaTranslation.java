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
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Maps;

/** Utility methods for translating schemas. */
public class SchemaTranslation {
  private static final BiMap<TypeName, RunnerApi.Schema.TypeName> TYPE_NAME_MAPPING =
      ImmutableBiMap.<TypeName, RunnerApi.Schema.TypeName>builder()
          .put(TypeName.BYTE, RunnerApi.Schema.TypeName.BYTE)
          .put(TypeName.INT16, RunnerApi.Schema.TypeName.INT16)
          .put(TypeName.INT32, RunnerApi.Schema.TypeName.INT32)
          .put(TypeName.INT64, RunnerApi.Schema.TypeName.INT64)
          .put(TypeName.DECIMAL, RunnerApi.Schema.TypeName.DECIMAL)
          .put(TypeName.FLOAT, RunnerApi.Schema.TypeName.FLOAT)
          .put(TypeName.DOUBLE, RunnerApi.Schema.TypeName.DOUBLE)
          .put(TypeName.STRING, RunnerApi.Schema.TypeName.STRING)
          .put(TypeName.DATETIME, RunnerApi.Schema.TypeName.DATETIME)
          .put(TypeName.BOOLEAN, RunnerApi.Schema.TypeName.BOOLEAN)
          .put(TypeName.BYTES, RunnerApi.Schema.TypeName.BYTES)
          .put(TypeName.ARRAY, RunnerApi.Schema.TypeName.ARRAY)
          .put(TypeName.MAP, RunnerApi.Schema.TypeName.MAP)
          .put(TypeName.ROW, RunnerApi.Schema.TypeName.ROW)
          .put(TypeName.LOGICAL_TYPE, RunnerApi.Schema.TypeName.LOGICAL_TYPE)
          .build();

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
    RunnerApi.Schema.FieldType.Builder builder =
        RunnerApi.Schema.FieldType.newBuilder()
            .setTypeName(TYPE_NAME_MAPPING.get(fieldType.getTypeName()));
    switch (fieldType.getTypeName()) {
      case ROW:
        builder.setRowSchema(toProto(fieldType.getRowSchema()));
        break;

      case ARRAY:
        builder.setCollectionElementType(toProto(fieldType.getCollectionElementType()));
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
                .setId(logicalType.getIdentifier())
                .setArgs(logicalType.getArgument())
                .setBaseType(toProto(logicalType.getBaseType()))
                .setSerializedClass(
                    ByteString.copyFrom(SerializableUtils.serializeToByteArray(logicalType)))
                .build());
        break;

      default:
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
    TypeName typeName = TYPE_NAME_MAPPING.inverse().get(protoFieldType.getTypeName());
    FieldType fieldType;
    switch (typeName) {
      case ROW:
        fieldType = FieldType.row(fromProto(protoFieldType.getRowSchema()));
        break;
      case ARRAY:
        fieldType = FieldType.array(fieldTypeFromProto(protoFieldType.getCollectionElementType()));
        break;
      case MAP:
        fieldType =
            FieldType.map(
                fieldTypeFromProto(protoFieldType.getMapType().getKeyType()),
                fieldTypeFromProto(protoFieldType.getMapType().getValueType()));
        break;
      case LOGICAL_TYPE:
        LogicalType logicalType =
            (LogicalType)
                SerializableUtils.deserializeFromByteArray(
                    protoFieldType.getLogicalType().getSerializedClass().toByteArray(),
                    "logicalType");
        fieldType = FieldType.logicalType(logicalType);
        break;
      default:
        fieldType = FieldType.of(typeName);
    }
    if (protoFieldType.getNullable()) {
      fieldType = fieldType.withNullable(true);
    }
    return fieldType;
  }
}
