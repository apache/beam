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
package org.apache.beam.runners.dataflow.util;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.protobuf.ByteString;

/** New class. */
public class SchemaCoderCloudObjectTranslator implements CloudObjectTranslator<SchemaCoder> {
  private static String SCHEMA = "schema";
  private static String TO_ROW_FUNCTION = "toRowFunction";
  private static String FROM_ROW_FUNCTION = "fromRowFunction";

  private static RunnerApi.Schema toProto(Schema schema) {
    RunnerApi.Schema.Builder builder =
        RunnerApi.Schema.newBuilder().setId(schema.getUUID().toString());
    for (Field field : schema.getFields()) {
      RunnerApi.Schema.Field protoField =
          toProto(
              field,
              schema.indexOf(field.getName()),
              schema.getPhysicalFieldLocations().get(field.getName()));
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
        RunnerApi.Schema.FieldType.newBuilder().setTypeName(toProto(fieldType.getTypeName()));
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
    return builder.build();
  }

  private static final RunnerApi.Schema.TypeName toProto(TypeName typeName) {
    switch (typeName) {
      case BYTE:
        return RunnerApi.Schema.TypeName.BYTE;

      case INT16:
        return RunnerApi.Schema.TypeName.INT16;

      case INT32:
        return RunnerApi.Schema.TypeName.INT32;

      case INT64:
        return RunnerApi.Schema.TypeName.INT64;

      case DECIMAL:
        return RunnerApi.Schema.TypeName.DECIMAL;

      case FLOAT:
        return RunnerApi.Schema.TypeName.FLOAT;

      case DOUBLE:
        return RunnerApi.Schema.TypeName.DOUBLE;

      case STRING:
        return RunnerApi.Schema.TypeName.STRING;

      case DATETIME:
        return RunnerApi.Schema.TypeName.DATETIME;

      case BOOLEAN:
        return RunnerApi.Schema.TypeName.BOOLEAN;

      case BYTES:
        return RunnerApi.Schema.TypeName.BYTES;

      case ARRAY:
        return RunnerApi.Schema.TypeName.ARRAY;

      case MAP:
        return RunnerApi.Schema.TypeName.MAP;

      case ROW:
        return RunnerApi.Schema.TypeName.ROW;

      case LOGICAL_TYPE:
        return RunnerApi.Schema.TypeName.LOGICAL_TYPE;

      default:
        throw new RuntimeException("Bad TypeName " + typeName);
    }
  }

  private static Schema fromProto(RunnerApi.Schema protoSchema) {
    Schema.Builder builder = Schema.builder();
    Map<String, Integer> encodingLocationMap = Maps.newHashMap();
    for (RunnerApi.Schema.Field protoField : protoSchema.getFieldsList()) {
      Field field = fieldFromProto(protoField);
      builder.addField(field);
      encodingLocationMap.put(protoField.getName(), protoField.getEncodingPosition());
    }
    Schema schema = builder.build();
    schema.setPhysicalFieldLocation(encodingLocationMap);
    schema.setUUID(UUID.fromString(protoSchema.getId()));

    return schema;
  }

  private static Field fieldFromProto(RunnerApi.Schema.Field protoField) {
    return Field.of(protoField.getName(), fieldTypeFromProto(protoField.getType()))
        .withDescription(protoField.getDescription());
  }

  private static FieldType fieldTypeFromProto(RunnerApi.Schema.FieldType protoFieldType) {
    TypeName typeName = typeNameFromProto(protoFieldType.getTypeName());
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

  private static TypeName typeNameFromProto(RunnerApi.Schema.TypeName protoTypeName) {
    switch (protoTypeName) {
      case BYTE:
        return TypeName.BYTE;

      case INT16:
        return TypeName.INT16;

      case INT32:
        return TypeName.INT32;

      case INT64:
        return TypeName.INT64;

      case DECIMAL:
        return TypeName.DECIMAL;

      case FLOAT:
        return TypeName.FLOAT;

      case DOUBLE:
        return TypeName.DOUBLE;

      case STRING:
        return TypeName.STRING;

      case DATETIME:
        return TypeName.DATETIME;

      case BOOLEAN:
        return TypeName.BOOLEAN;

      case BYTES:
        return TypeName.BYTES;

      case ARRAY:
        return TypeName.ARRAY;

      case MAP:
        return TypeName.MAP;

      case ROW:
        return TypeName.ROW;

      case LOGICAL_TYPE:
        return TypeName.LOGICAL_TYPE;

      default:
        throw new RuntimeException("Bad TypeName " + protoTypeName);
    }
  }

  /** new class. */
  @Override
  public CloudObject toCloudObject(SchemaCoder target, SdkComponents sdkComponents) {
    System.out.println("CONVERTING SCHEMA CODER TO CLOUD OBJECT");
    CloudObject base = CloudObject.forClass(SchemaCoder.class);

    Structs.addString(
        base,
        TO_ROW_FUNCTION,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(target.getToRowFunction())));
    Structs.addString(
        base,
        FROM_ROW_FUNCTION,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(target.getFromRowFunction())));
    Structs.addString(
        base, SCHEMA, StringUtils.byteArrayToJsonString(toProto(target.getSchema()).toByteArray()));
    return base;
  }

  /** new class. */
  @Override
  public SchemaCoder fromCloudObject(CloudObject cloudObject) {
    System.out.println("CONVERTING CLOUD OBJECT TO SCHEMA CODER");
    try {
      SerializableFunction toRowFunction =
          (SerializableFunction)
              SerializableUtils.deserializeFromByteArray(
                  StringUtils.jsonStringToByteArray(
                      Structs.getString(cloudObject, TO_ROW_FUNCTION)),
                  "toRowFunction");
      SerializableFunction fromRowFunction =
          (SerializableFunction)
              SerializableUtils.deserializeFromByteArray(
                  StringUtils.jsonStringToByteArray(
                      Structs.getString(cloudObject, FROM_ROW_FUNCTION)),
                  "fromRowFunction");
      RunnerApi.Schema protoSchema =
          RunnerApi.Schema.parseFrom(
              StringUtils.jsonStringToByteArray(Structs.getString(cloudObject, SCHEMA)));
      Schema schema = fromProto(protoSchema);

      return SchemaCoder.of(schema, toRowFunction, fromRowFunction);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Class<? extends SchemaCoder> getSupportedClass() {
    return SchemaCoder.class;
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(SchemaCoder.class).getClassName();
  }
}
