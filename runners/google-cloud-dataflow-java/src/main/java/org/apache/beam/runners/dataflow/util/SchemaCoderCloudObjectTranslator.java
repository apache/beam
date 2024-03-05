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

import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.JsonFormat;

/** Translator for Schema coders. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class SchemaCoderCloudObjectTranslator implements CloudObjectTranslator<SchemaCoder> {
  private static final String SCHEMA = "schema";
  private static final String TYPE_DESCRIPTOR = "typeDescriptor";
  private static final String TO_ROW_FUNCTION = "toRowFunction";
  private static final String FROM_ROW_FUNCTION = "fromRowFunction";

  /** Convert to a cloud object. */
  @Override
  public CloudObject toCloudObject(SchemaCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(SchemaCoder.class);

    Structs.addString(
        base,
        TYPE_DESCRIPTOR,
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(target.getEncodedTypeDescriptor())));
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

    try {
      Structs.addString(
          base,
          SCHEMA,
          JsonFormat.printer()
              .omittingInsignificantWhitespace()
              .print(SchemaTranslation.schemaToProto(target.getSchema(), true)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return base;
  }

  /** Convert from a cloud object. */
  @Override
  public SchemaCoder fromCloudObject(CloudObject cloudObject) {
    try {
      TypeDescriptor typeDescriptor =
          (TypeDescriptor)
              SerializableUtils.deserializeFromByteArray(
                  StringUtils.jsonStringToByteArray(
                      Structs.getString(cloudObject, TYPE_DESCRIPTOR)),
                  "typeDescriptor");
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
      SchemaApi.Schema.Builder schemaBuilder = SchemaApi.Schema.newBuilder();
      JsonFormat.parser().merge(Structs.getString(cloudObject, SCHEMA), schemaBuilder);
      Schema schema = SchemaTranslation.schemaFromProto(schemaBuilder.build());
      overrideEncodingPositions(schema);
      return SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static void overrideEncodingPositions(Schema schema) {
    @Nullable UUID uuid = schema.getUUID();
    if (schema.isEncodingPositionsOverridden() && uuid != null) {
      RowCoder.overrideEncodingPositions(uuid, schema.getEncodingPositions());
    }
    schema.getFields().stream()
        .map(Schema.Field::getType)
        .forEach(SchemaCoderCloudObjectTranslator::overrideEncodingPositions);
  }

  private static void overrideEncodingPositions(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ROW:
        overrideEncodingPositions(Preconditions.checkArgumentNotNull(fieldType.getRowSchema()));
        break;
      case ARRAY:
      case ITERABLE:
        overrideEncodingPositions(
            Preconditions.checkArgumentNotNull(fieldType.getCollectionElementType()));
        break;
      case MAP:
        overrideEncodingPositions(Preconditions.checkArgumentNotNull(fieldType.getMapKeyType()));
        overrideEncodingPositions(Preconditions.checkArgumentNotNull(fieldType.getMapValueType()));
        break;
      case LOGICAL_TYPE:
        Schema.LogicalType logicalType =
            Preconditions.checkArgumentNotNull(fieldType.getLogicalType());
        @Nullable Schema.FieldType argumentType = logicalType.getArgumentType();
        if (argumentType != null) {
          overrideEncodingPositions(argumentType);
        }
        overrideEncodingPositions(logicalType.getBaseType());
        break;
      default:
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
