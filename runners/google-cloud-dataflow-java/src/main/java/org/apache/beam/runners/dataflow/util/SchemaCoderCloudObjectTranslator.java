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
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Translator for Schema coders. */
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
    Structs.addString(
        base,
        SCHEMA,
        StringUtils.byteArrayToJsonString(
            SchemaTranslation.schemaToProto(target.getSchema(), true).toByteArray()));
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
      SchemaApi.Schema protoSchema =
          SchemaApi.Schema.parseFrom(
              StringUtils.jsonStringToByteArray(Structs.getString(cloudObject, SCHEMA)));
      Schema schema = SchemaTranslation.fromProto(protoSchema);
      return SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction);
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
