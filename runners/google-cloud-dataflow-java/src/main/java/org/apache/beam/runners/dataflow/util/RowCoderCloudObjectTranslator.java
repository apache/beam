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
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.util.JsonFormat;

/** Translator for row coders. */
public class RowCoderCloudObjectTranslator implements CloudObjectTranslator<RowCoder> {
  private static final String SCHEMA = "schema";

  @Override
  public Class<? extends RowCoder> getSupportedClass() {
    return RowCoder.class;
  }

  /** Convert to a cloud object. */
  @Override
  public CloudObject toCloudObject(RowCoder target, SdkComponents sdkComponents) {
    CloudObject base = CloudObject.forClass(RowCoder.class);
    try {
      Structs.addString(
          base,
          SCHEMA,
          JsonFormat.printer().print(SchemaTranslation.schemaToProto(target.getSchema(), true)));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return base;
  }

  /** Convert from a cloud object. */
  @Override
  public RowCoder fromCloudObject(CloudObject cloudObject) {
    try {
      SchemaApi.Schema.Builder schemaBuilder = SchemaApi.Schema.newBuilder();
      JsonFormat.parser().merge(Structs.getString(cloudObject, SCHEMA), schemaBuilder);
      Schema schema = SchemaTranslation.schemaFromProto(schemaBuilder.build());
      SchemaCoderCloudObjectTranslator.overrideEncodingPositions(schema);
      return RowCoder.of(schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String cloudObjectClassName() {
    return CloudObject.forClass(RowCoder.class).getClassName();
  }
}
