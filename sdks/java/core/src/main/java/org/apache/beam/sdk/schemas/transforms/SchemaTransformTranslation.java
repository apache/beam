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
package org.apache.beam.sdk.schemas.transforms;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.TransformPayloadTranslator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link TransformPayloadTranslator} implementation that translates between a Java {@link
 * SchemaTransform} and a protobuf payload for that transform.
 */
public class SchemaTransformTranslation {
  public abstract static class SchemaTransformPayloadTranslator<T extends SchemaTransform>
      implements TransformPayloadTranslator<T> {
    public abstract SchemaTransformProvider provider();

    @Override
    public String getUrn() {
      return BeamUrns.getUrn(SCHEMA_TRANSFORM);
    }

    @Override
    @SuppressWarnings("argument")
    public @Nullable FunctionSpec translate(
        AppliedPTransform<?, ?, T> application, SdkComponents components) throws IOException {
      SchemaApi.Schema expansionSchema =
          SchemaTranslation.schemaToProto(provider().configurationSchema(), true);
      Row configRow = toConfigRow(application.getTransform());
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      RowCoder.of(provider().configurationSchema()).encode(configRow, os);

      return FunctionSpec.newBuilder()
          .setUrn(getUrn())
          .setPayload(
              ExternalTransforms.SchemaTransformPayload.newBuilder()
                  .setIdentifier(provider().identifier())
                  .setConfigurationSchema(expansionSchema)
                  .setConfigurationRow(ByteString.copyFrom(os.toByteArray()))
                  .build()
                  .toByteString())
          .build();
    }

    @Override
    public T fromConfigRow(Row configRow, PipelineOptions options) {
      return (T) provider().from(configRow);
    }
  }
}
