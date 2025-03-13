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
package org.apache.beam.sdk.io.fileschematransform;

import com.google.auto.service.AutoService;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** A {@link FileReadSchemaTransformFormatProvider} that reads newline-delimited JSONs. */
@AutoService(FileReadSchemaTransformFormatProvider.class)
public class JsonReadSchemaTransformFormatProvider
    implements FileReadSchemaTransformFormatProvider {
  @Override
  public String identifier() {
    return "json";
  }

  @Override
  public PTransform<PCollection<ReadableFile>, PCollection<Row>> buildTransform(
      FileReadSchemaTransformConfiguration configuration) {

    return new PTransform<PCollection<ReadableFile>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollection<ReadableFile> input) {
        Schema beamSchema = JsonUtils.beamSchemaFromJsonSchema(configuration.getSafeSchema());

        PayloadSerializer payloadSerializer =
            new JsonPayloadSerializerProvider().getSerializer(beamSchema, ImmutableMap.of());

        return input
            .apply(TextIO.readFiles())
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        (String line) ->
                            payloadSerializer.deserialize(line.getBytes(StandardCharsets.UTF_8))))
            .setRowSchema(beamSchema);
      }
    };
  }
}
