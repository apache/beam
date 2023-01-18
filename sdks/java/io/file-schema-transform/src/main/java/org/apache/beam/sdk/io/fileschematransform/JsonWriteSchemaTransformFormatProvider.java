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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.applyCommonTextIOWriteFeatures;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.auto.service.AutoService;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** A {@link FileWriteSchemaTransformFormatProvider} for JSON format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class JsonWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  final String suffix = String.format(".%s", FileWriteSchemaTransformFormatProviders.JSON);

  @Override
  public String identifier() {
    return FileWriteSchemaTransformFormatProviders.JSON;
  }

  /**
   * Builds a {@link PTransform} that transforms a {@link Row} {@link PCollection} into result
   * {@link PCollection} file names written using {@link TextIO.Write}.
   */
  @Override
  public PTransform<PCollection<Row>, PCollection<String>> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {
    return new PTransform<PCollection<Row>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Row> input) {

        PCollection<String> json = input.apply("Row To Json", mapRowsToJsonStrings(schema));

        TextIO.Write write =
            TextIO.write().to(configuration.getFilenamePrefix()).withSuffix(suffix);

        write = applyCommonTextIOWriteFeatures(write, configuration);

        return json.apply("Write Json", write.withOutputFilenames())
            .getPerDestinationOutputFilenames()
            .apply("perDestinationOutputFilenames", Values.create());
      }
    };
  }

  /** Builds a {@link MapElements} transform to map {@link Row} to JSON strings. */
  MapElements<Row, String> mapRowsToJsonStrings(Schema schema) {
    return MapElements.into(strings()).via(new RowToJsonFn(schema));
  }

  private static class RowToJsonFn implements SerializableFunction<Row, String> {

    private final PayloadSerializer payloadSerializer;

    RowToJsonFn(Schema schema) {
      payloadSerializer =
          new JsonPayloadSerializerProvider().getSerializer(schema, ImmutableMap.of());
    }

    @Override
    public String apply(Row input) {
      return new String(payloadSerializer.serialize(input), StandardCharsets.UTF_8);
    }
  }
}
