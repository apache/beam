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
package org.apache.beam.examples.multilanguage.schematransforms;

import static org.apache.beam.examples.multilanguage.schematransforms.JavaCountProvider.Configuration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(SchemaTransformProvider.class)
public class JavaCountProvider extends TypedSchemaTransformProvider<Configuration> {
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:count:v1";
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        Schema outputSchema =
            Schema.builder().addStringField("word").addInt64Field("count").build();

        PCollection<Row> wordCounts =
            input
                .get("input")
                .apply(Count.perElement())
                .apply(
                    MapElements.into(TypeDescriptors.rows())
                        .via(
                            kv ->
                                Row.withSchema(outputSchema)
                                    .withFieldValue(
                                        "word",
                                        Preconditions.checkStateNotNull(
                                            kv.getKey().getString("word")))
                                    .withFieldValue("count", kv.getValue())
                                    .build()))
                .setRowSchema(outputSchema);

        return PCollectionRowTuple.of("output", wordCounts);
      }
    };
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  protected abstract static class Configuration {}
}
