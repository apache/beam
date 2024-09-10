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

import static org.apache.beam.examples.multilanguage.schematransforms.ExtractWordsProvider.Configuration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/** Splits a line into separate words and returns each word. */
@AutoService(SchemaTransformProvider.class)
public class ExtractWordsProvider extends TypedSchemaTransformProvider<Configuration> {
  public static final Schema OUTPUT_SCHEMA = Schema.builder().addStringField("word").build();

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:extract_words:v1";
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        return PCollectionRowTuple.of(
            "output",
            input.get("input").apply(ParDo.of(new ExtractWordsFn())).setRowSchema(OUTPUT_SCHEMA));
      }
    };
  }

  static class ExtractWordsFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row element, OutputReceiver<Row> receiver) {
      // Split the line into words.
      String line = Preconditions.checkStateNotNull(element.getString("line"));
      String[] words = line.split("[^\\p{L}]+", -1);

      for (String word : words) {
        if (!word.isEmpty()) {
          receiver.output(Row.withSchema(OUTPUT_SCHEMA).withFieldValue("word", word).build());
        }
      }
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  protected abstract static class Configuration {}
}
