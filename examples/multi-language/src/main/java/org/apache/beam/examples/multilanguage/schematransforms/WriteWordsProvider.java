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

import static org.apache.beam.examples.multilanguage.schematransforms.WriteWordsProvider.Configuration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.TypeDescriptors;

@AutoService(SchemaTransformProvider.class)
public class WriteWordsProvider extends TypedSchemaTransformProvider<Configuration> {
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:write_words:v1";
  }

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new WriteWordsTransform(configuration);
  }

  static class WriteWordsTransform extends SchemaTransform {
    private final String filePathPrefix;

    WriteWordsTransform(Configuration configuration) {
      this.filePathPrefix = configuration.getFilePathPrefix();
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      input
          .get("input")
          .apply(
              MapElements.into(TypeDescriptors.strings())
                  .via(row -> Preconditions.checkStateNotNull(row.getString("line"))))
          .apply(TextIO.write().to(filePathPrefix));

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_WriteWordsProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Writes to output files with this prefix.")
    public abstract String getFilePathPrefix();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFilePathPrefix(String filePathPrefix);

      public abstract Configuration build();
    }
  }
}
