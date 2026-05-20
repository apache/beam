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
package org.apache.beam.sdk.io.delta;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/** SchemaTransformProvider for {@link DeltaIO#read}. */
@AutoService(SchemaTransformProvider.class)
public class DeltaReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<DeltaReadSchemaTransformProvider.Configuration> {

  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new DeltaReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:delta_read:v1";
  }

  static class DeltaReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    DeltaReadSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> output =
          input
              .getPipeline()
              .apply(
                  DeltaIO.readRows()
                      .from(configuration.getTablePath())
                      .withVersion(configuration.getVersion())
                      .withTimestamp(configuration.getTimestamp()));

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_DeltaReadSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Path to the Delta Lake table.")
    abstract String getTablePath();

    @SchemaFieldDescription("Version of the Delta table to read.")
    @Nullable
    abstract Long getVersion();

    @SchemaFieldDescription("Timestamp of the Delta table to read.")
    @Nullable
    abstract String getTimestamp();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTablePath(String tablePath);

      abstract Builder setVersion(Long version);

      abstract Builder setTimestamp(String timestamp);

      abstract Configuration build();
    }
  }
}
