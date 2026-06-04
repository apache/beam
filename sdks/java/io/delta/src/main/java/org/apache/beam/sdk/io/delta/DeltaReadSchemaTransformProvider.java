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

import static org.apache.beam.sdk.io.delta.DeltaReadSchemaTransformProvider.Configuration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

/**
 * SchemaTransform implementation for {@link DeltaIO#readRows}. Reads records from Delta Lake and
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class DeltaReadSchemaTransformProvider extends TypedSchemaTransformProvider<Configuration> {
  static final String OUTPUT_TAG = "output";
  static final String IDENTIFIER = "beam:schematransform:org.apache.beam:delta_read:v1";

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
    return IDENTIFIER;
  }

  static class DeltaReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    DeltaReadSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      DeltaIO.ReadRows readRows = DeltaIO.readRows().from(configuration.getTablePath());
      if (configuration.getVersion() != null) {
        readRows = readRows.withVersion(configuration.getVersion());
      }
      if (configuration.getTimestamp() != null) {
        readRows = readRows.withTimestamp(configuration.getTimestamp());
      }
      Map<String, String> hadoopConfig = configuration.getHadoopConfig();
      if (hadoopConfig != null) {
        readRows = readRows.withConfig(hadoopConfig);
      }

      PCollection<Row> output = input.getPipeline().apply(readRows);
      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_DeltaReadSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Path of the Delta Lake table.")
    abstract String getTablePath();

    @SchemaFieldDescription("Version of the Delta Lake table to read.")
    @Nullable
    abstract Long getVersion();

    @SchemaFieldDescription("Timestamp of the Delta Lake table to read.")
    @Nullable
    abstract String getTimestamp();

    @SchemaFieldDescription("Hadoop configuration properties.")
    @Nullable
    abstract Map<String, String> getHadoopConfig();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTablePath(String tablePath);

      abstract Builder setVersion(@Nullable Long version);

      abstract Builder setTimestamp(@Nullable String timestamp);

      abstract Builder setHadoopConfig(@Nullable Map<String, String> hadoopConfig);

      abstract Configuration build();
    }
  }
}
