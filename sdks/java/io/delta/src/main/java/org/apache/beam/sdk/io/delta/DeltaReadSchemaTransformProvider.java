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
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.delta.DeltaReadSchemaTransformProvider.Configuration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
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
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class DeltaReadSchemaTransformProvider extends TypedSchemaTransformProvider<Configuration> {
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
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.DELTA_LAKE_READ);
  }

  static class DeltaReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    DeltaReadSchemaTransform(Configuration configuration) {
      this.configuration =
          java.util.Objects.requireNonNull(configuration, "configuration cannot be null");
    }

    Row getConfigurationRow() {
      try {
        return SchemaRegistry.createDefault()
            .getToRowFunction(Configuration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      DeltaIO.ReadRows read = DeltaIO.readRows().from(configuration.getTable());
      if (configuration.getVersion() != null) {
        read = read.withVersion(configuration.getVersion());
      }
      if (configuration.getTimestamp() != null) {
        read = read.withTimestamp(configuration.getTimestamp());
      }
      Map<String, String> hadoopConfig = configuration.getHadoopConfig();
      if (hadoopConfig != null) {
        read = read.withConfig(hadoopConfig);
      }

      PCollection<Row> output = input.getPipeline().apply(read);

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_DeltaReadSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Identifier of the Delta Lake table.")
    abstract String getTable();

    @SchemaFieldDescription("Version of the Delta Lake table to read.")
    @Nullable
    abstract Long getVersion();

    @SchemaFieldDescription("Timestamp of the Delta Lake table to read.")
    @Nullable
    abstract String getTimestamp();

    @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
    @Nullable
    abstract Map<String, String> getHadoopConfig();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTable(String table);

      abstract Builder setVersion(@Nullable Long version);

      abstract Builder setTimestamp(@Nullable String timestamp);

      abstract Builder setHadoopConfig(@Nullable Map<String, String> hadoopConfig);

      abstract Configuration build();
    }
  }
}
