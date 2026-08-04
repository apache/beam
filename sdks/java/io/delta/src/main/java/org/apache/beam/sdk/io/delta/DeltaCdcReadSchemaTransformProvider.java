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

import static org.apache.beam.sdk.io.delta.DeltaCdcReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
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
 * SchemaTransform implementation for {@link DeltaIO#readChanges}. Reads change records from Delta
 * Lake and outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class DeltaCdcReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new DeltaCdcReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.DELTA_LAKE_CDC_READ);
  }

  static class DeltaCdcReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    DeltaCdcReadSchemaTransform(Configuration configuration) {
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
      DeltaIO.ReadChanges read = DeltaIO.readChanges().from(configuration.getTable());
      Long startVersion = configuration.getStartVersion();
      if (startVersion != null) {
        read = read.withStartVersion(startVersion);
      }
      String startTimestamp = configuration.getStartTimestamp();
      if (startTimestamp != null) {
        read = read.withStartTimestamp(startTimestamp);
      }
      Long endVersion = configuration.getEndVersion();
      if (endVersion != null) {
        read = read.withEndVersion(endVersion);
      }
      String endTimestamp = configuration.getEndTimestamp();
      if (endTimestamp != null) {
        read = read.withEndTimestamp(endTimestamp);
      }
      Map<String, String> hadoopConfig = configuration.getHadoopConfig();
      if (hadoopConfig != null) {
        read = read.withConfig(hadoopConfig);
      }
      List<String> includeMetadataColumns = configuration.getIncludeMetadataColumns();
      if (includeMetadataColumns != null && !includeMetadataColumns.isEmpty()) {
        read = read.withMetadataColumns(includeMetadataColumns.toArray(new String[0]));
      }

      PCollection<Row> output = input.getPipeline().apply(read);

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_DeltaCdcReadSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Identifier of the Delta Lake table.")
    abstract String getTable();

    @SchemaFieldDescription(
        "Start version of the Delta Lake table to read changes from. Either this or the start timestamp has to be provided.")
    @Nullable
    abstract Long getStartVersion();

    @SchemaFieldDescription(
        "Start timestamp of the Delta Lake table to read changes from. Should be specified in the ISO 8601 standard. Either this or the start version has to be provided.")
    @Nullable
    abstract String getStartTimestamp();

    @SchemaFieldDescription("End version of the Delta Lake table to read changes up to.")
    @Nullable
    abstract Long getEndVersion();

    @SchemaFieldDescription(
        "End timestamp of the Delta Lake table to read changes up to. Should be specified in the ISO 8601 standard.")
    @Nullable
    abstract String getEndTimestamp();

    @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
    @Nullable
    abstract Map<String, String> getHadoopConfig();

    @SchemaFieldDescription(
        "Metadata columns to include in the output rows. Supported columns are: _change_type, _commit_version, and _commit_timestamp.")
    @Nullable
    abstract List<String> getIncludeMetadataColumns();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTable(String table);

      abstract Builder setStartVersion(Long startVersion);

      abstract Builder setStartTimestamp(String startTimestamp);

      abstract Builder setEndVersion(Long endVersion);

      abstract Builder setEndTimestamp(String endTimestamp);

      abstract Builder setHadoopConfig(Map<String, String> hadoopConfig);

      abstract Builder setIncludeMetadataColumns(List<String> includeMetadataColumns);

      abstract Configuration build();
    }
  }
}
