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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergCdcReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.iceberg.IcebergIO.ReadRows.StartingStrategy;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Enums;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * SchemaTransform implementation for {@link IcebergIO#readRows}. Reads records from Iceberg and
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergCdcReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new IcebergCdcReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_CDC_READ);
  }

  static class IcebergCdcReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    IcebergCdcReadSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
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
      @Nullable String strategyStr = configuration.getStartingStrategy();
      StartingStrategy strategy = null;
      if (strategyStr != null) {
        Optional<StartingStrategy> optional =
            Enums.getIfPresent(StartingStrategy.class, strategyStr.toUpperCase());
        if (!optional.isPresent()) {
          throw new IllegalArgumentException(
              "Invalid starting strategy. Valid values are: "
                  + Arrays.toString(StartingStrategy.values()));
        }
        strategy = optional.get();
      }

      IcebergIO.ReadRows readRows =
          IcebergIO.readRows(configuration.getIcebergCatalog())
              .withCdc()
              .from(TableIdentifier.parse(configuration.getTable()))
              .fromSnapshot(configuration.getFromSnapshot())
              .toSnapshot(configuration.getToSnapshot())
              .fromTimestamp(configuration.getFromTimestamp())
              .toTimestamp(configuration.getToTimestamp())
              .withStartingStrategy(strategy)
              .streaming(configuration.getStreaming())
              .keeping(configuration.getKeep())
              .dropping(configuration.getDrop())
              .withFilter(configuration.getFilter());

      @Nullable Integer pollIntervalSeconds = configuration.getPollIntervalSeconds();
      if (pollIntervalSeconds != null) {
        readRows = readRows.withPollInterval(Duration.standardSeconds(pollIntervalSeconds));
      }

      PCollection<Row> output = input.getPipeline().apply(readRows);

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_IcebergCdcReadSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("Identifier of the Iceberg table.")
    abstract String getTable();

    @SchemaFieldDescription("Name of the catalog containing the table.")
    @Nullable
    abstract String getCatalogName();

    @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
    @Nullable
    abstract Map<String, String> getCatalogProperties();

    @SchemaFieldDescription("Properties passed to the Hadoop Configuration.")
    @Nullable
    abstract Map<String, String> getConfigProperties();

    @SchemaFieldDescription("Starts reading from this snapshot ID (inclusive).")
    abstract @Nullable Long getFromSnapshot();

    @SchemaFieldDescription("Reads up to this snapshot ID (inclusive).")
    abstract @Nullable Long getToSnapshot();

    @SchemaFieldDescription(
        "Starts reading from the first snapshot (inclusive) that was created after this timestamp (in milliseconds).")
    abstract @Nullable Long getFromTimestamp();

    @SchemaFieldDescription(
        "Reads up to the latest snapshot (inclusive) created before this timestamp (in milliseconds).")
    abstract @Nullable Long getToTimestamp();

    @SchemaFieldDescription(
        "The source's starting strategy. Valid options are: \"earliest\" or \"latest\". Can be overriden "
            + "by setting a starting snapshot or timestamp. Defaults to earliest for batch, and latest for streaming.")
    abstract @Nullable String getStartingStrategy();

    @SchemaFieldDescription(
        "Enables streaming reads, where source continuously polls for snapshots forever.")
    abstract @Nullable Boolean getStreaming();

    @SchemaFieldDescription(
        "The interval at which to poll for new snapshots. Defaults to 60 seconds.")
    abstract @Nullable Integer getPollIntervalSeconds();

    @SchemaFieldDescription(
        "SQL-like predicate to filter data at scan time. Example: \"id > 5 AND status = 'ACTIVE'\". "
            + "Uses Apache Calcite syntax: https://calcite.apache.org/docs/reference.html")
    @Nullable
    abstract String getFilter();

    @SchemaFieldDescription(
        "A subset of column names to read exclusively. If null or empty, all columns will be read.")
    abstract @Nullable List<String> getKeep();

    @SchemaFieldDescription(
        "A subset of column names to exclude from reading. If null or empty, all columns will be read.")
    abstract @Nullable List<String> getDrop();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTable(String table);

      abstract Builder setCatalogName(String catalogName);

      abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      abstract Builder setConfigProperties(Map<String, String> confProperties);

      abstract Builder setFromSnapshot(Long snapshot);

      abstract Builder setToSnapshot(Long snapshot);

      abstract Builder setFromTimestamp(Long timestamp);

      abstract Builder setToTimestamp(Long timestamp);

      abstract Builder setStartingStrategy(String strategy);

      abstract Builder setPollIntervalSeconds(Integer pollInterval);

      abstract Builder setStreaming(Boolean streaming);

      abstract Builder setKeep(List<String> keep);

      abstract Builder setDrop(List<String> drop);

      abstract Builder setFilter(String filter);

      abstract Configuration build();
    }

    IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogName(getCatalogName())
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }
  }
}
