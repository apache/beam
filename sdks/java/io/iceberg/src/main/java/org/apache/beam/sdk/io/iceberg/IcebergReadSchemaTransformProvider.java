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

import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.Configuration;
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
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SchemaTransform implementation for {@link IcebergIO#readRows}. Reads records from Iceberg and
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<Configuration> {
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Configuration configuration) {
    return new IcebergReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_READ);
  }

  static class IcebergReadSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    IcebergReadSchemaTransform(Configuration configuration) {
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
      PCollection<Row> output =
          input
              .getPipeline()
              .apply(
                  IcebergIO.readRows(configuration.getIcebergCatalog())
                      .from(TableIdentifier.parse(configuration.getTable()))
                      .keeping(configuration.getKeep())
                      .dropping(configuration.getDrop())
                      .withFilter(configuration.getFilter()));

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    static Builder builder() {
      return new AutoValue_IcebergReadSchemaTransformProvider_Configuration.Builder();
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

    @SchemaFieldDescription(
        "A subset of column names to read exclusively. If null or empty, all columns will be read.")
    abstract @Nullable List<String> getKeep();

    @SchemaFieldDescription(
        "A subset of column names to exclude from reading. If null or empty, all columns will be read.")
    abstract @Nullable List<String> getDrop();

    @SchemaFieldDescription(
        "SQL-like predicate to filter data at scan time. Example: \"id > 5 AND status = 'ACTIVE'\". "
            + "Uses Apache Calcite syntax: https://calcite.apache.org/docs/reference.html")
    @Nullable
    abstract String getFilter();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTable(String table);

      abstract Builder setCatalogName(String catalogName);

      abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      abstract Builder setConfigProperties(Map<String, String> confProperties);

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
