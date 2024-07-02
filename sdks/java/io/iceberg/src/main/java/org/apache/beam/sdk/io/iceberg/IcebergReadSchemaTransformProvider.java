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

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.Config;
import org.apache.beam.sdk.managed.ManagedTransformConstants;
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

/**
 * SchemaTransform implementation for {@link IcebergIO#readRows}. Reads records from Iceberg and
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergReadSchemaTransformProvider extends TypedSchemaTransformProvider<Config> {
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(Config configuration) {
    return new IcebergReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return ManagedTransformConstants.ICEBERG_READ;
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Config {
    public static Builder builder() {
      return new AutoValue_IcebergReadSchemaTransformProvider_Config.Builder();
    }

    @SchemaFieldDescription("Identifier of the Iceberg table to write to.")
    public abstract String getTable();

    @SchemaFieldDescription("Name of the catalog containing the table.")
    public abstract String getCatalogName();

    @SchemaFieldDescription("Configuration properties used to set up the Iceberg catalog.")
    public abstract Map<String, String> getCatalogProperties();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogName(String catalogName);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Config build();
    }
  }

  static class IcebergReadSchemaTransform extends SchemaTransform {
    private final Config configuration;

    IcebergReadSchemaTransform(Config configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(Config.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Properties properties = new Properties();
      properties.putAll(configuration.getCatalogProperties());

      IcebergCatalogConfig.Builder catalogBuilder =
          IcebergCatalogConfig.builder()
              .setCatalogName(configuration.getCatalogName())
              .setProperties(properties);

      PCollection<Row> output =
          input
              .getPipeline()
              .apply(
                  IcebergIO.readRows(catalogBuilder.build())
                      .from(TableIdentifier.parse(configuration.getTable())));

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }
}
