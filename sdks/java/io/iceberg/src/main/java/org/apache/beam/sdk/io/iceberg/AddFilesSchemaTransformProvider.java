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

import static org.apache.beam.sdk.io.iceberg.AddFiles.ERROR_TAG;
import static org.apache.beam.sdk.io.iceberg.AddFiles.OUTPUT_TAG;
import static org.apache.beam.sdk.io.iceberg.AddFilesSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoService(SchemaTransformProvider.class)
public class AddFilesSchemaTransformProvider extends TypedSchemaTransformProvider<Configuration> {
  @Override
  public AddFilesSchemaTransform from(Configuration configuration) {
    return new AddFilesSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:iceberg_add_files:v1";
  }

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Configuration {
    public static Builder builder() {
      return new AutoValue_AddFilesSchemaTransformProvider_Configuration.Builder();
    }

    @SchemaFieldDescription("A fully-qualified table identifier.")
    public abstract String getTable();

    @SchemaFieldDescription("Properties used to set up the Iceberg catalog.")
    public abstract @Nullable Map<String, String> getCatalogProperties();

    @SchemaFieldDescription("Properties passed to the Hadoop ")
    public abstract @Nullable Map<String, String> getConfigProperties();

    @SchemaFieldDescription(
        "For a streaming pipeline, sets the frequency at which incoming files are appended (default 600, or 10min).")
    public abstract @Nullable Integer getTriggeringFrequencySeconds();

    @SchemaFieldDescription("The number of data files per manifest (default 10,000 files).")
    public abstract @Nullable Integer getManifestFileSize();

    @SchemaFieldDescription(
        "The prefix shared among all partitions. For example, a data file may have the following"
            + " location:%n"
            + "'gs://bucket/namespace/table/data/id=13/name=beam/data_file.parquet'%n%n"
            + "The provided prefix should go up until the partition information:%n"
            + "'gs://bucket/namespace/table/data/'.%n"
            + "If not provided, will try determining each DataFile's partition from its metrics metadata.")
    public abstract @Nullable String getLocationPrefix();

    @SchemaFieldDescription(
        "Fields used to create a partition spec that is applied when tables are created. For a field 'foo', "
            + "the available partition transforms are:\n\n"
            + "- `foo`\n"
            + "- `truncate(foo, N)`\n"
            + "- `bucket(foo, N)`\n"
            + "- `hour(foo)`\n"
            + "- `day(foo)`\n"
            + "- `month(foo)`\n"
            + "- `year(foo)`\n"
            + "- `void(foo)`\n\n"
            + "For more information on partition transforms, please visit https://iceberg.apache.org/spec/#partition-transforms.")
    public abstract @Nullable List<String> getPartitionFields();

    @SchemaFieldDescription(
        "Iceberg table properties to be set on the table when it is created.\n"
            + "For more information on table properties,"
            + " please visit https://iceberg.apache.org/docs/latest/configuration/#table-properties.")
    public abstract @Nullable Map<String, String> getTableProperties();

    @SchemaFieldDescription(
        "Fields used to set the table's sort order, applied when the table is created. "
            + "Each entry has the form `<term> [asc|desc] [nulls first|nulls last]`, where `<term>` "
            + "is a field name or one of the partition transforms (e.g. `bucket(col, 4)`, `day(ts)`). "
            + "Direction defaults to ascending; null order defaults to nulls-first for ascending and "
            + "nulls-last for descending.\n"
            + "For more information on sort orders, please visit https://iceberg.apache.org/spec/#sort-orders.")
    public abstract @Nullable List<String> getSortFields();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    public abstract @Nullable ErrorHandling getErrorHandling();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setTable(String table);

      public abstract Builder setCatalogProperties(Map<String, String> catalogProperties);

      public abstract Builder setConfigProperties(Map<String, String> confProperties);

      public abstract Builder setTriggeringFrequencySeconds(Integer triggeringFrequencySeconds);

      public abstract Builder setManifestFileSize(Integer size);

      public abstract Builder setLocationPrefix(String prefix);

      public abstract Builder setPartitionFields(List<String> fields);

      public abstract Builder setTableProperties(Map<String, String> props);

      public abstract Builder setSortFields(List<String> sortFields);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Configuration build();
    }

    public IcebergCatalogConfig getIcebergCatalog() {
      return IcebergCatalogConfig.builder()
          .setCatalogProperties(getCatalogProperties())
          .setConfigProperties(getConfigProperties())
          .build();
    }
  }

  public static class AddFilesSchemaTransform extends SchemaTransform {
    private final Configuration configuration;

    public AddFilesSchemaTransform(Configuration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema inputSchema = input.getSinglePCollection().getSchema();
      Preconditions.checkState(
          inputSchema.getFieldCount() == 1
              && inputSchema.getField(0).getType().getTypeName().equals(Schema.TypeName.STRING),
          "Incoming Row Schema must contain only one field of type String. Instead, got schema: %s",
          inputSchema);

      @Nullable Integer frequency = configuration.getTriggeringFrequencySeconds();

      PCollectionRowTuple result =
          input
              .getSinglePCollection()
              .apply("Filter empty paths", Filter.by(row -> row.getString(0) != null))
              .apply(
                  "ExtractPaths",
                  MapElements.into(TypeDescriptors.strings())
                      .via(row -> checkStateNotNull(row.getString(0))))
              .apply(
                  new AddFiles(
                      configuration.getIcebergCatalog(),
                      configuration.getTable(),
                      configuration.getLocationPrefix(),
                      configuration.getPartitionFields(),
                      configuration.getSortFields(),
                      configuration.getTableProperties(),
                      configuration.getManifestFileSize(),
                      frequency != null ? Duration.standardSeconds(frequency) : null));

      PCollectionRowTuple output = PCollectionRowTuple.of("snapshots", result.get(OUTPUT_TAG));
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        output = output.and(errorHandling.getOutput(), result.get(ERROR_TAG));
      }
      return output;
    }
  }
}
