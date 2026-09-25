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
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    @SchemaFieldDescription("Properties passed to the Hadoop configuration the catalog uses.")
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

    @SchemaFieldDescription(
        "Lets the transform change the table schema so that the table has a column for every"
            + " column the files have."
            + " Values: ALLOW_FIELD_ADDITION (columns a file has and the table lacks are added, as"
            + " optional), ALLOW_FIELD_RELAXATION (a required table column becomes optional when a"
            + " file lacks it or may hold nulls in it), ALLOW_TYPE_PROMOTION (a column type is"
            + " widened, for example int to long). Leave it empty to never change the table"
            + " schema. When any option is set, the transform reads the footer of every Parquet"
            + " file and commits the allowed changes before registering any file, so every"
            + " registered file has statistics for all of its columns. A file that needs a change"
            + " that is not allowed is incompatible; see incompatible_schema_handling. Only"
            + " Parquet files can be checked: ORC and Avro files are sent to the error output"
            + " unless unverifiable_file_handling is ACCEPT. Files sent to the error output are"
            + " dropped unless error_handling is set. If the table does not exist it is created"
            + " from the union of the Parquet schemas; with none, every file goes to the error"
            + " output. Batch pipelines only; streaming pipelines cannot use schema evolution"
            + " yet.")
    public abstract @Nullable List<String> getSchemaEvolutionOptions();

    @SchemaFieldDescription(
        "Columns that must always be present and never null, as dotted paths for nested fields"
            + " (for example address.city). They are never made optional, whatever the options"
            + " allow, and are created as required when the transform creates the table. A file"
            + " that lacks one of these columns, or holds nulls in it, is sent to the error output"
            + " (see error_handling). So is a file whose footer marks the column as optional and"
            + " has no null-count statistics for it, unless unverifiable_file_handling is ACCEPT."
            + " Requires schema_evolution_options.")
    public abstract @Nullable List<String> getRequiredColumns();

    @SchemaFieldDescription(
        "When true, nothing is committed or registered: the transform reads the files' schemas"
            + " and emits a `dry_run_report` output with one row that describes what a real run"
            + " would do. Its `allowed` field is true when every file schema can be merged and the"
            + " configuration raises no problem; otherwise its `reason` field says what a real run"
            + " would do about it (fail, or route the files to the error output). Its `schemas`"
            + " field lists each distinct file schema with the changes a real run would make for"
            + " it and, when it cannot be merged, why. The output only exists when this is set;"
            + " consume it as input: `<this transform's name>.dry_run_report`. Against a missing"
            + " table, a REST catalog needs table-create permission even though no table is"
            + " created.")
    public abstract @Nullable Boolean getDryRun();

    @SchemaFieldDescription(
        "What happens when a file's schema cannot be made to fit the table: it needs a change"
            + " that is not allowed, or it conflicts with the table or with another file."
            + " FAIL_PIPELINE (the default) fails the pipeline before any schema change is"
            + " committed. ROUTE_TO_ERRORS commits the changes for the other files and sends the"
            + " incompatible files to the error output; it requires error_handling.")
    public abstract @Nullable String getIncompatibleSchemaHandling();

    @SchemaFieldDescription(
        "What happens to a file the checks cannot verify: an ORC or Avro file (the checks read"
            + " Parquet footers only), or a Parquet file with no null-count statistics for a"
            + " required column (statistics disabled by the writer, or a column under a list or"
            + " map). REJECT (the default) sends the file to the error output (see"
            + " error_handling). ACCEPT registers it without the checks; such files are counted"
            + " and logged. A file that fails a check is always sent to the error output. An"
            + " accepted file that"
            + " lacks a required column, or holds nulls in it, makes reads of the table fail.")
    public abstract @Nullable String getUnverifiableFileHandling();

    @SchemaFieldDescription(
        "Whether and where to output the files that could not be registered, as rows with the"
            + " file path and the error. Without it those files are dropped.")
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

      public abstract Builder setSchemaEvolutionOptions(List<String> options);

      public abstract Builder setRequiredColumns(List<String> columns);

      public abstract Builder setIncompatibleSchemaHandling(String handling);

      public abstract Builder setUnverifiableFileHandling(String handling);

      public abstract Builder setDryRun(Boolean dryRun);

      public abstract Configuration build();
    }

    /** Validates and converts the schema evolution settings; null when none are set. */
    public @Nullable SchemaEvolutionConfig getSchemaEvolution() {
      List<String> optionNames = getSchemaEvolutionOptions();
      List<String> pins = getRequiredColumns();
      String handlingName = getIncompatibleSchemaHandling();
      String unverifiableName = getUnverifiableFileHandling();
      boolean dryRun = Boolean.TRUE.equals(getDryRun());
      boolean nothingSet =
          (optionNames == null || optionNames.isEmpty())
              && (pins == null || pins.isEmpty())
              && handlingName == null
              && unverifiableName == null
              && !dryRun;
      if (nothingSet) {
        return null;
      }
      // SchemaEvolutionConfig.build() checks this too; this copy names the YAML keys
      Preconditions.checkArgument(
          optionNames != null && !optionNames.isEmpty(),
          "required_columns, incompatible_schema_handling, unverifiable_file_handling and"
              + " dry_run need at least one schema_evolution_options entry");
      Set<SchemaEvolutionOption> options = EnumSet.noneOf(SchemaEvolutionOption.class);
      for (String name : checkStateNotNull(optionNames)) {
        options.add(parseEnum(SchemaEvolutionOption.class, name, "schema_evolution_options"));
      }
      SchemaEvolutionConfig.Builder builder = SchemaEvolutionConfig.builder().setOptions(options);
      if (pins != null) {
        builder = builder.setRequiredColumns(new LinkedHashSet<>(pins));
      }
      builder = builder.setDryRun(dryRun);
      if (handlingName != null) {
        SchemaEvolutionConfig.IncompatibleSchemaHandling handling =
            parseEnum(
                SchemaEvolutionConfig.IncompatibleSchemaHandling.class,
                handlingName,
                "incompatible_schema_handling");
        // the error output exists only with error_handling; routed files would vanish otherwise
        Preconditions.checkArgument(
            handling != SchemaEvolutionConfig.IncompatibleSchemaHandling.ROUTE_TO_ERRORS
                || ErrorHandling.hasOutput(getErrorHandling()),
            "incompatible_schema_handling: ROUTE_TO_ERRORS needs error_handling to receive the"
                + " routed files");
        builder = builder.setIncompatibleSchemaHandling(handling);
      }
      if (unverifiableName != null) {
        builder =
            builder.setUnverifiableFileHandling(
                parseEnum(
                    SchemaEvolutionConfig.UnverifiableFileHandling.class,
                    unverifiableName,
                    "unverifiable_file_handling"));
      }
      return builder.build();
    }

    private static <T extends Enum<T>> T parseEnum(Class<T> type, String name, String option) {
      for (T value : checkStateNotNull(type.getEnumConstants())) {
        if (value.name().equalsIgnoreCase(name.trim())) {
          return value;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Invalid %s value '%s'. Valid values: %s",
              option, name, Arrays.toString(type.getEnumConstants())));
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
                      frequency != null ? Duration.standardSeconds(frequency) : null,
                      configuration.getSchemaEvolution()));

      PCollectionRowTuple output = PCollectionRowTuple.of("snapshots", result.get(OUTPUT_TAG));
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (errorHandling != null) {
        output = output.and(errorHandling.getOutput(), result.get(ERROR_TAG));
      }
      if (Boolean.TRUE.equals(configuration.getDryRun())) {
        output = output.and(AddFiles.DRY_RUN_TAG, result.get(AddFiles.DRY_RUN_TAG));
      }
      return output;
    }
  }
}
