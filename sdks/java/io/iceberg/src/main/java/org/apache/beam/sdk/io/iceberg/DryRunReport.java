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

import static org.apache.beam.sdk.metrics.Metrics.counter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dry run of the schema pre-pass: computes the {@link CommitSchemaUnion#plan} a real commit would,
 * on scratch transactions only, and reports it without committing or registering anything. Planning
 * the real fold rather than classifying each schema alone is what makes a schema that is fine
 * against the table but conflicts with another schema of the input come out with the blame a real
 * run would assign.
 *
 * <p>One row per window. {@code allowed} is true when every file schema can be merged and the
 * configuration raises no problem; otherwise {@code reason} says what a real run would do about it.
 * {@code schemas} holds one entry per distinct file schema with the changes a real run would make
 * on an existing table ({@code schema_key} is a short hash to group by) and, when the schema cannot
 * be merged, why. {@code created_table} is the table a real run would create from the union of the
 * allowed schemas, absent when the table exists or no schema can seed it; {@code
 * would_create_table} is false whenever a real run would not create it, including when it would
 * fail first. {@code table_changes} lists what a real run changes without a schema change, such as
 * regenerating the name mapping.
 *
 * <p>The file counts and the counters count checked Parquet files by whether their schema is
 * allowed. Unreadable files always go to the error output in a real run; unchecked (ORC, Avro)
 * files count separately even when {@code unchecked_registered} says ACCEPT registers them. Pin
 * evidence is per file (the footer's null counts), so a pin violation or an unproven pin is not
 * predicted here.
 */
class DryRunReport extends DoFn<List<CollectDistinctSchemas.SchemaGroup>, Row> {
  private static final Logger LOG = LoggerFactory.getLogger(DryRunReport.class);

  static final String FILES_ALLOWED_COUNTER = "numDryRunFilesAllowed";
  static final String FILES_INCOMPATIBLE_COUNTER = "numDryRunFilesIncompatible";
  static final String FILES_UNREADABLE_COUNTER = "numDryRunFilesUnreadable";
  static final String FILES_UNCHECKED_COUNTER = "numDryRunFilesUnchecked";
  static final String CONFIG_PROBLEMS_COUNTER = "numDryRunConfigProblems";

  private static final Counter numFilesAllowed = counter(DryRunReport.class, FILES_ALLOWED_COUNTER);
  private static final Counter numFilesIncompatible =
      counter(DryRunReport.class, FILES_INCOMPATIBLE_COUNTER);
  private static final Counter numFilesUnreadable =
      counter(DryRunReport.class, FILES_UNREADABLE_COUNTER);
  private static final Counter numFilesUnchecked =
      counter(DryRunReport.class, FILES_UNCHECKED_COUNTER);
  private static final Counter numConfigProblems =
      counter(DryRunReport.class, CONFIG_PROBLEMS_COUNTER);

  static final Schema SCHEMA_ENTRY =
      Schema.builder()
          .addStringField("schema_key")
          .addStringField("schema")
          .addInt64Field("num_files")
          .addArrayField("changes", Schema.FieldType.STRING)
          .addBooleanField("allowed")
          .addStringField("reason")
          .build();

  static final Schema CREATED_TABLE =
      Schema.builder()
          .addStringField("schema")
          .addArrayField("columns", Schema.FieldType.STRING)
          .build();

  static final Schema REPORT_SCHEMA =
      Schema.builder()
          .addBooleanField("allowed")
          .addStringField("reason")
          .addBooleanField("would_create_table")
          .addInt64Field("files_allowed")
          .addInt64Field("files_incompatible")
          .addInt64Field("files_unreadable")
          .addInt64Field("files_unchecked")
          .addBooleanField("unchecked_registered")
          .addArrayField("table_changes", Schema.FieldType.STRING)
          .addArrayField("config_problems", Schema.FieldType.STRING)
          .addNullableRowField("created_table", CREATED_TABLE)
          .addArrayField("schemas", Schema.FieldType.row(SCHEMA_ENTRY))
          .build();

  static final String NAME_MAPPING_CHANGE =
      "regenerate the name mapping property to cover the schema";

  /** Wide inputs would otherwise put every column of every schema into one log entry. */
  private static final int MAX_RENDERED_ENTRIES = 50;

  private static final int MAX_RENDERED_CHANGES = 10;

  private final IcebergCatalogConfig catalogConfig;
  private final String identifier;
  private final CommitSchemaUnion.Settings settings;
  private transient @MonotonicNonNull Catalog catalog;

  DryRunReport(
      IcebergCatalogConfig catalogConfig, String identifier, CommitSchemaUnion.Settings settings) {
    this.catalogConfig = catalogConfig;
    this.identifier = identifier;
    this.settings = settings;
  }

  /** One {@code schemas} entry before it is a Row: the totals and the rendered log read these. */
  private static final class SchemaEntry {
    final String key;
    final String schema;
    final long files;
    final List<String> changes;
    final boolean allowed;
    final String reason;

    SchemaEntry(
        String key,
        String schema,
        long files,
        List<String> changes,
        boolean allowed,
        String reason) {
      this.key = key;
      this.schema = schema;
      this.files = files;
      this.changes = changes;
      this.allowed = allowed;
      this.reason = reason;
    }

    Row toRow() {
      return Row.withSchema(SCHEMA_ENTRY)
          .withFieldValue("schema_key", key)
          .withFieldValue("schema", schema)
          .withFieldValue("num_files", files)
          .withFieldValue("changes", changes)
          .withFieldValue("allowed", allowed)
          .withFieldValue("reason", reason)
          .build();
    }
  }

  /** The window's schema groups: the ones the plan sees, and the files that contribute none. */
  private static final class Input {
    final List<CollectDistinctSchemas.SchemaGroup> readable = new ArrayList<>();
    long unreadableFiles;
    long uncheckedFiles;

    static Input of(List<CollectDistinctSchemas.SchemaGroup> schemas) {
      Input input = new Input();
      for (CollectDistinctSchemas.SchemaGroup group : schemas) {
        if (group.getSchemaJson().equals(ReadFooterSchema.UNREADABLE_KEY)) {
          input.unreadableFiles += group.getFiles();
        } else if (group.getSchemaJson().equals(ReadFooterSchema.UNCHECKED_FORMAT_KEY)) {
          input.uncheckedFiles += group.getFiles();
        } else {
          input.readable.add(group);
        }
      }
      return input;
    }
  }

  private static final class Totals {
    int allowedSchemas;
    long allowedFiles;
    int incompatibleSchemas;
    long incompatibleFiles;

    static Totals of(List<SchemaEntry> entries) {
      Totals totals = new Totals();
      for (SchemaEntry entry : entries) {
        if (entry.allowed) {
          totals.allowedSchemas++;
          totals.allowedFiles += entry.files;
        } else {
          totals.incompatibleSchemas++;
          totals.incompatibleFiles += entry.files;
        }
      }
      return totals;
    }
  }

  @ProcessElement
  public void process(
      @Element List<CollectDistinctSchemas.SchemaGroup> schemas, OutputReceiver<Row> out) {
    if (catalog == null) {
      catalog = catalogConfig.catalog();
    }
    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(identifier);
    Input input = Input.of(schemas);
    SchemaPlan plan = CommitSchemaUnion.plan(catalog, tableId, input.readable, settings);

    List<SchemaEntry> entries = new ArrayList<>();
    for (CollectDistinctSchemas.SchemaGroup group : input.readable) {
      entries.add(schemaEntry(group, plan));
    }
    Totals totals = Totals.of(entries);

    SchemaPlan.@Nullable Creation creation = null;
    if (plan instanceof SchemaPlan.Creation) {
      creation = (SchemaPlan.Creation) plan;
    }
    @Nullable String creationProblem = creation == null ? null : creation.problem;
    boolean allowed =
        totals.incompatibleSchemas == 0 && plan.configProblems.isEmpty() && creationProblem == null;
    boolean wouldFail =
        creationProblem != null
            || (settings.handling == IncompatibleSchemaHandling.FAIL_PIPELINE && !allowed);
    boolean wouldCreateTable = creation != null && creation.canCreate() && !wouldFail;
    String consequence = consequence(totals, plan.configProblems, creationProblem);

    List<String> configProblems = new ArrayList<>(plan.configProblems);
    if (creationProblem != null) {
      configProblems.add(creationProblem);
    }
    List<String> tableChanges = new ArrayList<>();
    if (plan instanceof SchemaPlan.Evolution && ((SchemaPlan.Evolution) plan).repairsNameMapping) {
      tableChanges.add(NAME_MAPPING_CHANGE);
    }
    boolean uncheckedRegistered =
        settings.config.getUnverifiableFileHandling()
            == SchemaEvolutionConfig.UnverifiableFileHandling.ACCEPT;
    List<Row> entryRows = new ArrayList<>();
    for (SchemaEntry entry : entries) {
      entryRows.add(entry.toRow());
    }

    Row.FieldValueBuilder report =
        Row.withSchema(REPORT_SCHEMA)
            .withFieldValue("allowed", allowed)
            .withFieldValue("reason", consequence)
            .withFieldValue("would_create_table", wouldCreateTable)
            .withFieldValue("files_allowed", totals.allowedFiles)
            .withFieldValue("files_incompatible", totals.incompatibleFiles)
            .withFieldValue("files_unreadable", input.unreadableFiles)
            .withFieldValue("files_unchecked", input.uncheckedFiles)
            .withFieldValue("unchecked_registered", uncheckedRegistered)
            .withFieldValue("table_changes", tableChanges)
            .withFieldValue("config_problems", configProblems)
            .withFieldValue("schemas", entryRows);
    List<String> createdColumns = Collections.emptyList();
    if (creation != null && creation.newSchema != null) {
      createdColumns = createdColumns(creation.newSchema);
      report =
          report.withFieldValue(
              "created_table",
              Row.withSchema(CREATED_TABLE)
                  .withFieldValue("schema", SchemaParser.toJson(creation.newSchema))
                  .withFieldValue("columns", createdColumns)
                  .build());
    }
    out.output(report.build());

    numFilesAllowed.inc(totals.allowedFiles);
    numFilesIncompatible.inc(totals.incompatibleFiles);
    numFilesUnreadable.inc(input.unreadableFiles);
    numFilesUnchecked.inc(input.uncheckedFiles);
    numConfigProblems.inc(configProblems.size());
    LOG.info(
        "Dry run for {}{}: {}{}{}\n{}",
        identifier,
        wouldCreateTable ? " (table would be created)" : "",
        summary(input, totals),
        consequence.isEmpty() ? "" : "; " + consequence,
        tableChanges.isEmpty() ? "" : "; " + String.join("; ", tableChanges),
        render(entries, createdColumns));
  }

  private SchemaEntry schemaEntry(CollectDistinctSchemas.SchemaGroup group, SchemaPlan plan) {
    String json = group.getSchemaJson();
    @Nullable String incompatibleReason = plan.incompatibleReason(json);
    // a created table's columns are reported once, as created_table: no delta exists then
    List<String> changes = Collections.emptyList();
    SchemaPlan.@Nullable SchemaToMerge toMerge = plan.toMerge(json);
    if (toMerge != null && toMerge.delta != null) {
      changes = toMerge.delta.descriptions();
    }
    return new SchemaEntry(
        key(json),
        json,
        group.getFiles(),
        changes,
        incompatibleReason == null,
        incompatibleReason == null ? "" : incompatibleReason);
  }

  private static String summary(Input input, Totals totals) {
    return String.format(
        "%d distinct schemas; %d allowed covering %d files; %d incompatible covering %d files;"
            + " %d files unreadable; %d files unchecked (ORC or Avro)",
        input.readable.size(),
        totals.allowedSchemas,
        totals.allowedFiles,
        totals.incompatibleSchemas,
        totals.incompatibleFiles,
        input.unreadableFiles,
        input.uncheckedFiles);
  }

  private String consequence(
      Totals totals, List<String> configProblems, @Nullable String creationProblem) {
    IncompatibleSchemaHandling handling = settings.handling;
    List<String> parts = new ArrayList<>();
    if (creationProblem != null) {
      parts.add("a real run would fail to create the table: " + creationProblem);
    }
    if (totals.incompatibleSchemas > 0) {
      parts.add(
          handling == IncompatibleSchemaHandling.FAIL_PIPELINE
              ? "a real run would fail before committing (" + handling + ")"
              : "a real run would route "
                  + totals.incompatibleFiles
                  + " files to errors ("
                  + handling
                  + ")");
    }
    if (!configProblems.isEmpty()) {
      parts.add(
          (handling == IncompatibleSchemaHandling.FAIL_PIPELINE
                  ? "a real run would fail on the configuration: "
                  : "a real run would warn on the configuration: ")
              + String.join("; ", configProblems));
    }
    return String.join("; ", parts);
  }

  private static String render(List<SchemaEntry> entries, List<String> createdColumns) {
    StringBuilder rendered = new StringBuilder();
    for (SchemaEntry entry : entries.subList(0, Math.min(entries.size(), MAX_RENDERED_ENTRIES))) {
      rendered.append(
          String.format(
              "  %-7s %8d  allowed=%-5s %s %s%n",
              entry.key, entry.files, entry.allowed, cut(entry.changes), entry.reason));
    }
    if (entries.size() > MAX_RENDERED_ENTRIES) {
      rendered
          .append("  ... and ")
          .append(entries.size() - MAX_RENDERED_ENTRIES)
          .append(" more schemas\n");
    }
    if (!createdColumns.isEmpty()) {
      rendered.append("  created table: ").append(cut(createdColumns)).append('\n');
    }
    return rendered.toString();
  }

  private static List<String> cut(List<String> changes) {
    if (changes.size() <= MAX_RENDERED_CHANGES) {
      return changes;
    }
    List<String> shown = new ArrayList<>(changes.subList(0, MAX_RENDERED_CHANGES));
    shown.add("... and " + (changes.size() - MAX_RENDERED_CHANGES) + " more");
    return shown;
  }

  /** Top-level columns of the table a real run would create; nested detail is in the schema. */
  private static List<String> createdColumns(org.apache.iceberg.Schema created) {
    List<String> columns = new ArrayList<>();
    for (org.apache.iceberg.types.Types.NestedField field : created.columns()) {
      columns.add(
          "create "
              + (field.isOptional() ? "optional " : "required ")
              + field.name()
              + " "
              + typeLabel(field.type()));
    }
    return columns;
  }

  /** Nested types print field ids, which a creation reassigns, so only their kind is named. */
  private static String typeLabel(Type type) {
    if (type.isPrimitiveType()) {
      return type.toString();
    }
    return type.typeId().name().toLowerCase(Locale.ROOT);
  }

  static String key(String schemaJson) {
    return "s"
        + Hashing.murmur3_32_fixed().hashUnencodedChars(schemaJson).toString().substring(0, 6);
  }
}
