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
 * <p>Rows, by {@code row_type}: {@code schema}, one per distinct schema, with the changes a real
 * run would make on an existing table ({@code schema_key} is a short hash to group by); {@code
 * create} for the table a real run would create, once, from the union of the allowed schemas;
 * {@code unreadable} for files whose schema could not be read; {@code unchecked} for ORC and Avro
 * files the per-file checks cannot verify; and {@code summary}. The summary is {@code allowed} when
 * no schema is incompatible and the configuration raises no problem; its {@code changes} hold the
 * totals line and any table-level change a real run would make without a schema change, such as
 * regenerating the name mapping. {@code would_create_table} is false whenever a real run would not
 * create the table, including when it would fail first.
 *
 * <p>The counters and the totals count checked Parquet files by their schema's verdict; unchecked
 * files count separately even when ACCEPT registers them, so the unchecked row can be {@code
 * allowed} while its files are outside {@code numDryRunFilesAllowed}. Pin evidence is per file (the
 * footer's null counts), so a pin violation or an unproven pin is not predicted here.
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

  static final Schema REPORT_SCHEMA =
      Schema.builder()
          .addStringField("row_type")
          .addStringField("schema_key")
          .addStringField("schema")
          .addInt64Field("num_files")
          .addArrayField("changes", Schema.FieldType.STRING)
          .addBooleanField("allowed")
          .addStringField("reason")
          .addBooleanField("would_create_table")
          .build();

  static final String SCHEMA_ROW = "schema";
  static final String CREATE_ROW = "create";
  static final String UNREADABLE_ROW = "unreadable";
  static final String UNCHECKED_ROW = "unchecked";
  static final String SUMMARY_ROW = "summary";

  static final String NAME_MAPPING_CHANGE =
      "regenerate the name mapping property to cover the schema";

  /** Wide inputs would otherwise put every column of every schema into one log entry. */
  private static final int MAX_RENDERED_ROWS = 50;

  private static final int MAX_RENDERED_CHANGES = 10;

  static final String UNREADABLE_REASON =
      "the file schema could not be read (unknown format, or an unreadable footer);"
          + " a real run routes these files to the error output";

  static final String UNCHECKED_REJECTED_REASON =
      "ORC and Avro files cannot be checked; a real run routes these files to the error output"
          + " (UnverifiableFileHandling.REJECT)";

  static final String UNCHECKED_ACCEPTED_REASON =
      "ORC and Avro files cannot be checked; a real run registers these files unchecked"
          + " (UnverifiableFileHandling.ACCEPT)";

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

  /** One report row before it is a Row: the summary and the rendered log read these. */
  private static final class Line {
    final String rowType;
    final String schemaKey;
    final String schema;
    final long files;
    final List<String> changes;
    final boolean allowed;
    final String reason;

    Line(
        String rowType,
        String schemaKey,
        String schema,
        long files,
        List<String> changes,
        boolean allowed,
        String reason) {
      this.rowType = rowType;
      this.schemaKey = schemaKey;
      this.schema = schema;
      this.files = files;
      this.changes = changes;
      this.allowed = allowed;
      this.reason = reason;
    }

    Row toRow(boolean wouldCreateTable) {
      return Row.withSchema(REPORT_SCHEMA)
          .withFieldValue("row_type", rowType)
          .withFieldValue("schema_key", schemaKey)
          .withFieldValue("schema", schema)
          .withFieldValue("num_files", files)
          .withFieldValue("changes", changes)
          .withFieldValue("allowed", allowed)
          .withFieldValue("reason", reason)
          .withFieldValue("would_create_table", wouldCreateTable)
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

    static Totals of(List<Line> schemaLines) {
      Totals totals = new Totals();
      for (Line line : schemaLines) {
        if (line.allowed) {
          totals.allowedSchemas++;
          totals.allowedFiles += line.files;
        } else {
          totals.incompatibleSchemas++;
          totals.incompatibleFiles += line.files;
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
    CommitSchemaUnion.Plan plan =
        CommitSchemaUnion.plan(catalog, tableId, input.readable, settings);

    List<Line> lines = new ArrayList<>();
    for (CollectDistinctSchemas.SchemaGroup group : input.readable) {
      lines.add(schemaLine(group, plan));
    }
    Totals totals = Totals.of(lines);

    boolean repairsNameMapping =
        plan instanceof CommitSchemaUnion.EvolutionPlan
            && ((CommitSchemaUnion.EvolutionPlan) plan).repairsNameMapping;
    CommitSchemaUnion.@Nullable CreationPlan creation = null;
    if (plan instanceof CommitSchemaUnion.CreationPlan) {
      creation = (CommitSchemaUnion.CreationPlan) plan;
    }
    @Nullable String creationProblem = creation == null ? null : creation.problem;
    boolean wouldFail =
        creationProblem != null
            || (settings.handling == IncompatibleSchemaHandling.FAIL_PIPELINE
                && (totals.incompatibleSchemas > 0 || !plan.configProblems.isEmpty()));
    boolean wouldCreateTable = creation != null && creation.canCreate() && !wouldFail;

    if (creation != null && creation.newSchema != null) {
      lines.add(
          createLine(creation.newSchema, totals.allowedFiles, wouldCreateTable, creationProblem));
    }
    if (input.unreadableFiles > 0) {
      lines.add(unreadableLine(input.unreadableFiles));
    }
    if (input.uncheckedFiles > 0) {
      lines.add(uncheckedLine(input.uncheckedFiles));
    }

    String summary = summary(input, totals);
    String consequence = consequence(totals, plan.configProblems, creationProblem);
    List<String> summaryChanges = new ArrayList<>();
    summaryChanges.add(summary);
    if (repairsNameMapping) {
      summaryChanges.add(NAME_MAPPING_CHANGE);
    }
    Line summaryLine =
        new Line(
            SUMMARY_ROW,
            "",
            "",
            totals.allowedFiles
                + totals.incompatibleFiles
                + input.unreadableFiles
                + input.uncheckedFiles,
            summaryChanges,
            totals.incompatibleSchemas == 0
                && plan.configProblems.isEmpty()
                && creationProblem == null,
            consequence);

    for (Line line : lines) {
      out.output(line.toRow(wouldCreateTable));
    }
    out.output(summaryLine.toRow(wouldCreateTable));
    numFilesAllowed.inc(totals.allowedFiles);
    numFilesIncompatible.inc(totals.incompatibleFiles);
    numFilesUnreadable.inc(input.unreadableFiles);
    numFilesUnchecked.inc(input.uncheckedFiles);
    numConfigProblems.inc(plan.configProblems.size() + (creationProblem == null ? 0 : 1));
    LOG.info(
        "Dry run for {}{}: {}{}\n{}",
        identifier,
        wouldCreateTable ? " (table would be created)" : "",
        summary,
        consequence.isEmpty() ? "" : "; " + consequence,
        render(lines));
  }

  private Line schemaLine(CollectDistinctSchemas.SchemaGroup group, CommitSchemaUnion.Plan plan) {
    String json = group.getSchemaJson();
    @Nullable String incompatibleReason = plan.incompatibleReason(json);
    // a created table's columns are reported once, on the create row: no delta exists then
    List<String> changes = Collections.emptyList();
    CommitSchemaUnion.@Nullable SchemaToMerge toMerge = plan.toMerge(json);
    if (toMerge != null && toMerge.delta != null) {
      changes = toMerge.delta.descriptions();
    }
    return new Line(
        SCHEMA_ROW,
        key(json),
        json,
        group.getFiles(),
        changes,
        incompatibleReason == null,
        incompatibleReason == null ? "" : incompatibleReason);
  }

  private static Line createLine(
      org.apache.iceberg.Schema created,
      long files,
      boolean wouldCreateTable,
      @Nullable String creationProblem) {
    String reason = "";
    if (creationProblem != null) {
      reason = creationProblem;
    } else if (!wouldCreateTable) {
      reason = "a real run fails before creating the table (see the summary row)";
    }
    return new Line(
        CREATE_ROW,
        "",
        SchemaParser.toJson(created),
        files,
        createdColumns(created),
        wouldCreateTable,
        reason);
  }

  private static Line unreadableLine(long files) {
    return new Line(
        UNREADABLE_ROW, "", "", files, Collections.emptyList(), false, UNREADABLE_REASON);
  }

  private Line uncheckedLine(long files) {
    boolean accepted =
        settings.config.getUnverifiableFileHandling()
            == SchemaEvolutionConfig.UnverifiableFileHandling.ACCEPT;
    return new Line(
        UNCHECKED_ROW,
        "",
        "",
        files,
        Collections.emptyList(),
        accepted,
        accepted ? UNCHECKED_ACCEPTED_REASON : UNCHECKED_REJECTED_REASON);
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

  private static String render(List<Line> lines) {
    StringBuilder rendered = new StringBuilder();
    for (Line line : lines.subList(0, Math.min(lines.size(), MAX_RENDERED_ROWS))) {
      rendered.append(
          String.format(
              "  %-10s %-7s %8d  allowed=%-5s %s %s%n",
              line.rowType,
              line.schemaKey,
              line.files,
              line.allowed,
              cut(line.changes),
              line.reason));
    }
    if (lines.size() > MAX_RENDERED_ROWS) {
      rendered.append("  ... and ").append(lines.size() - MAX_RENDERED_ROWS).append(" more rows\n");
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
    List<String> changes = new ArrayList<>();
    for (org.apache.iceberg.types.Types.NestedField field : created.columns()) {
      changes.add(
          "create "
              + (field.isOptional() ? "optional " : "required ")
              + field.name()
              + " "
              + typeLabel(field.type()));
    }
    return changes;
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
