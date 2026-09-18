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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Schema evolution settings for {@link AddFiles}. With no options the table schema is never changed
 * and files register as on a plain AddFiles; every other setting requires at least one option.
 *
 * <pre>{@code
 * SchemaEvolutionConfig.builder()
 *     .setOptions(EnumSet.of(ALLOW_FIELD_ADDITION, ALLOW_FIELD_RELAXATION, ALLOW_TYPE_PROMOTION))
 *     .setRequiredColumns(Set.of("id", "address.city"))   // never relaxed
 *     .setIncompatibleSchemaHandling(IncompatibleSchemaHandling.ROUTE_TO_ERRORS)
 *     .build();
 * }</pre>
 *
 * <p><b>Pins.</b> Required columns are pinned: never made optional whatever the options say, and
 * created required when this transform creates the table. A Parquet file that lacks a pinned column
 * or has nulls in it is routed to the error output. Pins name canonical (table) paths, dotted for
 * nested fields, with the container segment spelled out under lists and maps ({@code
 * addresses.element.city}, {@code attributes.value.total}). A top-level column whose own name
 * contains a dot cannot be pinned.
 *
 * <p><b>Unverifiable files.</b> The per-file checks read Parquet footers. An ORC or Avro file
 * cannot be checked at all, and a Parquet file whose footer carries no null-count statistics for a
 * pinned column (a writer with statistics disabled, or a pin under a list or map, whose physical
 * chunk path the check does not map) cannot prove the pin. {@link UnverifiableFileHandling} decides
 * whether such a file is routed to the error output (the default) or registered on trust.
 *
 * <p><b>Incompatible schemas.</b> A schema that needs a change the options do not allow, or that
 * conflicts with the table or with another file's schema. {@link IncompatibleSchemaHandling}
 * decides whether that fails the pipeline before any schema commit (the batch default) or skips the
 * schema so its files reach the error output (the streaming default). Files whose footer cannot be
 * read or converted always go to the error output and never fail the pipeline.
 *
 * <p><b>Dry run.</b> Reports what a real run would do, per distinct file schema, on the {@code
 * dry_run_report} output; nothing is committed or registered. The report is a PCollection like any
 * other, so attach a sink to keep it; the rendered table is also logged at INFO and its totals are
 * published as counters ({@code numDryRunFilesAllowed}, {@code numDryRunFilesIncompatible}, {@code
 * numDryRunFilesUnreadable}, {@code numDryRunFilesUnchecked}, {@code numDryRunConfigProblems}).
 * Read the summary row first: {@code allowed} is the verdict and {@code reason} the consequence;
 * then each row with {@code allowed} false names the option or conflict to fix; a {@code create}
 * row shows the table a real run would create. Adjust the settings, rerun until the summary is
 * allowed, then run for real with an error output attached. Against a missing table the dry run
 * computes the union through the catalog's create-transaction API, which a REST catalog serves as a
 * stage-create request: the credentials need table-create permission even though no table is
 * created.
 */
@AutoValue
public abstract class SchemaEvolutionConfig implements Serializable {

  public enum IncompatibleSchemaHandling {
    /**
     * Fail the pipeline before committing any schema change, with a message listing every
     * incompatible schema, its reason and file count. The batch default, and batch only: in
     * streaming a failing window's commit would be retried forever and hold every later window, so
     * {@link AddFiles} rejects this setting for unbounded input.
     */
    FAIL_PIPELINE,
    /**
     * Skip the incompatible schema, commit the rest, and route its files to the error output with
     * the specific reason. The streaming default.
     */
    ROUTE_TO_ERRORS
  }

  /**
   * What to do with a file the per-file checks cannot verify: a non-Parquet file, or a Parquet file
   * with no null-count statistics for a pinned column. A file that fails a check is always routed
   * to the error output.
   */
  public enum UnverifiableFileHandling {
    /** Route the file to the error output. The default: "cannot prove" is not "proven". */
    REJECT,
    /**
     * Register the file unchecked, counted ({@code numUncheckedFormatFiles}, {@code
     * numUnprovenPinFiles}) and logged. A trusted file that lacks a required column or holds nulls
     * in one breaks reads of the table at query time, not at registration. A non-Parquet file never
     * contributes to schema inference, so it cannot seed a missing table.
     */
    ACCEPT
  }

  public abstract Set<SchemaEvolutionOption> getOptions();

  /**
   * Canonical column paths (dotted for nested fields) that are never relaxed and are created
   * required; files that cannot prove they hold no nulls in them go to the error output.
   */
  public abstract Set<String> getRequiredColumns();

  public boolean isPinned(String columnPath) {
    return getRequiredColumns().contains(columnPath);
  }

  /**
   * Report what the pre-pass would do on the {@code dry_run_report} output (one row per distinct
   * file schema plus a summary row per window); commit and register nothing.
   */
  public abstract boolean getDryRun();

  /**
   * Unset resolves by mode: {@code FAIL_PIPELINE} in batch, {@code ROUTE_TO_ERRORS} in streaming.
   */
  public abstract @Nullable IncompatibleSchemaHandling getIncompatibleSchemaHandling();

  public abstract UnverifiableFileHandling getUnverifiableFileHandling();

  /** The handling to apply: the configured one, or the default for the input's mode when unset. */
  public IncompatibleSchemaHandling incompatibleSchemaHandlingFor(PCollection.IsBounded mode) {
    IncompatibleSchemaHandling handling = getIncompatibleSchemaHandling();
    if (handling != null) {
      return handling;
    }
    return mode == PCollection.IsBounded.BOUNDED
        ? IncompatibleSchemaHandling.FAIL_PIPELINE
        : IncompatibleSchemaHandling.ROUTE_TO_ERRORS;
  }

  public boolean isEnabled() {
    return !getOptions().isEmpty();
  }

  public boolean allows(SchemaEvolutionOption option) {
    return getOptions().contains(option);
  }

  public static SchemaEvolutionConfig disabled() {
    return builder().build();
  }

  public static SchemaEvolutionConfig of(SchemaEvolutionOption... options) {
    Set<SchemaEvolutionOption> set = EnumSet.noneOf(SchemaEvolutionOption.class);
    Collections.addAll(set, options);
    return builder().setOptions(set).build();
  }

  public static Builder builder() {
    return new AutoValue_SchemaEvolutionConfig.Builder()
        .setOptions(Collections.emptySet())
        .setRequiredColumns(Collections.emptySet())
        .setUnverifiableFileHandling(UnverifiableFileHandling.REJECT)
        .setDryRun(false);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setOptions(Set<SchemaEvolutionOption> options);

    public abstract Builder setRequiredColumns(Set<String> requiredColumns);

    public abstract Builder setIncompatibleSchemaHandling(
        @Nullable IncompatibleSchemaHandling handling);

    public abstract Builder setUnverifiableFileHandling(UnverifiableFileHandling handling);

    public abstract Builder setDryRun(boolean dryRun);

    abstract SchemaEvolutionConfig autoBuild();

    /** Any setting without an option would silently do nothing, so they are rejected. */
    public SchemaEvolutionConfig build() {
      SchemaEvolutionConfig config = autoBuild();
      for (String column : config.getRequiredColumns()) {
        Preconditions.checkArgument(
            !column.trim().isEmpty() && column.equals(column.trim()),
            "required column is blank or has surrounding whitespace: '%s'",
            column);
      }
      Preconditions.checkArgument(
          config.isEnabled()
              || (config.getRequiredColumns().isEmpty()
                  && !config.getDryRun()
                  && config.getIncompatibleSchemaHandling() == null
                  && config.getUnverifiableFileHandling() == UnverifiableFileHandling.REJECT),
          "required columns, dry run, incompatible schema handling and unverifiable file"
              + " handling need at least one schema evolution option");
      return config;
    }
  }
}
