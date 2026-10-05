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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies the distinct file schemas of a window to the table in one commit: computes the {@link
 * SchemaPlan} against a fresh load of the table, {@code replay}s the planned union onto the real
 * transaction as one schema update, repairs the name mapping, commits once. The table gains a
 * single schema version per window; nothing is committed when nothing changes. A dry run reports
 * the same plan instead of committing it.
 *
 * <p>When the table does not exist, {@code create} builds it from the planned union instead, with
 * pinned columns and their ancestors required.
 *
 * <p>Incompatible schemas either fail the whole call before any commit ({@link
 * IncompatibleSchemaHandling#FAIL_PIPELINE}) or are skipped so their files reach the error output
 * at registration ({@link IncompatibleSchemaHandling#ROUTE_TO_ERRORS}).
 */
final class CommitSchemaUnion {
  private static final Logger LOG = LoggerFactory.getLogger(CommitSchemaUnion.class);

  /**
   * How long a commit or a plan keeps retrying, unless the table's own {@code commit.retry.*}
   * properties say otherwise. More patient than Iceberg's defaults because Iceberg cannot retry
   * these commits itself: a schema update inside a transaction cannot be re-applied after a
   * concurrent commit of any kind, a data append included, so every such commit lands here.
   */
  static final Duration DEFAULT_RETRY_MIN_WAIT = Duration.millis(250);

  static final Duration DEFAULT_RETRY_MAX_WAIT = Duration.standardSeconds(10);
  static final Duration DEFAULT_RETRY_TOTAL_TIMEOUT = Duration.standardMinutes(1);

  /** Tells the runner a worker waiting out a busy catalog is throttled, not busy. */
  private static final Counter throttledMillis =
      Metrics.counter(Metrics.THROTTLE_TIME_NAMESPACE, Metrics.THROTTLE_TIME_COUNTER_NAME);

  /** Returned when the table does not exist and there is no schema to create it from. */
  static final long NO_TABLE = -1L;

  /** What a table is created with when it does not exist yet; unused on an existing table. */
  static final class NewTableSettings implements Serializable {
    final @Nullable List<String> partitionFields;
    final @Nullable List<String> sortFields;
    final @Nullable Map<String, String> properties;

    NewTableSettings(
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields,
        @Nullable Map<String, String> properties) {
      this.partitionFields = partitionFields;
      this.sortFields = sortFields;
      this.properties = properties;
    }
  }

  /** How a schema commit behaves; the same for the commit and for a dry run of it. */
  static final class Settings implements Serializable {
    final SchemaEvolutionConfig config;

    /** Resolved for the pipeline's mode; the config's own value may be unset. */
    final IncompatibleSchemaHandling handling;

    final NewTableSettings newTable;

    Settings(
        SchemaEvolutionConfig config,
        IncompatibleSchemaHandling handling,
        NewTableSettings newTable) {
      this.config = config;
      this.handling = handling;
      this.newTable = newTable;
    }
  }

  /** Injectable so tests can exercise the commit retry path. */
  interface Committer extends Serializable {
    void commit(Transaction txn);
  }

  static final Committer DEFAULT_COMMITTER = Transaction::commitTransaction;

  /** Thrown under FAIL_PIPELINE; the message lists every incompatible schema. */
  static final class IncompatibleSchemaException extends IllegalStateException {
    IncompatibleSchemaException(String message) {
      super(message);
    }
  }

  private CommitSchemaUnion() {}

  /**
   * Applies the schemas and returns the table's schema id after the call, or {@link #NO_TABLE} when
   * the table is missing and there is no schema to create it from.
   *
   * @param schemas the window's distinct schema groups, most common first
   */
  static long commit(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      Settings settings,
      Committer committer) {
    return commit(catalog, tableId, schemas, settings, committer, Sleeper.DEFAULT);
  }

  /** Test hook: the sleeper keeps the retry tests from waiting out the backoff. */
  static long commit(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      Settings settings,
      Committer committer,
      Sleeper sleeper) {
    return withRetry(
        catalog,
        tableId,
        sleeper,
        table -> commitOnce(catalog, tableId, table, schemas, settings, committer));
  }

  /** One attempt at a plan or a commit, against the table as it is now; null when missing. */
  private interface Attempt<T> {
    T run(@Nullable Table table);
  }

  /**
   * Runs an attempt again, against a fresh load of the table, when a concurrent commit or a create
   * race invalidates the state it started from. Gives up when the backoff time is spent.
   */
  private static <T> T withRetry(
      Catalog catalog, TableIdentifier tableId, Sleeper sleeper, Attempt<T> once) {
    @Nullable BackOff backoff = null;
    for (int attempt = 1; ; attempt++) {
      @Nullable Table table;
      try {
        table = catalog.loadTable(tableId);
      } catch (NoSuchTableException e) {
        table = null;
      }
      try {
        return once.run(table);
      } catch (CommitFailedException | AlreadyExistsException e) {
        if (backoff == null) {
          backoff = retryBackoff(table).backoff();
        }
        LOG.info(
            "Schema pre-pass attempt {} for {} failed: {}",
            attempt,
            tableId,
            AddFiles.errorMessage(e));
        try {
          if (!BackOffUtils.next(sleeper, backoff)) {
            throw e;
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  /**
   * Jittered exponential backoff, bounded by time; a retry count only applies when the table sets
   * one. Values the backoff would reject are clamped, so a bad table property cannot replace the
   * commit failure with a configuration error.
   */
  private static FluentBackoff retryBackoff(@Nullable Table table) {
    Map<String, String> properties = table == null ? Collections.emptyMap() : table.properties();
    long minWaitMillis =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_MIN_RETRY_WAIT_MS,
            DEFAULT_RETRY_MIN_WAIT.getMillis());
    long maxWaitMillis =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_MAX_RETRY_WAIT_MS,
            DEFAULT_RETRY_MAX_WAIT.getMillis());
    long totalMillis =
        PropertyUtil.propertyAsLong(
            properties,
            TableProperties.COMMIT_TOTAL_RETRY_TIME_MS,
            DEFAULT_RETRY_TOTAL_TIMEOUT.getMillis());
    int maxRetries =
        PropertyUtil.propertyAsInt(
            properties, TableProperties.COMMIT_NUM_RETRIES, Integer.MAX_VALUE);
    return FluentBackoff.DEFAULT
        .withExponent(2.0)
        .withInitialBackoff(Duration.millis(Math.max(1, minWaitMillis)))
        .withMaxBackoff(Duration.millis(Math.max(1, maxWaitMillis)))
        .withMaxCumulativeBackoff(Duration.millis(Math.max(1, totalMillis)))
        .withMaxRetries(Math.max(0, maxRetries))
        .withThrottledTimeCounter(throttledMillis);
  }

  /** The plan for the window's schemas, reloading the table when a concurrent change moves it. */
  static SchemaPlan plan(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      Settings settings) {
    return withRetry(
        catalog,
        tableId,
        Sleeper.DEFAULT,
        table -> SchemaPlan.compute(catalog, tableId, table, schemas, settings));
  }

  private static long commitOnce(
      Catalog catalog,
      TableIdentifier tableId,
      @Nullable Table table,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      Settings settings,
      Committer committer) {
    SchemaPlan plan = SchemaPlan.compute(catalog, tableId, table, schemas, settings);
    if (plan instanceof SchemaPlan.Creation) {
      return create((SchemaPlan.Creation) plan, catalog, tableId, settings, committer);
    }
    return evolve((SchemaPlan.Evolution) plan, tableId, settings.handling, committer);
  }

  /** Replays the planned union onto the table and repairs the name mapping, in one commit. */
  private static long evolve(
      SchemaPlan.Evolution plan,
      TableIdentifier tableId,
      IncompatibleSchemaHandling handling,
      Committer committer) {
    failOrWarnOnIncompatibleSchemas(
        tableId, plan.incompatibleSchemas, handling, "no schema change was committed");
    failOrWarnOnConfigProblems(tableId, plan.configProblems, handling);

    Transaction txn = plan.newTransaction(tableId);
    if (plan.newSchema != null) {
      replay(txn, plan.newSchema, tableId);
    }
    if (plan.repairsNameMapping) {
      stageNameMapping(txn);
    }
    if (plan.newSchema == null && !plan.repairsNameMapping) {
      LOG.info("Table {} already covers every file schema; nothing to commit", tableId);
      return plan.base.schemaId();
    }
    committer.commit(txn);
    long schemaId = txn.table().schema().schemaId();
    long mergedFiles = 0;
    for (SchemaPlan.SchemaToMerge item : plan.schemasToMerge) {
      mergedFiles += item.files;
    }
    LOG.info(
        "Committed schema union for {}: {} schema(s) covering {} file(s), now at schema id {}",
        tableId,
        plan.schemasToMerge.size(),
        mergedFiles,
        schemaId);
    return schemaId;
  }

  /**
   * One union replays the fold's net effect (additions, promotions, relaxations) so the table gains
   * a single schema version instead of one per folded schema. The checkState is a pure bug
   * detector: concurrent changes are caught earlier, when the plan opens its transaction.
   */
  private static void replay(Transaction txn, Schema merged, TableIdentifier tableId) {
    txn.updateSchema().unionByNameWith(merged).commit();
    // toString of the args runs only on failure
    Schema foldResult = TypeUtil.assignIncreasingFreshIds(merged);
    Schema replayResult = TypeUtil.assignIncreasingFreshIds(txn.table().schema());
    checkState(
        replayResult.sameSchema(foldResult),
        "replaying the folded schema union for %s diverged from the fold; fold: %s replay: %s",
        tableId,
        foldResult,
        replayResult);
  }

  /**
   * Creates the table from the union of the window's schemas, with every column optional at every
   * level so that one lucky file cannot impose required columns on the table - except pinned
   * columns and their ancestors, which are created required. Columns come out in the read side's
   * canonical order (sorted by name at every level), not in any file's declared order; later unions
   * append after them.
   */
  private static long create(
      SchemaPlan.Creation plan,
      Catalog catalog,
      TableIdentifier tableId,
      Settings settings,
      Committer committer) {
    if (plan.schemasToMerge.isEmpty() && plan.incompatibleSchemas.isEmpty()) {
      LOG.info("Table {} does not exist and no file schema was read; not creating it", tableId);
      return NO_TABLE;
    }
    failOrWarnOnIncompatibleSchemas(
        tableId, plan.incompatibleSchemas, settings.handling, "no table was created");
    if (plan.newSchema == null) {
      LOG.info("Table {} does not exist and no file schema can seed it; not creating it", tableId);
      return NO_TABLE;
    }
    if (plan.problem != null) {
      throw new IllegalStateException(plan.problem);
    }
    failOrWarnOnConfigProblems(tableId, plan.configProblems, settings.handling);
    Map<String, String> properties = new HashMap<>();
    if (settings.newTable.properties != null) {
      properties.putAll(settings.newTable.properties);
    }
    Transaction txn =
        catalog
            .buildTable(tableId, plan.newSchema)
            .withPartitionSpec(checkStateNotNull(plan.spec))
            .withSortOrder(checkStateNotNull(plan.sortOrder))
            .withProperties(properties)
            .createTransaction();
    if (SchemaPlan.needsNameMapping(txn.table().properties(), txn.table().schema())) {
      stageNameMapping(txn);
    }
    committer.commit(txn);
    long schemaId = txn.table().schema().schemaId();
    LOG.info(
        "Created table {} from {} file schema(s), schema id {}",
        tableId,
        plan.schemasToMerge.size(),
        schemaId);
    return schemaId;
  }

  private static void failOrWarnOnIncompatibleSchemas(
      TableIdentifier tableId,
      List<SchemaPlan.IncompatibleSchema> incompatible,
      IncompatibleSchemaHandling handling,
      String consequence) {
    if (incompatible.isEmpty()) {
      return;
    }
    long files = 0;
    for (SchemaPlan.IncompatibleSchema item : incompatible) {
      files += item.files;
    }
    if (handling == IncompatibleSchemaHandling.FAIL_PIPELINE) {
      throw new IncompatibleSchemaException(
          "Incompatible schemas for "
              + tableId
              + " ("
              + incompatible.size()
              + " schema(s), "
              + files
              + " file(s)); "
              + consequence
              + ":\n  "
              + joinLines(incompatible));
    }
    LOG.warn(
        "Skipping {} incompatible schema(s) ({} file(s)) for {}; their files will be routed to"
            + " the error output:\n  {}",
        incompatible.size(),
        files,
        tableId,
        joinLines(incompatible));
  }

  /** A configuration problem fails the run under FAIL_PIPELINE and warns under ROUTE_TO_ERRORS. */
  private static void failOrWarnOnConfigProblems(
      TableIdentifier tableId, List<String> problems, IncompatibleSchemaHandling handling) {
    if (problems.isEmpty()) {
      return;
    }
    if (handling == IncompatibleSchemaHandling.FAIL_PIPELINE) {
      throw new IncompatibleSchemaException(String.join("; ", problems));
    }
    for (String problem : problems) {
      LOG.warn("Configuration problem on {}: {}", tableId, problem);
    }
  }

  /** Regenerates the name mapping property for the transaction's schema. */
  private static void stageNameMapping(Transaction txn) {
    Schema schema = txn.table().schema();
    @Nullable NameMapping existing =
        NameMappingUtils.parseOrNull(
            txn.table().properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    String regenerated = NameMappingUtils.regenerate(schema, existing);
    txn.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, regenerated).commit();
  }

  private static String joinLines(List<SchemaPlan.IncompatibleSchema> items) {
    List<String> lines = new ArrayList<>();
    for (SchemaPlan.IncompatibleSchema item : items) {
      lines.add(item.toString());
    }
    return String.join("\n  ", lines);
  }
}
