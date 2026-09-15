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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies the distinct file schemas of a window to the table in one commit, in phases named by the
 * methods of this class: {@code classify} each schema against a fresh load of the table (most
 * common first), {@code fold} the accepted ones into a single union on scratch transactions that
 * are never committed, {@code replay} the folded result onto the real transaction as one schema
 * update, repair the name mapping, commit once. The fold keeps per-schema blame for cross-schema
 * conflicts while the table gains a single schema version per window. Nothing is committed when
 * nothing changes.
 *
 * <p>When the table does not exist, {@code create} builds it instead: {@code foldForCreate}
 * computes the same union, and the table is born from it directly with pinned columns and their
 * ancestors required.
 *
 * <p>Incompatible schemas either fail the whole call before any commit ({@link
 * IncompatibleSchemaHandling#FAIL_PIPELINE}) or are skipped so their files reach the error output
 * at registration ({@link IncompatibleSchemaHandling#ROUTE_TO_ERRORS}).
 */
final class CommitSchemaUnion {
  private static final Logger LOG = LoggerFactory.getLogger(CommitSchemaUnion.class);

  static final int MAX_ATTEMPTS = 5;

  /** Returned when the table does not exist and there is no schema to create it from. */
  static final long NO_TABLE = -1L;

  /** How to create the table when it does not exist: from the union of the window's schemas. */
  static final class TableCreation implements Serializable {
    final @Nullable List<String> partitionFields;
    final @Nullable List<String> sortFields;
    final @Nullable Map<String, String> properties;

    TableCreation(
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields,
        @Nullable Map<String, String> properties) {
      this.partitionFields = partitionFields;
      this.sortFields = sortFields;
      this.properties = properties;
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

  private static final class Accepted {
    final Schema schema;
    final String json;
    final long files;

    /** Null on the create path: the seed table is empty, so there is nothing to relax. */
    final @Nullable SchemaDelta delta;

    Accepted(Schema schema, String json, long files, @Nullable SchemaDelta delta) {
      this.schema = schema;
      this.json = json;
      this.files = files;
      this.delta = delta;
    }
  }

  private static final class Incompatible {
    final String schemaJson;
    final long files;
    final String reason;

    Incompatible(String schemaJson, long files, String reason) {
      this.schemaJson = schemaJson;
      this.files = files;
      this.reason = reason;
    }

    @Override
    public String toString() {
      return files + " file(s) with schema " + truncate(schemaJson) + ": " + reason;
    }
  }

  /** Canonical JSON of a wide schema runs to hundreds of KB; the reason is what matters. */
  private static final int MAX_SCHEMA_JSON_CHARS = 1024;

  private static String truncate(String json) {
    if (json.length() <= MAX_SCHEMA_JSON_CHARS) {
      return json;
    }
    return json.substring(0, MAX_SCHEMA_JSON_CHARS)
        + "... ("
        + (json.length() - MAX_SCHEMA_JSON_CHARS)
        + " chars truncated)";
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
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      TableCreation creation,
      Committer committer) {
    // The catalog is already under contention when a retry fires; back off (jittered by
    // FluentBackoff) instead of piling on. Iceberg's own metadata retries (commit.retry.*)
    // sit below this loop.
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withMaxRetries(MAX_ATTEMPTS - 1)
            .withInitialBackoff(Duration.millis(100))
            .withMaxBackoff(Duration.standardSeconds(2))
            .backoff();
    for (int attempt = 1; ; attempt++) {
      try {
        return commitOnce(catalog, tableId, schemas, config, handling, creation, committer);
      } catch (CommitFailedException | AlreadyExistsException e) {
        // a concurrent commit, or a create race: the next attempt loads the fresh state
        try {
          if (!BackOffUtils.next(Sleeper.DEFAULT, backoff)) {
            throw e;
          }
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw e;
        }
        LOG.info(
            "Schema commit attempt {}/{} for {} failed; reloading and rebuilding",
            attempt,
            MAX_ATTEMPTS,
            tableId,
            e);
      }
    }
  }

  private static long commitOnce(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      TableCreation creation,
      Committer committer) {
    Table table;
    try {
      table = catalog.loadTable(tableId);
    } catch (NoSuchTableException e) {
      return create(catalog, tableId, schemas, config, handling, creation, committer);
    }
    // Every transaction below must share this snapshot: classification, the fold and the replay
    // all reason about the same table state (newTransactionOn enforces it).
    Schema base = table.schema();

    List<Incompatible> incompatible = new ArrayList<>();
    List<Accepted> accepted = classify(table, schemas, config, incompatible);
    Schema merged = fold(table, base, tableId, accepted, incompatible);

    Transaction txn = newTransactionOn(table, base, tableId);
    if (merged != null) {
      replay(txn, merged, tableId);
    }
    boolean staged = merged != null;
    staged |= stageNameMapping(txn);

    if (!incompatible.isEmpty()) {
      reportIncompatible(tableId, incompatible, handling, "no schema change was committed");
    }

    if (!staged) {
      LOG.info(
          "Table {} already covers all {} file schema(s); nothing to commit",
          tableId,
          schemas.size());
      return table.schema().schemaId();
    }
    committer.commit(txn);
    table.refresh();
    long acceptedFiles = 0;
    for (Accepted item : accepted) {
      acceptedFiles += item.files;
    }
    LOG.info(
        "Committed schema union for {}: {} schema(s) covering {} file(s), now at schema id {}",
        tableId,
        accepted.size(),
        acceptedFiles,
        table.schema().schemaId());
    return table.schema().schemaId();
  }

  /**
   * Sorts the window's schemas into the ones the table must change for (accepted) and the ones it
   * must not ({@code incompatible}, with the reason); schemas the table already covers drop out.
   */
  private static List<Accepted> classify(
      Table table,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config,
      List<Incompatible> incompatible) {
    List<Accepted> accepted = new ArrayList<>();
    for (CollectDistinctSchemas.SchemaGroup group : schemas) {
      Schema fileSchema =
          FileSchemas.markRequired(
              SchemaParser.fromJson(group.getSchemaJson()), group.getNullFreeColumns());
      SchemaDelta delta = SchemaDelta.classify(table, fileSchema);
      if (delta.isEmpty()) {
        continue;
      }
      if (!delta.allowedBy(config)) {
        incompatible.add(
            new Incompatible(
                group.getSchemaJson(), group.getFiles(), delta.disallowedReason(config)));
        continue;
      }
      accepted.add(new Accepted(fileSchema, group.getSchemaJson(), group.getFiles(), delta));
    }
    return accepted;
  }

  /**
   * Unions the accepted schemas into the table schema on scratch transactions that are never
   * committed, relaxing every field the window adds; a schema that conflicts with another only
   * surfaces here, moves to {@code incompatible} and the fold restarts without it. Returns the
   * folded schema, or null when nothing needs to change.
   */
  private static @Nullable Schema fold(
      Table table,
      Schema base,
      TableIdentifier tableId,
      List<Accepted> accepted,
      List<Incompatible> incompatible) {
    while (true) {
      Transaction scratch = newTransactionOn(table, base, tableId);
      Accepted failed = stageAll(scratch, accepted, incompatible);
      if (failed != null) {
        accepted.remove(failed);
        continue;
      }
      if (accepted.isEmpty()) {
        return null;
      }
      relaxNewRequiredFields(scratch, base);
      return scratch.table().schema();
    }
  }

  /**
   * One union replays the fold's net effect (additions, promotions, relaxations) so the table gains
   * a single schema version instead of one per folded schema. The checkState is a pure bug
   * detector: concurrent changes are caught earlier, by newTransactionOn.
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
   * columns and their ancestors, which are created required.
   */
  private static long create(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      TableCreation creation,
      Committer committer) {
    if (schemas.isEmpty()) {
      LOG.info("Table {} does not exist and no file schema was read; not creating it", tableId);
      return NO_TABLE;
    }
    List<Incompatible> incompatible = new ArrayList<>();
    Schema merged = foldForCreate(catalog, tableId, schemas, incompatible);
    if (!incompatible.isEmpty()) {
      reportIncompatible(tableId, incompatible, handling, "no table was created");
    }
    // The real table is built from the folded result directly.
    Schema created = createdSchema(merged, config);
    reportUnenforceablePins(tableId, created, config, handling);
    Map<String, String> properties =
        creation.properties == null ? new HashMap<>() : new HashMap<>(creation.properties);
    Transaction txn =
        catalog
            .buildTable(tableId, created)
            .withPartitionSpec(PartitionUtils.toPartitionSpec(creation.partitionFields, created))
            .withSortOrder(SortOrderUtils.toSortOrder(creation.sortFields, created))
            .withProperties(properties)
            .createTransaction();
    stageNameMapping(txn);
    committer.commit(txn);
    Table table = catalog.loadTable(tableId);
    LOG.info(
        "Created table {} from {} file schema(s), schema id {}",
        tableId,
        schemas.size() - incompatible.size(),
        table.schema().schemaId());
    return table.schema().schemaId();
  }

  /**
   * Unions the window's schemas into one on scratch create transactions that are never committed,
   * seeded by the most common schema; conflicts move to {@code incompatible} and the fold restarts
   * without the offender.
   */
  private static Schema foldForCreate(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      List<Incompatible> incompatible) {
    Schema seed = SchemaParser.fromJson(schemas.get(0).getSchemaJson());
    List<Accepted> rest = new ArrayList<>();
    for (CollectDistinctSchemas.SchemaGroup group : schemas.subList(1, schemas.size())) {
      Schema fileSchema = SchemaParser.fromJson(group.getSchemaJson());
      rest.add(new Accepted(fileSchema, group.getSchemaJson(), group.getFiles(), null));
    }
    while (true) {
      Transaction scratch = catalog.buildTable(tableId, seed).createTransaction();
      Accepted failed = stageAll(scratch, rest, incompatible);
      if (failed == null) {
        return scratch.table().schema();
      }
      rest.remove(failed);
    }
  }

  /**
   * A pin the created schema did not end up enforcing - the column appears in no file schema, or
   * the configured spelling resolves to a field the pin walk did not reach (a short container
   * spelling like a.b for a.element.b, or a path inside a map key) - would stay inert forever,
   * since later windows only add columns optional: a config error under FAIL_PIPELINE, a warning
   * under ROUTE_TO_ERRORS (streaming may see the column later).
   */
  private static void reportUnenforceablePins(
      TableIdentifier tableId,
      Schema created,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling) {
    List<String> unenforceable = new ArrayList<>();
    for (String pin : config.getRequiredColumns()) {
      Types.NestedField field = created.findField(pin);
      if (field == null || field.isOptional()) {
        unenforceable.add(pin);
      }
    }
    if (unenforceable.isEmpty()) {
      return;
    }
    Collections.sort(unenforceable);
    if (handling == IncompatibleSchemaHandling.FAIL_PIPELINE) {
      throw new IncompatibleSchemaException(
          "Pinned column(s) "
              + unenforceable
              + " appear in none of the file schemas creating "
              + tableId
              + ", or their spelling does not match the column path; the created table cannot"
              + " make them required");
    }
    LOG.warn(
        "Pinned column(s) {} appear in none of the file schemas creating {}, or their spelling"
            + " does not match the column path; the created table cannot make them required",
        unenforceable,
        tableId);
  }

  /**
   * The created schema: every field optional at every level, list elements and map values included,
   * except pinned paths and their ancestors, which stay required so the schema advertises the
   * guarantee the per-file pin check enforces (a null ancestor nulls the pinned leaf). Map key
   * subtrees keep their declared shape (keys are required by definition; pins inside them are not
   * honored). Nothing depends on a created table's schema yet, so this is the schema-authoring
   * moment; evolution never tightens columns afterwards.
   */
  static Schema createdSchema(Schema merged, SchemaEvolutionConfig config) {
    Pins pins = new Pins(config.getRequiredColumns());
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField field : merged.asStruct().fields()) {
      fields.add(createdField(field, field.name(), pins));
    }
    return new Schema(fields);
  }

  private static Types.NestedField createdField(Types.NestedField field, String path, Pins pins) {
    boolean required = pins.isPinned(path) || pins.pinnedColumnBeneath(path) != null;
    return Types.NestedField.from(field)
        .ofType(createdType(field.type(), path, pins))
        .isOptional(!required)
        .build();
  }

  private static Type createdType(Type type, String path, Pins pins) {
    if (type.isStructType()) {
      List<Types.NestedField> fields = new ArrayList<>();
      for (Types.NestedField field : type.asStructType().fields()) {
        fields.add(createdField(field, path + "." + field.name(), pins));
      }
      return Types.StructType.of(fields);
    }
    if (type.isListType()) {
      Types.ListType list = type.asListType();
      String elementPath = path + ".element";
      Type elementType = createdType(list.elementType(), elementPath, pins);
      boolean required =
          pins.isPinned(elementPath) || pins.pinnedColumnBeneath(elementPath) != null;
      return required
          ? Types.ListType.ofRequired(list.elementId(), elementType)
          : Types.ListType.ofOptional(list.elementId(), elementType);
    }
    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      String valuePath = path + ".value";
      Type valueType = createdType(map.valueType(), valuePath, pins);
      boolean required = pins.isPinned(valuePath) || pins.pinnedColumnBeneath(valuePath) != null;
      return required
          ? Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueType)
          : Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueType);
    }
    return type;
  }

  private static void reportIncompatible(
      TableIdentifier tableId,
      List<Incompatible> incompatible,
      IncompatibleSchemaHandling handling,
      String consequence) {
    long files = 0;
    for (Incompatible item : incompatible) {
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

  /**
   * Stages one union per accepted schema onto {@code txn}: a scratch transaction on the evolve path
   * (its per-schema versions stay in memory; only the folded result is ever committed), the create
   * transaction on the create path. A schema can conflict with another schema's additions, which
   * only surfaces while staging and poisons the transaction, so on a conflict the offender is
   * returned for the caller to drop and retry with a fresh transaction.
   */
  private static @Nullable Accepted stageAll(
      Transaction txn, List<Accepted> accepted, List<Incompatible> incompatible) {
    for (Accepted item : accepted) {
      // Both caught types carry staging conflicts: ValidationException from Schema
      // construction at apply ("multiple fields for name"), IllegalArgumentException from
      // SchemaUpdate preconditions ("Cannot change column type").
      try {
        stage(txn, item);
      } catch (ValidationException | IllegalArgumentException e) {
        incompatible.add(
            new Incompatible(
                item.json,
                item.files,
                "conflicts with another file schema in the same window: "
                    + AddFiles.errorMessage(e)));
        return item;
      }
    }
    return null;
  }

  /**
   * Iceberg refreshes the table on every {@code newTransaction()}, so a concurrent schema commit
   * can slip between two transactions here. Any drift from the snapshot the window classified
   * against is thrown as {@link CommitFailedException} so the commit-level retry reloads and
   * rebuilds, leaving the replay checkState as a pure bug detector.
   */
  private static Transaction newTransactionOn(Table table, Schema base, TableIdentifier tableId) {
    Transaction txn = table.newTransaction();
    if (!txn.table().schema().sameSchema(base)) {
      throw new CommitFailedException(
          "concurrent schema change on %s while staging the schema union", tableId);
    }
    return txn;
  }

  private static void stage(Transaction txn, Accepted item) {
    UpdateSchema update = txn.updateSchema().unionByNameWith(item.schema);
    if (item.delta != null) {
      for (String path : item.delta.absentRequiredPaths()) {
        update = update.makeColumnOptional(path);
      }
    }
    update.commit();
  }

  /**
   * New columns are optional at every level. The union adds top-level columns optional but keeps
   * the file's optionality below them, so one file's luck would otherwise impose required fields on
   * everyone. Pins do not shape new columns: they keep existing required columns from being relaxed
   * (SchemaDelta) and gate files at registration.
   */
  private static void relaxNewRequiredFields(Transaction txn, Schema before) {
    List<String> toRelax = newRequiredPaths(before, txn.table().schema());
    if (toRelax.isEmpty()) {
      return;
    }
    UpdateSchema update = txn.updateSchema();
    for (String path : toRelax) {
      update = update.makeColumnOptional(path);
    }
    update.commit();
  }

  /**
   * Paths of required fields that {@code after} has and {@code before} lacks, in schema order;
   * includes fields under lists and maps (a required list element or map value counts). Map key
   * subtrees are skipped: keys are required by definition and relaxing inside a struct key would
   * change key identity.
   */
  static List<String> newRequiredPaths(Schema before, Schema after) {
    Set<Integer> beforeIds = TypeUtil.indexById(before.asStruct()).keySet();
    List<String> paths = new ArrayList<>();
    collectNewRequired(after.asStruct(), "", beforeIds, paths);
    return paths;
  }

  private static void collectNewRequired(
      Type.NestedType type, String prefix, Set<Integer> beforeIds, List<String> paths) {
    for (Types.NestedField field : type.fields()) {
      if (type.isMapType() && field.fieldId() == type.asMapType().keyId()) {
        continue;
      }
      String path = prefix + field.name();
      if (!beforeIds.contains(field.fieldId()) && field.isRequired()) {
        paths.add(path);
      }
      if (field.type().isNestedType()) {
        collectNewRequired(field.type().asNestedType(), path + ".", beforeIds, paths);
      }
    }
  }

  /** Regenerates the name mapping when absent, malformed or not covering the staged schema. */
  private static boolean stageNameMapping(Transaction txn) {
    Schema schema = txn.table().schema();
    @Nullable NameMapping existing =
        NameMappingUtils.parseOrNull(
            txn.table().properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    if (existing != null && NameMappingUtils.covers(existing, schema.asStruct())) {
      return false;
    }
    String regenerated = NameMappingUtils.regenerate(schema, existing);
    txn.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, regenerated).commit();
    return true;
  }

  private static String joinLines(List<Incompatible> items) {
    List<String> lines = new ArrayList<>();
    for (Incompatible item : items) {
      lines.add(item.toString());
    }
    return String.join("\n  ", lines);
  }
}
