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
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies the distinct file schemas of a window to the table in one transaction: fresh load,
 * classify each schema most common first, fold the allowed unions (plus explicit relaxations for
 * required columns absent from files) on a scratch transaction, stage the folded result as one
 * schema update, repair the name mapping, commit once. The fold keeps per-schema blame for
 * cross-schema conflicts while the table gains a single schema version per window; the scratch
 * transaction is never committed. Nothing is committed when nothing changes.
 *
 * <p>Incompatible schemas either fail the whole call before any commit ({@link
 * IncompatibleSchemaHandling#FAIL_PIPELINE}) or are skipped so their files reach the error output
 * at registration ({@link IncompatibleSchemaHandling#ROUTE_TO_ERRORS}).
 */
final class CommitSchemaUnion {
  private static final Logger LOG = LoggerFactory.getLogger(CommitSchemaUnion.class);

  static final int MAX_ATTEMPTS = 5;

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
      return files + " file(s) with schema " + schemaJson + ": " + reason;
    }
  }

  private CommitSchemaUnion() {}

  /**
   * Applies the schemas and returns the table's schema id after the call.
   *
   * @param schemas the window's distinct schema groups, most common first
   */
  static long commit(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config,
      IncompatibleSchemaHandling handling,
      Committer committer) {
    for (int attempt = 1; ; attempt++) {
      try {
        return commitOnce(catalog, tableId, schemas, config, handling, committer);
      } catch (CommitFailedException e) {
        if (attempt >= MAX_ATTEMPTS) {
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
      Committer committer) {
    Table table = catalog.loadTable(tableId);
    // Every transaction below must share this snapshot: classification, the fold and the replay
    // all reason about the same table state (newTransactionOn enforces it).
    Schema base = table.schema();
    List<Incompatible> incompatible = new ArrayList<>();
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

    Transaction scratch = stageAll(table, base, tableId, accepted, incompatible);
    boolean folded = !accepted.isEmpty();
    if (folded) {
      relaxNewRequiredFields(scratch, base);
    }

    Transaction txn = newTransactionOn(table, base, tableId);
    if (folded) {
      Schema merged = scratch.table().schema();
      // One union replays the fold's net effect (additions, promotions, relaxations) so the
      // table gains a single schema version instead of one per folded schema.
      txn.updateSchema().unionByNameWith(merged).commit();
      checkState(
          TypeUtil.assignIncreasingFreshIds(txn.table().schema())
              .sameSchema(TypeUtil.assignIncreasingFreshIds(merged)),
          "replaying the folded schema union for %s diverged from the fold",
          tableId);
    }
    boolean staged = folded;
    staged |= stageNameMapping(txn);

    if (!incompatible.isEmpty()) {
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
                + " file(s)); no schema change was committed:\n  "
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

  private static final class Accepted {
    final Schema schema;
    final String json;
    final long files;
    final SchemaDelta delta;

    Accepted(Schema schema, String json, long files, SchemaDelta delta) {
      this.schema = schema;
      this.json = json;
      this.files = files;
      this.delta = delta;
    }
  }

  /**
   * Folds one union per accepted schema into a scratch transaction the caller must never commit;
   * its intermediate schema versions exist only in memory. A schema can conflict with another
   * schema's additions, which only surfaces while staging and poisons the transaction, so on a
   * conflict the offender moves to {@code incompatible} and the transaction is rebuilt without it.
   */
  private static Transaction stageAll(
      Table table,
      Schema base,
      TableIdentifier tableId,
      List<Accepted> accepted,
      List<Incompatible> incompatible) {
    while (true) {
      Transaction txn = newTransactionOn(table, base, tableId);
      Accepted failed = null;
      for (Accepted item : accepted) {
        try {
          stage(txn, item);
        } catch (ValidationException | IllegalArgumentException e) {
          failed = item;
          incompatible.add(
              new Incompatible(
                  item.json,
                  item.files,
                  "conflicts with another file schema in the same window: "
                      + AddFiles.errorMessage(e)));
          break;
        }
      }
      if (failed == null) {
        return txn;
      }
      accepted.remove(failed);
    }
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
    for (String path : item.delta.absentRequiredPaths()) {
      update = update.makeColumnOptional(path);
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
