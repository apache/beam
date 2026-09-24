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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
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

/**
 * What one schema commit would do, as data: which distinct file schemas it merges into the table,
 * which it refuses and why, and the schema the table ends up with. {@link #compute} works it out on
 * scratch transactions that are never committed: {@code classify} each schema against the table
 * (most common first), then {@code fold} the accepted ones into a single union, which keeps
 * per-schema blame for cross-schema conflicts. {@link CommitSchemaUnion} executes the plan and the
 * dry run reports it, so a check added here reaches both and the two cannot drift.
 */
abstract class SchemaPlan {
  final List<SchemaToMerge> schemasToMerge;
  final List<IncompatibleSchema> incompatibleSchemas;

  /** The union to replay, or the schema to create the table with; null when nothing changes. */
  final @Nullable Schema newSchema;

  /** Problems the configuration raises against the planned schema, as messages. */
  final List<String> configProblems;

  private SchemaPlan(Verdicts verdicts, @Nullable Schema newSchema, List<String> configProblems) {
    this.schemasToMerge = verdicts.toMerge;
    this.incompatibleSchemas = verdicts.incompatible;
    this.newSchema = newSchema;
    this.configProblems = configProblems;
  }

  /**
   * The schema's entry when the table must change for it; null when the schema is incompatible or
   * the table already covers it.
   */
  @Nullable SchemaToMerge toMerge(String schemaJson) {
    for (SchemaToMerge item : schemasToMerge) {
      if (item.json.equals(schemaJson)) {
        return item;
      }
    }
    return null;
  }

  /** Why the schema is incompatible; null when it is compatible. */
  @Nullable String incompatibleReason(String schemaJson) {
    for (IncompatibleSchema item : incompatibleSchemas) {
      if (item.schemaJson.equals(schemaJson)) {
        return item.reason;
      }
    }
    return null;
  }

  /** The plan against an existing table. */
  static final class Evolution extends SchemaPlan {
    final Table table;

    /** The table schema every decision was made against. */
    final Schema base;

    /** Whether the commit regenerates the name mapping property, schema change or not. */
    final boolean repairsNameMapping;

    private Evolution(
        Table table,
        Schema base,
        Verdicts verdicts,
        @Nullable Schema newSchema,
        boolean repairsNameMapping,
        List<String> configProblems) {
      super(verdicts, newSchema, configProblems);
      this.table = table;
      this.base = base;
      this.repairsNameMapping = repairsNameMapping;
    }

    /** A transaction on the table, checked against the snapshot this plan reasoned about. */
    Transaction newTransaction(TableIdentifier tableId) {
      return transactionOn(table, base, tableId);
    }
  }

  /** The plan when the table does not exist; {@code newSchema} is what it would be created with. */
  static final class Creation extends SchemaPlan {
    /** Set together with {@link #sortOrder} unless {@link #problem} is. */
    final @Nullable PartitionSpec spec;

    final @Nullable SortOrder sortOrder;

    /** Why the table cannot be created as configured; fails a real run under either handling. */
    final @Nullable String problem;

    private Creation(
        Verdicts verdicts,
        @Nullable Schema newSchema,
        @Nullable PartitionSpec spec,
        @Nullable SortOrder sortOrder,
        @Nullable String problem,
        List<String> configProblems) {
      super(verdicts, newSchema, configProblems);
      this.spec = spec;
      this.sortOrder = sortOrder;
      this.problem = problem;
    }

    /** No file schema is left to create the table from. */
    static Creation nothingToCreate(Verdicts verdicts) {
      return new Creation(verdicts, null, null, null, null, new ArrayList<>());
    }

    static Creation of(
        Verdicts verdicts,
        Schema created,
        PartitionSpec spec,
        SortOrder sortOrder,
        List<String> configProblems) {
      return new Creation(verdicts, created, spec, sortOrder, null, configProblems);
    }

    /** The configured partition or sort fields do not fit the schema. */
    static Creation blocked(
        Verdicts verdicts, Schema created, String problem, List<String> configProblems) {
      return new Creation(verdicts, created, null, null, problem, configProblems);
    }

    /** There is a schema to create from, and the configured partition and sort fields fit it. */
    boolean canCreate() {
      return newSchema != null && problem == null;
    }
  }

  /** One distinct file schema the table must change for, and may under the options. */
  static final class SchemaToMerge {
    final Schema schema;
    final String json;
    final long files;

    /** Null on the create path: the seed table is empty, so there is nothing to relax. */
    final @Nullable SchemaDelta delta;

    SchemaToMerge(Schema schema, String json, long files, @Nullable SchemaDelta delta) {
      this.schema = schema;
      this.json = json;
      this.files = files;
      this.delta = delta;
    }
  }

  /** One distinct file schema the commit refuses, with the reason. */
  static final class IncompatibleSchema {
    final String schemaJson;
    final long files;
    final String reason;

    IncompatibleSchema(String schemaJson, long files, String reason) {
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

  /** Where each distinct file schema of the window stands while the plan is worked out. */
  private static final class Verdicts {
    final List<SchemaToMerge> toMerge = new ArrayList<>();
    final List<IncompatibleSchema> incompatible = new ArrayList<>();

    void refuse(CollectDistinctSchemas.SchemaGroup group, String reason) {
      incompatible.add(new IncompatibleSchema(group.getSchemaJson(), group.getFiles(), reason));
    }

    void refuse(Conflict conflict) {
      toMerge.remove(conflict.schema);
      incompatible.add(
          new IncompatibleSchema(conflict.schema.json, conflict.schema.files, conflict.reason));
    }
  }

  /**
   * A schema that fits the table on its own but not the union of the window: a column another
   * schema of the window adds has the same name with a different type, or a name differing only in
   * case. Classification checks each schema against the table alone, so this surfaces only while
   * the union is staged.
   */
  private static final class Conflict {
    final SchemaToMerge schema;
    final String reason;

    Conflict(SchemaToMerge schema, String reason) {
      this.schema = schema;
      this.reason = reason;
    }
  }

  /**
   * The plan for the window's schemas against the table as loaded ({@code null} when it does not
   * exist). Nothing is committed; on a missing table the fold goes through the catalog's create
   * transaction, which a REST catalog serves as a stage-create request.
   *
   * @param schemas the window's distinct schema groups, most common first
   */
  static SchemaPlan compute(
      Catalog catalog,
      TableIdentifier tableId,
      @Nullable Table table,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      CommitSchemaUnion.Settings settings) {
    if (table == null) {
      return planCreation(catalog, tableId, schemas, settings);
    }
    return planEvolution(table, tableId, schemas, settings.config);
  }

  private static Evolution planEvolution(
      Table table,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config) {
    Schema base = table.schema();
    if (schemas.isEmpty()) {
      // the pipeline never commits for a window without schemas; a direct caller gets the same
      return new Evolution(table, base, new Verdicts(), null, false, new ArrayList<>());
    }
    Verdicts verdicts = classify(table, base, schemas, config);
    @Nullable Schema newSchema = fold(table, base, tableId, verdicts);
    Schema afterwards = newSchema != null ? newSchema : base;
    boolean repairsNameMapping = needsNameMapping(table.properties(), afterwards);
    return new Evolution(table, base, verdicts, newSchema, repairsNameMapping, new ArrayList<>());
  }

  private static Creation planCreation(
      Catalog catalog,
      TableIdentifier tableId,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      CommitSchemaUnion.Settings settings) {
    Verdicts verdicts = classifyForCreate(schemas);
    @Nullable Schema union = foldForCreate(catalog, tableId, verdicts);
    if (union == null) {
      return Creation.nothingToCreate(verdicts);
    }
    Schema created = createdSchema(union, settings.config);
    List<String> configProblems = new ArrayList<>();
    addPinProblems(tableId, created, settings.config, configProblems);
    CommitSchemaUnion.NewTableSettings newTable = settings.newTable;
    try {
      PartitionSpec spec = PartitionUtils.toPartitionSpec(newTable.partitionFields, created);
      SortOrder sortOrder = SortOrderUtils.toSortOrder(newTable.sortFields, created);
      return Creation.of(verdicts, created, spec, sortOrder, configProblems);
    } catch (IllegalArgumentException | ValidationException e) {
      String problem =
          "Table "
              + tableId
              + " cannot be created with partition fields "
              + newTable.partitionFields
              + " and sort fields "
              + newTable.sortFields
              + " on the union of the file schemas: "
              + AddFiles.errorMessage(e);
      return Creation.blocked(verdicts, created, problem, configProblems);
    }
  }

  private static void addPinProblems(
      TableIdentifier tableId,
      Schema created,
      SchemaEvolutionConfig config,
      List<String> configProblems) {
    List<String> unenforceable = unenforceablePins(created, config);
    if (!unenforceable.isEmpty()) {
      configProblems.add(
          "Pinned column(s) "
              + unenforceable
              + " appear in none of the file schemas creating "
              + tableId
              + ", or their spelling does not match the column path; the created table cannot"
              + " make them required");
    }
  }

  /**
   * Sorts the window's schemas into the ones the table must change for ({@code toMerge}) and the
   * ones it must not ({@code incompatible}, with the reason); schemas the table already covers drop
   * out.
   */
  private static Verdicts classify(
      Table table,
      Schema base,
      List<CollectDistinctSchemas.SchemaGroup> schemas,
      SchemaEvolutionConfig config) {
    Verdicts verdicts = new Verdicts();
    for (CollectDistinctSchemas.SchemaGroup group : schemas) {
      Schema fileSchema =
          FileSchemas.markRequired(
              SchemaParser.fromJson(group.getSchemaJson()), group.getNullFreeColumns());
      SchemaDelta delta = SchemaDelta.classify(table, base, fileSchema);
      if (delta.isEmpty()) {
        continue;
      }
      if (!delta.allowedBy(config)) {
        verdicts.refuse(group, delta.disallowedReason(config));
        continue;
      }
      verdicts.toMerge.add(
          new SchemaToMerge(fileSchema, group.getSchemaJson(), group.getFiles(), delta));
    }
    return verdicts;
  }

  /**
   * Unions the accepted schemas into the table schema on scratch transactions that are never
   * committed, relaxing every field the window adds; a schema that conflicts with another only
   * surfaces here, moves to {@code incompatible} and the fold restarts without it. Returns the
   * folded schema, or null when nothing needs to change.
   */
  private static @Nullable Schema fold(
      Table table, Schema base, TableIdentifier tableId, Verdicts verdicts) {
    while (!verdicts.toMerge.isEmpty()) {
      Transaction scratch = transactionOn(table, base, tableId);
      @Nullable Conflict conflict = stageAll(scratch, verdicts.toMerge);
      if (conflict == null) {
        relaxNewRequiredFields(scratch, base);
        return scratch.table().schema();
      }
      verdicts.refuse(conflict);
    }
    return null;
  }

  /**
   * The evolve path refuses names no table can absorb (dotted, empty, differing only in case within
   * one file) as conflicts in classify; a table must not be born with them either.
   */
  private static Verdicts classifyForCreate(List<CollectDistinctSchemas.SchemaGroup> schemas) {
    Verdicts verdicts = new Verdicts();
    for (CollectDistinctSchemas.SchemaGroup group : schemas) {
      Schema fileSchema = SchemaParser.fromJson(group.getSchemaJson());
      List<SchemaChange> invalidNames = new ArrayList<>();
      ColumnNameChecks.findInvalidNames(fileSchema.asStruct(), "", invalidNames);
      if (!invalidNames.isEmpty()) {
        verdicts.refuse(
            group, "file schema has column names no table can hold: " + describe(invalidNames));
        continue;
      }
      verdicts.toMerge.add(
          new SchemaToMerge(fileSchema, group.getSchemaJson(), group.getFiles(), null));
    }
    return verdicts;
  }

  /**
   * Unions the accepted schemas into one on scratch create transactions that are never committed,
   * seeded by the most common schema; a schema that conflicts with the others moves to {@code
   * incompatible}, leaves {@code toMerge}, and the fold restarts without it. Returns the union, or
   * null when no schema is left to create from. A REST catalog serves each create transaction as a
   * stage-create request, so the caller needs table-create permission even in a dry run.
   */
  private static @Nullable Schema foldForCreate(
      Catalog catalog, TableIdentifier tableId, Verdicts verdicts) {
    List<SchemaToMerge> toMerge = verdicts.toMerge;
    while (!toMerge.isEmpty()) {
      Schema seed = toMerge.get(0).schema;
      Transaction scratch = catalog.buildTable(tableId, seed).createTransaction();
      @Nullable Conflict conflict = stageAll(scratch, toMerge.subList(1, toMerge.size()));
      if (conflict == null) {
        return scratch.table().schema();
      }
      verdicts.refuse(conflict);
    }
    return null;
  }

  /**
   * Pins the created schema did not end up enforcing: the column appears in no file schema, or the
   * configured spelling resolves to a field the pin walk did not reach (a short container spelling
   * like a.b for a.element.b, or a path inside a map key). Such a pin would stay inert forever,
   * since later windows only add columns optional, so the plan reports it as a configuration
   * problem.
   */
  static List<String> unenforceablePins(Schema created, SchemaEvolutionConfig config) {
    List<String> unenforceable = new ArrayList<>();
    for (String pin : config.getRequiredColumns()) {
      Types.NestedField field = created.findField(pin);
      if (field == null || field.isOptional()) {
        unenforceable.add(pin);
      }
    }
    Collections.sort(unenforceable);
    return unenforceable;
  }

  /**
   * The created schema: every field optional at every level, list elements and map values included,
   * except pinned paths and their ancestors, which stay required so the schema advertises the
   * guarantee the per-file pin check enforces (a null ancestor nulls the pinned leaf). Map key
   * subtrees keep their declared shape (keys are required by definition; pins inside them are not
   * honored). Nothing depends on a created table's schema yet, so this is the schema-authoring
   * moment; evolution never tightens columns afterwards. Column order is the union's, which is the
   * canonical (name-sorted) order of the file schemas.
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
    boolean required = pins.isPinnedOrAncestorOfPin(path);
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
      boolean required = pins.isPinnedOrAncestorOfPin(elementPath);
      return required
          ? Types.ListType.ofRequired(list.elementId(), elementType)
          : Types.ListType.ofOptional(list.elementId(), elementType);
    }
    if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      String valuePath = path + ".value";
      Type valueType = createdType(map.valueType(), valuePath, pins);
      boolean required = pins.isPinnedOrAncestorOfPin(valuePath);
      return required
          ? Types.MapType.ofRequired(map.keyId(), map.valueId(), map.keyType(), valueType)
          : Types.MapType.ofOptional(map.keyId(), map.valueId(), map.keyType(), valueType);
    }
    return type;
  }

  /**
   * Stages one union per accepted schema onto {@code txn}: a scratch transaction on the evolve path
   * (its per-schema versions stay in memory; only the folded result is ever committed), the create
   * transaction on the create path. A schema can conflict with another schema's additions, which
   * only surfaces while staging and poisons the transaction, so on a conflict the offender is
   * returned for the caller to drop and retry with a fresh transaction.
   */
  private static @Nullable Conflict stageAll(Transaction txn, List<SchemaToMerge> toMerge) {
    for (SchemaToMerge item : toMerge) {
      // classify checked each schema against the base table only; a column that differs only in
      // case from one an EARLIER schema of the window added would union as a second column.
      List<SchemaChange> collisions = new ArrayList<>();
      ColumnNameChecks.findCaseCollisions(
          txn.table().schema().asStruct(), item.schema.asStruct(), "", collisions);
      if (!collisions.isEmpty()) {
        return new Conflict(
            item, "conflicts with another file schema in the same window: " + describe(collisions));
      }
      // Both caught types carry staging conflicts: ValidationException from Schema
      // construction at apply ("multiple fields for name"), IllegalArgumentException from
      // SchemaUpdate preconditions ("Cannot change column type").
      try {
        stage(txn, item);
      } catch (ValidationException | IllegalArgumentException e) {
        return new Conflict(
            item,
            "conflicts with another file schema in the same window: " + AddFiles.errorMessage(e));
      }
    }
    return null;
  }

  private static String describe(List<SchemaChange> changes) {
    List<String> descriptions = new ArrayList<>();
    for (SchemaChange change : changes) {
      descriptions.add(change.description);
    }
    return String.join("; ", descriptions);
  }

  /**
   * Iceberg refreshes the table on every {@code newTransaction()}, so a concurrent schema commit
   * can slip between two transactions here. Any drift from the snapshot the window classified
   * against is thrown as {@link CommitFailedException} so the retry in {@link CommitSchemaUnion}
   * reloads and rebuilds, leaving the replay checkState as a pure bug detector.
   */
  static Transaction transactionOn(Table table, Schema base, TableIdentifier tableId) {
    Transaction txn = table.newTransaction();
    if (!txn.table().schema().sameSchema(base)) {
      throw new CommitFailedException(
          "concurrent schema change on %s while staging the schema union", tableId);
    }
    return txn;
  }

  private static void stage(Transaction txn, SchemaToMerge item) {
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

  /** The table's name mapping property is absent, malformed or does not cover {@code schema}. */
  static boolean needsNameMapping(Map<String, String> tableProperties, Schema schema) {
    @Nullable NameMapping existing =
        NameMappingUtils.parseOrNull(tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING));
    return existing == null || !NameMappingUtils.covers(existing, schema.asStruct());
  }
}
