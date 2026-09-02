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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergDestination;
import org.apache.beam.sdk.io.iceberg.IcebergTableCreateConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-destination table resolution for the CDC sink: loads (or auto-creates) the destination {@link
 * Table}, validates that it can accept CDC writes, and precomputes the per-destination artifacts
 * the write path needs. All catalog I/O and table-level validation lives here; failures are thrown
 * as {@link TableConfigException}.
 *
 * <p>One instance lives inside each worker {@code DoFn}, single-owner and not thread-safe. Results
 * are memoized per destination string; the memo is what pins a destination's resolution, including
 * its {@link Dest#spec()}, for the worker's lifetime.
 */
final class TableSetup implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(TableSetup.class);

  private final IcebergCatalogConfig catalogConfig;
  private final CdcWriteConfig config;
  private final DynamicDestinations destinations;

  /** The run runId; a run-spec stamp carrying it names the spec {@link #resolve} pins to. */
  private final String runId;

  /**
   * Matches {@link TableCache}'s bound so this memo (whose {@link Dest}s strongly reference their
   * {@link Table}s) can never pin more table metadata than that cache would hold. Exceeding it
   * costs a re-resolve, not a failure.
   */
  private static final int MAX_MEMOIZED_DESTS = 1000;

  /** Per-destination memo, lazily initialized (never serialized). */
  private transient @Nullable Map<String, Dest> dests;

  TableSetup(
      IcebergCatalogConfig catalogConfig,
      CdcWriteConfig config,
      DynamicDestinations destinations,
      String runId) {
    this.catalogConfig = catalogConfig;
    this.config = config;
    this.destinations = destinations;
    this.runId = runId;
  }

  /**
   * Returns the resolved, validated {@link Dest} for {@code destString}, memoized per destination
   * string. Under block sharding a memoized {@link Dest} is re-checked against the table's live
   * partition spec before it is handed back ({@link #requireResolvedPartitionSpec}).
   *
   * @throws TableConfigException for any table-level problem (including catalog failures)
   */
  Dest get(String destString, Schema sourceRowSchema) {
    Map<String, Dest> memo = dests;
    if (memo == null) {
      // Access-ordered LRU: this class is single-threaded by contract, so a LinkedHashMap is the
      // whole mechanism needed to keep the memo bounded.
      memo =
          new LinkedHashMap<String, Dest>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Dest> eldest) {
              return size() > MAX_MEMOIZED_DESTS;
            }
          };
      dests = memo;
    }
    @Nullable Dest existing = memo.get(destString);
    if (existing != null) {
      requireResolvedPartitionSpec(destString, existing);
      return existing;
    }
    Dest dest;
    try {
      dest = resolve(destString, sourceRowSchema);
    } catch (TableConfigException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new TableConfigException("Failed resolving destination table '" + destString + "'.", e);
    }
    memo.put(destString, dest);
    return dest;
  }

  /** Type-stable murmur3 hash of encoded primary-key bytes. Stable across workers and JVMs. */
  static int pkHash(byte[] pkBytes) {
    return Hashing.murmur3_32_fixed().hashBytes(pkBytes).asInt();
  }

  /**
   * The deterministic shard for {@code pkBytes}: {@code floorMod(murmur3_32(pkBytes), numShards)}.
   */
  static int shardFor(byte[] pkBytes, int numShards) {
    return Math.floorMod(pkHash(pkBytes), numShards);
  }

  /**
   * The deterministic shard for an already-computed value hash, used by {@link PartitionShardPlan}
   * for the partition tuple's block base: {@code floorMod(murmur3_32(hash), numShards)}.
   */
  static int shardForHash(int hash, int numShards) {
    return Math.floorMod(Hashing.murmur3_32_fixed().hashInt(hash).asInt(), numShards);
  }

  private Dest resolve(String destString, Schema sourceRowSchema) {
    TableIdentifier identifier = IcebergUtils.parseTableIdentifier(destString);
    Table table =
        TableCache.getAndRefreshIfStale(
            catalogConfig,
            identifier,
            () -> loadOrCreateTable(identifier, destString, sourceRowSchema));

    int formatVersion = TableUtil.formatVersion(table);
    if (formatVersion < 2) {
      throw new TableConfigException(
          "CDC sink requires an Iceberg format-version >= 2 table, but '"
              + destString
              + "' is format-version "
              + formatVersion
              + ". Use IcebergIO.writeRows (the append sink) for V1 tables.");
    }

    org.apache.iceberg.Schema tableSchema = table.schema();
    Schema cdcDataSchema = cdcDataSchema(destString, sourceRowSchema, tableSchema);
    Set<Integer> equalityFieldIds = equalityFieldIds(destString, tableSchema);
    requireNonNullableEqualityFields(destString, tableSchema, equalityFieldIds);
    Schema pkSchema = pkSchema(tableSchema, cdcDataSchema, equalityFieldIds);

    RowCoder pkCoder = RowCoder.of(pkSchema);
    try {
      pkCoder.verifyDeterministic();
    } catch (Coder.NonDeterministicException e) {
      throw new TableConfigException(
          "Primary-key coder for table '"
              + destString
              + "' (schema "
              + pkSchema
              + ") is not deterministic; Iceberg identifier fields should be primitive and "
              + "required.",
          e);
    }

    PartitionSpec spec = runSpec(table);
    validatePartitioning(destString, spec, tableSchema, equalityFieldIds);

    int[] pkFieldPositions = new int[pkSchema.getFieldCount()];
    for (int i = 0; i < pkSchema.getFieldCount(); i++) {
      pkFieldPositions[i] = cdcDataSchema.indexOf(pkSchema.getField(i).getName());
    }
    ProjectionPlan projectionPlan = ProjectionPlan.of(cdcDataSchema, sourceRowSchema, destString);

    return new Dest(
        table,
        spec,
        ImmutableSet.copyOf(equalityFieldIds),
        pkSchema,
        pkCoder,
        pkFieldPositions,
        cdcDataSchema,
        projectionPlan,
        partitionShardPlan(destString, spec, tableSchema, cdcDataSchema));
  }

  /**
   * The spec to resolve and validate against. Resolves to the most recently stamped spec in this
   * run. Otherwise, falls back to the live {@code table.spec()}.
   */
  private PartitionSpec runSpec(Table table) {
    @Nullable Integer stamped = CommitToken.readRunSpec(table, config.getSinkId(), runId);
    if (stamped != null) {
      @Nullable PartitionSpec pinned = table.specs().get(stamped);
      if (pinned != null) {
        return pinned;
      }
    }
    return table.spec();
  }

  /**
   * The destination's {@link PartitionShardPlan} when {@code shards_per_partition} is below {@code
   * num_shards} AND the table is partitioned, else {@code null} (plain primary-key sharding). A cap
   * on an unpartitioned destination is a no-op with a WARN, not a rejection: rejecting would fail a
   * whole dynamic-destinations pipeline over one table, and the fallback is what the operator wants
   * anyway.
   */
  private @Nullable PartitionShardPlan partitionShardPlan(
      String destString,
      PartitionSpec spec,
      org.apache.iceberg.Schema tableSchema,
      Schema cdcDataSchema) {
    if (config.getShardsPerPartition() >= config.getNumShards()) {
      return null;
    }
    if (spec.isUnpartitioned()) {
      LOG.warn(
          "shards_per_partition ({}) is below num_shards ({}) but destination '{}' is "
              + "unpartitioned, so there is no partition to bound; ignoring the cap and sharding "
              + "by primary key across num_shards shards. The option only helps partitioned "
              + "tables.",
          config.getShardsPerPartition(),
          config.getNumShards(),
          destString);
      return null;
    }
    return PartitionShardPlan.of(spec, tableSchema, cdcDataSchema);
  }

  /**
   * Block-sharding-only drift check, run on every memo hit. Under {@code shards_per_partition} the
   * assigners derive each record's shard from the partition tuple under the resolved spec, so
   * workers resolving different specs would split one primary key's window across shards: silent
   * same-commit duplicates nothing downstream detects. The default path needs no check: the write
   * path builds writers from the pinned {@link Dest#spec()}, never the live {@code table.spec()}.
   * Freshness is best-effort: the compared spec is the process-cached table's, refreshed only when
   * something in the process refreshes it.
   */
  private static void requireResolvedPartitionSpec(String destString, Dest dest) {
    if (dest.partitionShardPlan() == null) {
      return;
    }
    int currentSpecId = dest.table().spec().specId();
    if (currentSpecId != dest.spec().specId()) {
      throw new TableConfigException(
          "Table '"
              + destString
              + "' changed its partition spec while the CDC sink was running with a "
              + "shards_per_partition cap (spec id "
              + dest.spec().specId()
              + " when the sink resolved the table, spec id "
              + currentSpecId
              + " now). Partition-block sharding derives each record's shard from the partition "
              + "tuple under the resolved spec, so workers resolving different specs would split "
              + "one primary key's window across shards and silently duplicate rows within a "
              + "commit. "
              + "Drain the pipeline before evolving the partition spec, and restart it "
              + "afterwards.");
    }
  }

  /**
   * Loads the table, auto-creating it (namespace first) if it does not exist. Auto-creation
   * requires configured equality columns (a brand-new table has no identifier fields to infer
   * from); the created schema is the source row schema minus the control columns, with the equality
   * columns as identifier fields, honoring the destination's {@link IcebergTableCreateConfig} plus
   * a format-version 2 default.
   */
  private Table loadOrCreateTable(
      TableIdentifier identifier, String destString, Schema sourceRowSchema) {
    Catalog catalog = catalogConfig.catalog();
    try {
      return catalog.loadTable(identifier);
    } catch (NoSuchTableException e) {
      // Missing table: fall through to auto-create (parity with the append sink).
    }

    @Nullable List<String> equalityColumns = config.getEqualityColumns();
    if (equalityColumns == null || equalityColumns.isEmpty()) {
      throw new TableConfigException(
          "Table '"
              + destString
              + "' does not exist and no equality_columns are configured, so its identifier "
              + "(primary-key) fields cannot be determined for auto-creation. Configure "
              + "equality_columns, or pre-create the table with identifier fields.");
    }

    Schema createSchema = config.stripControlColumns(sourceRowSchema);
    org.apache.iceberg.Schema base = IcebergUtils.beamSchemaToIcebergSchema(createSchema);
    Set<Integer> identifierFieldIds = new LinkedHashSet<>();
    for (String column : equalityColumns) {
      requireTopLevelEqualityColumn(column);
      Types.NestedField field = base.findField(column);
      if (field == null) {
        throw new TableConfigException(
            "Cannot auto-create table '"
                + destString
                + "': equality column '"
                + column
                + "' is not present in the input data schema "
                + createSchema
                + ".");
      }
      // Iceberg refuses an OPTIONAL identifier field with a cryptic error; detect the nullable
      // Beam field here so the message names the input field the user controls.
      if (createSchema.getField(column).getType().getNullable()) {
        throw new TableConfigException(
            "Cannot auto-create table '"
                + destString
                + "': equality column '"
                + column
                + "' must be non-nullable in the input schema (a nullable column cannot be an "
                + "Iceberg identifier field). Make the input field non-nullable, or pre-create "
                + "the table with required identifier fields.");
      }
      identifierFieldIds.add(field.fieldId());
    }
    org.apache.iceberg.Schema schemaWithIds =
        new org.apache.iceberg.Schema(base.columns(), identifierFieldIds);

    IcebergDestination destination = destinations.instantiateDestination(destString);
    @Nullable IcebergTableCreateConfig createConfig = destination.getTableCreateConfig();
    PartitionSpec partitionSpec =
        createConfig != null ? createConfig.getPartitionSpec() : PartitionSpec.unpartitioned();
    SortOrder sortOrder = createConfig != null ? createConfig.getSortOrder() : SortOrder.unsorted();
    Map<String, String> properties = new HashMap<>();
    if (createConfig != null) {
      @Nullable Map<String, String> createProperties = createConfig.getTableProperties();
      if (createProperties != null) {
        properties.putAll(createProperties);
      }
    }
    properties.putIfAbsent(TableProperties.FORMAT_VERSION, "2");

    Namespace namespace = identifier.namespace();
    if (!namespace.isEmpty() && catalog instanceof SupportsNamespaces) {
      SupportsNamespaces supportsNamespaces = (SupportsNamespaces) catalog;
      if (!supportsNamespaces.namespaceExists(namespace)) {
        try {
          supportsNamespaces.createNamespace(namespace);
          LOG.info("Created new namespace '{}'.", namespace);
        } catch (AlreadyExistsException ignored) {
          // Race: another worker created the namespace first.
        }
      }
    }

    try {
      Table table =
          catalog
              .buildTable(identifier, schemaWithIds)
              .withPartitionSpec(partitionSpec)
              .withSortOrder(sortOrder)
              .withProperties(properties)
              .create();
      LOG.info(
          "CDC sink auto-created table '{}' with schema {}, partition spec {}, sort order {}, "
              + "properties {}.",
          identifier,
          schemaWithIds,
          partitionSpec,
          sortOrder,
          properties);
      return table;
    } catch (AlreadyExistsException ignored) {
      // Race: another worker created the table first.
      return catalog.loadTable(identifier);
    }
  }

  /**
   * Returns the table schema as a Beam {@link Schema}, validating that the source row schema
   * (control columns stripped) matches the table's top-level column names exactly AND in the same
   * order. Order matters: the written rows and the shuffle coder are built positionally, so a
   * column reorder would silently write values into the wrong columns.
   */
  private Schema cdcDataSchema(
      String destString, Schema sourceRowSchema, org.apache.iceberg.Schema tableSchema) {
    Schema canonical = IcebergUtils.icebergSchemaToBeamSchema(tableSchema);

    String sequenceNumberColumn = config.getSequenceNumberColumn();
    rejectControlColumnCollision(
        destString, canonical, sequenceNumberColumn, "sequence_number_column");
    @Nullable String changeTypeColumn = config.getChangeTypeColumn();
    if (changeTypeColumn != null) {
      rejectControlColumnCollision(destString, canonical, changeTypeColumn, "change_type_column");
    }

    Schema stripped = config.stripControlColumns(sourceRowSchema);
    List<String> remaining = stripped.getFieldNames();
    List<String> canonicalNames = canonical.getFieldNames();
    if (!remaining.equals(canonicalNames)) {
      throw new TableConfigException(schemaMismatchMessage(destString, canonicalNames, remaining));
    }
    requireMatchingColumnTypes(destString, canonical, stripped);
    return canonical;
  }

  /**
   * Column-by-column type and nullability check behind the name check: rows are encoded against the
   * table-derived schema, so a mismatched type would only fail later as an opaque coder error. A
   * non-null input column on an optional table column is fine; the reverse is not.
   */
  private static void requireMatchingColumnTypes(
      String destString, Schema canonical, Schema stripped) {
    for (int i = 0; i < canonical.getFieldCount(); i++) {
      String name = canonical.getField(i).getName();
      Schema.FieldType tableType = canonical.getField(i).getType();
      Schema.FieldType inputType = stripped.getField(i).getType();
      if (!tableType.withNullable(false).equals(inputType.withNullable(false))) {
        throw new TableConfigException(
            "CDC data schema mismatch for table '"
                + destString
                + "': column '"
                + name
                + "' is "
                + inputType
                + " in the input but "
                + tableType
                + " in the table. Align the input schema with the table.");
      }
      if (inputType.getNullable() && !tableType.getNullable()) {
        throw new TableConfigException(
            "CDC data schema mismatch for table '"
                + destString
                + "': column '"
                + name
                + "' is nullable in the input but required in the table. Align the input schema "
                + "with the table.");
      }
    }
  }

  /** The mismatch message: unexpected/missing columns (or the order difference) plus hints. */
  private String schemaMismatchMessage(
      String destString, List<String> canonicalNames, List<String> remaining) {
    Set<String> unexpected = new LinkedHashSet<>(remaining);
    unexpected.removeAll(canonicalNames);
    Set<String> missing = new LinkedHashSet<>(canonicalNames);
    missing.removeAll(remaining);
    StringBuilder msg =
        new StringBuilder("CDC data schema mismatch for table '").append(destString).append("':");
    if (!unexpected.isEmpty()) {
      msg.append(" unexpected columns (in the input, not in the table): ")
          .append(unexpected)
          .append(";");
    }
    if (!missing.isEmpty()) {
      msg.append(" missing columns (in the table, not supplied by the input): ")
          .append(missing)
          .append(";");
    }
    if (unexpected.isEmpty() && missing.isEmpty()) {
      msg.append(
          " the input's data columns match the table's columns but in a different order;"
              + " column order must match the table (rows are projected and encoded"
              + " positionally);");
    }
    msg.append(" the sequence-number column ('")
        .append(config.getSequenceNumberColumn())
        .append("') and the change-type column are stripped before writing (configured via ")
        .append("sequence_number_column / change_type_column). Table columns: ")
        .append(canonicalNames)
        .append("; input columns after stripping: ")
        .append(remaining)
        .append(".");
    return msg.toString();
  }

  /**
   * Equality columns must be top-level: Iceberg resolves dotted paths to nested fields, which are
   * out of scope as identifier columns (and a same-named leaf could silently misbind).
   */
  private static void requireTopLevelEqualityColumn(String name) {
    if (name.contains(".")) {
      throw new TableConfigException(
          "equality_columns must be top-level columns; got '"
              + name
              + "' (nested fields are not supported).");
    }
  }

  /** Rejects a table column whose name collides with the named control-column option. */
  private static void rejectControlColumnCollision(
      String destString, Schema canonical, String controlColumn, String optionName) {
    if (canonical.hasField(controlColumn)) {
      throw new TableConfigException(
          "Table '"
              + destString
              + "' has a column '"
              + controlColumn
              + "' that collides with "
              + optionName
              + "; the sink strips that column from written rows. Rename the table column, choose "
              + "a different "
              + optionName
              + ", or duplicate the value into a differently-named data column upstream.");
    }
  }

  /**
   * The Iceberg field ids that define a row's identity: the configured equality columns (resolved
   * by name) when set, else the table's identifier fields.
   */
  private Set<Integer> equalityFieldIds(String destString, org.apache.iceberg.Schema tableSchema) {
    @Nullable List<String> override = config.getEqualityColumns();
    if (override != null) {
      if (override.isEmpty()) {
        // An empty override is a misconfiguration, not a request for the identifier fields.
        throw new TableConfigException(
            "equality_columns must be non-empty or unset (leave unset to use the identifier "
                + "fields of table '"
                + destString
                + "').");
      }
      ImmutableSet.Builder<Integer> ids = ImmutableSet.builder();
      for (String name : override) {
        requireTopLevelEqualityColumn(name);
        Types.NestedField field = tableSchema.findField(name);
        if (field == null) {
          throw new TableConfigException(
              "Configured equality column '"
                  + name
                  + "' does not exist in table '"
                  + destString
                  + "'. Table columns: "
                  + columnNames(tableSchema)
                  + ".");
        }
        ids.add(field.fieldId());
      }
      return ids.build();
    }
    Set<Integer> identifierFieldIds = tableSchema.identifierFieldIds();
    if (identifierFieldIds.isEmpty()) {
      throw new TableConfigException(
          "Table '"
              + destString
              + "' has no identifier (primary-key) fields and no equality_columns are "
              + "configured. Configure equality_columns, or add identifier fields to the table.");
    }
    return identifierFieldIds;
  }

  /** Equality columns must be required (non-null): a nullable column cannot define row identity. */
  private static void requireNonNullableEqualityFields(
      String destString, org.apache.iceberg.Schema tableSchema, Set<Integer> equalityFieldIds) {
    for (int fieldId : equalityFieldIds) {
      Types.NestedField field = checkStateNotNull(tableSchema.findField(fieldId));
      if (!field.isRequired()) {
        throw new TableConfigException(
            "Equality column '"
                + field.name()
                + "' (field id "
                + fieldId
                + ") of table '"
                + destString
                + "' must be required (non-null); a nullable column cannot define row identity.");
      }
    }
  }

  /**
   * The Beam schema of the equality columns, in ascending Iceberg field-id order (a stable,
   * table-derived order independent of how the identifier fields or overrides were declared).
   */
  private static Schema pkSchema(
      org.apache.iceberg.Schema tableSchema, Schema cdcDataSchema, Set<Integer> equalityFieldIds) {
    Schema.Builder builder = Schema.builder();
    for (int fieldId : new TreeSet<>(equalityFieldIds)) {
      Types.NestedField field = checkStateNotNull(tableSchema.findField(fieldId));
      builder.addField(cdcDataSchema.getField(field.name()));
    }
    return builder.build();
  }

  /**
   * Tables may be partitioned on any columns; two options additionally require every partition
   * source field to be an equality field, because they need a row's partition to be a pure function
   * of its primary key: {@code upsert} (before-images are dropped, so a moved row's equality delete
   * could only ever route to its new partition) and a {@code shards_per_partition} cap (the shard
   * is derived from the partition tuple).
   */
  private void validatePartitioning(
      String destString,
      PartitionSpec spec,
      org.apache.iceberg.Schema tableSchema,
      Set<Integer> equalityFieldIds) {
    if (spec.isUnpartitioned()) {
      return;
    }
    String requirement;
    if (config.getUpsert()) {
      requirement =
          "upsert drops before-images, so a row that moved partitions could never be deleted "
              + "from its old partition";
    } else if (config.getShardsPerPartition() < config.getNumShards()) {
      requirement =
          "shards_per_partition ("
              + config.getShardsPerPartition()
              + ") is below num_shards ("
              + config.getNumShards()
              + ") and derives each record's shard from its partition tuple, which must therefore "
              + "be a pure function of the primary key";
    } else {
      return;
    }
    List<String> nonKeySources = new ArrayList<>();
    for (PartitionField field : spec.fields()) {
      if (!equalityFieldIds.contains(field.sourceId())) {
        nonKeySources.add("'" + tableSchema.findColumnName(field.sourceId()) + "'");
      }
    }
    if (!nonKeySources.isEmpty()) {
      throw new TableConfigException(
          "Table '"
              + destString
              + "' has partition source columns "
              + nonKeySources
              + " that are not equality columns, but "
              + requirement
              + ". Partition only on equality columns, or drop the option.");
    }
  }

  private static List<String> columnNames(org.apache.iceberg.Schema tableSchema) {
    List<String> names = new ArrayList<>(tableSchema.columns().size());
    for (Types.NestedField field : tableSchema.columns()) {
      names.add(field.name());
    }
    return names;
  }

  /**
   * Precomputed per-destination state. All fields are fixed at resolution time; the {@link
   * #projectionPlan()} additionally self-heals when a row's source schema drifts.
   */
  static final class Dest {
    private final Table table;
    private final PartitionSpec spec;
    private final Set<Integer> equalityFieldIds;
    private final Schema pkSchema;
    private final RowCoder pkCoder;
    private final int[] pkFieldPositions;
    private final Schema cdcDataSchema;
    private final ProjectionPlan projectionPlan;
    private final @Nullable PartitionShardPlan partitionShardPlan;

    private Dest(
        Table table,
        PartitionSpec spec,
        Set<Integer> equalityFieldIds,
        Schema pkSchema,
        RowCoder pkCoder,
        int[] pkFieldPositions,
        Schema cdcDataSchema,
        ProjectionPlan projectionPlan,
        @Nullable PartitionShardPlan partitionShardPlan) {
      this.table = table;
      this.spec = spec;
      this.equalityFieldIds = equalityFieldIds;
      this.pkSchema = pkSchema;
      this.pkCoder = pkCoder;
      this.pkFieldPositions = pkFieldPositions;
      this.cdcDataSchema = cdcDataSchema;
      this.projectionPlan = projectionPlan;
      this.partitionShardPlan = partitionShardPlan;
    }

    /**
     * The destination table, loaded or auto-created. This is the process-shared {@link TableCache}
     * instance, which may be refreshed in place, so its live metadata can be newer than the
     * memoized schemas held here.
     */
    Table table() {
      return table;
    }

    /**
     * The partition spec this destination was resolved and validated against: the worker's pin. The
     * live {@link #table()} can be refreshed onto a newer spec; the write path must build writers
     * from this pinned spec.
     */
    PartitionSpec spec() {
      return spec;
    }

    /** The Iceberg field ids of the equality (primary-key) columns. */
    Set<Integer> equalityFieldIds() {
      return equalityFieldIds;
    }

    /** The Beam schema of the equality columns, in ascending Iceberg field-id order. */
    Schema pkSchema() {
      return pkSchema;
    }

    /** A deterministic coder for {@link #pkSchema()} rows. */
    RowCoder pkCoder() {
      return pkCoder;
    }

    /**
     * Position of each {@link #pkSchema()} field within {@link #cdcDataSchema()}; do not mutate.
     */
    int[] pkFieldPositions() {
      return pkFieldPositions;
    }

    /** The written-row schema: the table's schema in Beam form (control columns stripped). */
    Schema cdcDataSchema() {
      return cdcDataSchema;
    }

    /** The positional source-to-{@link #cdcDataSchema()} projection. */
    ProjectionPlan projectionPlan() {
      return projectionPlan;
    }

    /** The partition-block sharding plan, or {@code null} to shard by plain primary-key hash. */
    @Nullable PartitionShardPlan partitionShardPlan() {
      return partitionShardPlan;
    }
  }

  /**
   * Positional projection from a source row schema to the CDC data schema, dropping the control
   * columns. One mapping is cached; {@link #project} rebuilds it by field name when a row's schema
   * drifts from the build-time source schema, and {@link #matches} lets callers observe the drift.
   */
  static final class ProjectionPlan {
    private final Schema targetSchema;

    private final String destString;

    /** The source schema the current {@link #positions} were computed from. */
    private Schema sourceSchema;

    /** For each {@link #targetSchema} field, its index in {@link #sourceSchema}. */
    private int[] positions;

    private boolean driftWarned;

    private ProjectionPlan(
        Schema targetSchema, Schema sourceSchema, int[] positions, String destString) {
      this.targetSchema = targetSchema;
      this.sourceSchema = sourceSchema;
      this.positions = positions;
      this.destString = destString;
    }

    static ProjectionPlan of(Schema targetSchema, Schema sourceSchema, String destString) {
      return new ProjectionPlan(
          targetSchema, sourceSchema, positionsFor(targetSchema, sourceSchema), destString);
    }

    /** Whether the current mapping was built for {@code schema}. */
    @SuppressWarnings("ReferenceEquality")
    boolean matches(Schema schema) {
      return sourceSchema == schema || sourceSchema.equals(schema);
    }

    /** Projects {@code source} to the target schema positionally. */
    Row project(Row source) {
      Schema schema = source.getSchema();
      if (!matches(schema)) {
        if (!driftWarned) {
          driftWarned = true;
          LOG.warn(
              "CDC sink destination '{}' received a row whose schema drifted from the cached "
                  + "projection plan; positions were re-resolved by name. This is correct but "
                  + "slower. Supply rows with a stable schema to avoid it.",
              destString);
        }
        positions = positionsFor(targetSchema, schema);
        sourceSchema = schema;
      }
      List<@Nullable Object> values = new ArrayList<>(positions.length);
      for (int position : positions) {
        values.add(source.getValue(position));
      }
      return Row.withSchema(targetSchema).attachValues(values);
    }

    /**
     * For each {@code targetSchema} field, its index in {@code sourceSchema}; {@code indexOf} makes
     * a drifted-away column fail loudly by name.
     */
    private static int[] positionsFor(Schema targetSchema, Schema sourceSchema) {
      int[] positions = new int[targetSchema.getFieldCount()];
      for (int i = 0; i < targetSchema.getFieldCount(); i++) {
        positions[i] = sourceSchema.indexOf(targetSchema.getField(i).getName());
      }
      return positions;
    }
  }

  /**
   * A table-level (as opposed to record-level) configuration failure: the destination table (or the
   * sink configuration as applied to it) cannot accept CDC writes at all. Callers rethrow this
   * fail-fast, bypassing any per-record poison-record handling.
   */
  static final class TableConfigException extends RuntimeException {
    TableConfigException(String message) {
      super(message);
    }

    TableConfigException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
