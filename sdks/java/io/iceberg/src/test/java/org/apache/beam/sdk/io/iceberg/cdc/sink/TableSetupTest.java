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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergDestination;
import org.apache.beam.sdk.io.iceberg.IcebergTableCreateConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TableSetup}. */
@RunWith(JUnit4.class)
public class TableSetupTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  /** Canonical test table schema: {@code id INT (required)}, {@code name}/{@code data} STRING. */
  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  /** Data schema for {@link #ICEBERG_SCHEMA}. */
  private static final Schema DATA_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("data", Schema.FieldType.STRING)
          .build();

  private Catalog catalog;
  private IcebergCatalogConfig catalogConfig;

  @Before
  public void setUp() {
    catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
    catalogConfig = CdcSinkTestUtils.catalogConfig(tmp.getRoot());
  }

  private static TableIdentifier uniqueId(String prefix) {
    return TableIdentifier.of("db", prefix + "_" + System.nanoTime());
  }

  /** A fresh unpartitioned V2 {@link #ICEBERG_SCHEMA} table (PK {@code id}), named from prefix. */
  private TableIdentifier v2Table(String prefix) {
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    return id;
  }

  /** {@link #v2Table} partitioned by {@code bucket(column, buckets)}. */
  private TableIdentifier bucketPartitionedTable(String prefix, String column, int buckets) {
    TableIdentifier id = uniqueId(prefix);
    PartitionSpec spec = PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket(column, buckets).build();
    CdcSinkTestUtils.createTable(catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, spec);
    return id;
  }

  private static CdcWriteConfig.Builder cfg() {
    return CdcWriteConfig.builder().setSinkId("test-sink").setNumShards(8).setShardsPerPartition(8);
  }

  private TableSetup tableSetup(CdcWriteConfig config, DynamicDestinations destinations) {
    return new TableSetup(catalogConfig, config, destinations, "test-runId");
  }

  private TableSetup tableSetup(CdcWriteConfig config) {
    return tableSetup(config, new TestDestinations(DATA_SCHEMA, null, null));
  }

  private static Schema dataSchemaFor(org.apache.iceberg.Schema icebergSchema) {
    return IcebergUtils.icebergSchemaToBeamSchema(icebergSchema);
  }

  // -------------------------------------------------------------------------------------------
  // Loading and Dest population
  // -------------------------------------------------------------------------------------------

  @Test
  public void loadsExistingTableAndPopulatesDest() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "num", Types.LongType.get()));
    TableIdentifier id = uniqueId("existing");
    // Identifier fields deliberately declared out of ascending-field-id order: {3, 1}.
    CdcSinkTestUtils.createTable(
        catalog, id, schema, ImmutableSet.of(3, 1), 2, PartitionSpec.unpartitioned());

    Schema sourceSchema = dataSchemaFor(schema);
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(sourceSchema, null, null));

    TableSetup.Dest dest = setup.get(id.toString(), sourceSchema);

    assertThat(dest.table().name(), containsString(id.name()));
    assertThat(dest.equalityFieldIds(), containsInAnyOrder(1, 3));
    // pkSchema is in ascending field-id order even though the identifiers were declared {3, 1}.
    assertThat(dest.pkSchema().getFieldNames(), contains("id", "num"));
    assertThat(dest.cdcDataSchema(), equalTo(IcebergUtils.icebergSchemaToBeamSchema(schema)));
    assertArrayEquals(new int[] {0, 2}, dest.pkFieldPositions());
    assertThat(dest.pkCoder(), notNullValue());
  }

  @Test
  public void memoizesDestPerDestinationString() {
    TableIdentifier id = v2Table("memoized");
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.Dest first = setup.get(id.toString(), DATA_SCHEMA);
    TableSetup.Dest second = setup.get(id.toString(), DATA_SCHEMA);

    assertThat(second, sameInstance(first));
  }

  /**
   * The memo is keyed by destination string: two destinations resolved through ONE {@link
   * TableSetup} get their own {@link TableSetup.Dest} each; a memo ignoring the destination would
   * silently hand every later destination the first table's Dest, and single-destination tests
   * cannot see it.
   */
  @Test
  public void memoizesEachDestinationSeparately() {
    // Genuinely different tables: different column names, types, and identifier-field counts, so a
    // cross-wired Dest cannot masquerade as the right one.
    org.apache.iceberg.Schema schemaA =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    org.apache.iceberg.Schema schemaB =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "sku", Types.StringType.get()),
            Types.NestedField.required(2, "region", Types.StringType.get()),
            Types.NestedField.optional(3, "qty", Types.LongType.get()));
    TableIdentifier idA = uniqueId("multi_a");
    TableIdentifier idB = uniqueId("multi_b");
    CdcSinkTestUtils.createTable(
        catalog, idA, schemaA, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    CdcSinkTestUtils.createTable(
        catalog, idB, schemaB, ImmutableSet.of(1, 2), 2, PartitionSpec.unpartitioned());

    Schema sourceA = dataSchemaFor(schemaA);
    Schema sourceB = dataSchemaFor(schemaB);
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(sourceA, null, null));

    TableSetup.Dest destA = setup.get(idA.toString(), sourceA);
    TableSetup.Dest destB = setup.get(idB.toString(), sourceB);

    assertThat(destB, not(sameInstance(destA)));
    assertThat(destA.table().name(), containsString(idA.name()));
    assertThat(destB.table().name(), containsString(idB.name()));
    assertThat(destA.equalityFieldIds(), contains(1));
    assertThat(destB.equalityFieldIds(), containsInAnyOrder(1, 2));
    assertThat(destA.pkSchema().getFieldNames(), contains("id"));
    assertThat(destB.pkSchema().getFieldNames(), contains("sku", "region"));
    assertThat(destA.cdcDataSchema().getFieldNames(), contains("id", "name"));
    assertThat(destB.cdcDataSchema().getFieldNames(), contains("sku", "region", "qty"));
    assertArrayEquals(new int[] {0}, destA.pkFieldPositions());
    assertArrayEquals(new int[] {0, 1}, destB.pkFieldPositions());

    // Both entries live in the memo at once: re-getting either returns ITS OWN instance.
    assertThat(setup.get(idA.toString(), sourceA), sameInstance(destA));
    assertThat(setup.get(idB.toString(), sourceB), sameInstance(destB));
  }

  @Test
  public void serializesAndResolvesAfterDeserialization() {
    TableIdentifier id = v2Table("serializable");

    TableSetup roundTripped = SerializableUtils.clone(tableSetup(cfg().build()));

    TableSetup.Dest dest = roundTripped.get(id.toString(), DATA_SCHEMA);
    assertThat(dest.pkSchema().getFieldNames(), contains("id"));
  }

  // -------------------------------------------------------------------------------------------
  // Auto-creation
  // -------------------------------------------------------------------------------------------

  @Test
  public void autoCreatesMissingTable() {
    TableIdentifier id = uniqueId("autocreate");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("id")).build();
    TestDestinations destinations =
        new TestDestinations(DATA_SCHEMA, ImmutableList.of("id"), ImmutableList.of("name"));

    TableSetup.Dest dest = tableSetup(config, destinations).get(id.toString(), DATA_SCHEMA);

    Table table = catalog.loadTable(id);
    assertThat(TableUtil.formatVersion(table), equalTo(2));
    // Identifier fields are the configured equality columns.
    int idFieldId = table.schema().findField("id").fieldId();
    assertThat(table.schema().identifierFieldIds(), contains(idFieldId));
    // Partition spec and sort order from the destination's create config are honored.
    assertThat(table.spec().fields(), hasSize(1));
    PartitionField partitionField = table.spec().fields().get(0);
    assertThat(partitionField.sourceId(), equalTo(idFieldId));
    assertTrue(partitionField.transform().isIdentity());
    assertThat(table.sortOrder().fields(), hasSize(1));
    assertThat(
        table.sortOrder().fields().get(0).sourceId(),
        equalTo(table.schema().findField("name").fieldId()));

    assertThat(dest.equalityFieldIds(), contains(idFieldId));
    assertThat(dest.cdcDataSchema().getFieldNames(), contains("id", "name", "data"));
  }

  /**
   * Pins the created columns' TYPES and required/optional flags: a wrong type or a silently
   * nullable column is invisible to the name-only assertions everywhere else.
   */
  @Test
  public void autoCreatedColumnsCarryInputTypesAndNullability() {
    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("code")
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("amount", Schema.FieldType.DOUBLE)
            .addInt64Field("version")
            .addBooleanField("active")
            .build();
    TableIdentifier id = uniqueId("autocreate_types");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("id", "code")).build();

    TableSetup.Dest dest =
        tableSetup(config, new TestDestinations(inputSchema, null, null))
            .get(id.toString(), inputSchema);

    org.apache.iceberg.Schema created = catalog.loadTable(id).schema();
    assertFieldIs(created, "id", Types.IntegerType.get(), /* required= */ true);
    assertFieldIs(created, "code", Types.StringType.get(), /* required= */ true);
    assertFieldIs(created, "name", Types.StringType.get(), /* required= */ false);
    assertFieldIs(created, "amount", Types.DoubleType.get(), /* required= */ false);
    assertFieldIs(created, "version", Types.LongType.get(), /* required= */ true);
    assertFieldIs(created, "active", Types.BooleanType.get(), /* required= */ true);
    // Both equality columns became identifier fields.
    assertThat(
        created.identifierFieldIds(),
        containsInAnyOrder(created.findField("id").fieldId(), created.findField("code").fieldId()));
    // The sink's own resolved view agrees with the table it just created.
    assertThat(dest.cdcDataSchema(), equalTo(IcebergUtils.icebergSchemaToBeamSchema(created)));
    assertThat(dest.pkSchema().getFieldNames(), contains("id", "code"));
  }

  /** Asserts one created Iceberg column's type and required/optional flag. */
  private static void assertFieldIs(
      org.apache.iceberg.Schema schema, String name, Type type, boolean required) {
    Types.NestedField field = schema.findField(name);
    assertThat("column '" + name + "' is missing", field, notNullValue());
    assertThat("column '" + name + "' type", field.type(), equalTo(type));
    assertThat("column '" + name + "' requiredness", field.isRequired(), equalTo(required));
  }

  /**
   * Create-config table properties reach the created table, and the sink's {@code format-version=2}
   * default applies only when the user did not ask for one (a plain {@code put} would silently
   * downgrade a requested V3 table).
   */
  @Test
  public void autoCreateHonorsTablePropertiesAndDefaultsToFormatVersion2() {
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("id")).build();

    // facet: explicit format-version 3 and a custom property both honored.
    TableIdentifier propsId = uniqueId("autocreate_props");
    TestDestinations destinations =
        new TestDestinations(
            DATA_SCHEMA,
            null,
            null,
            ImmutableMap.of("format-version", "3", "cdc.test.owner", "cdc-team"));
    tableSetup(config, destinations).get(propsId.toString(), DATA_SCHEMA);
    Table table = catalog.loadTable(propsId);
    assertThat(TableUtil.formatVersion(table), equalTo(3));
    assertThat(table.properties().get("cdc.test.owner"), equalTo("cdc-team"));

    // facet: no create-config properties at all still defaults to V2.
    TableIdentifier defaultId = uniqueId("autocreate_default_fv");
    tableSetup(config, new TestDestinations(DATA_SCHEMA, null, null))
        .get(defaultId.toString(), DATA_SCHEMA);
    assertThat(TableUtil.formatVersion(catalog.loadTable(defaultId)), equalTo(2));
  }

  @Test
  public void rejectsAutoCreateWithNullableEqualityColumn() {
    TableIdentifier id = uniqueId("autocreate_nullable");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("name")).build();
    TableSetup setup = tableSetup(config);

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("'name'"));
    assertThat(error.getMessage(), containsString("non-nullable"));
  }

  @Test
  public void rejectsAutoCreateWithoutEqualityColumns() {
    TableIdentifier id = uniqueId("autocreate_no_eq");
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("does not exist"));
    assertThat(error.getMessage(), containsString("equality_columns"));
  }

  // -------------------------------------------------------------------------------------------
  // Validation rejections
  // -------------------------------------------------------------------------------------------

  @Test
  public void rejectsFormatVersion1Table() {
    TableIdentifier id = uniqueId("v1");
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 1, PartitionSpec.unpartitioned());
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString(id.toString()));
    assertThat(error.getMessage(), containsString("append sink"));
  }

  @Test
  public void rejectsNullableEqualityColumn() {
    TableIdentifier id = v2Table("nullable_pk");
    // 'name' exists in the table but is optional, so it cannot define row identity.
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("name")).build();
    TableSetup setup = tableSetup(config);

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("'name'"));
    assertThat(error.getMessage(), containsString("must be required"));
  }

  /** A fresh {@code day(ts)}-partitioned table whose partition source is NOT an equality column. */
  private TableIdentifier nonKeyDayPartitionedTable(String prefix) {
    org.apache.iceberg.Schema schema = timestampSchema();
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(
        catalog,
        id,
        schema,
        ImmutableSet.of(1),
        2,
        PartitionSpec.builderFor(schema).day("ts").build());
    return id;
  }

  /**
   * {@code day(ts)} with PK {@code [id]} resolves at default config: the writer routes each
   * equality delete by its block's opening record, so no key-derived-partition requirement applies.
   */
  @Test
  public void acceptsNonKeyPartitionSourceAtDefaultConfig() {
    TableIdentifier id = nonKeyDayPartitionedTable("nonkey_default");
    Schema sourceSchema = dataSchemaFor(timestampSchema());
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(sourceSchema, null, null));

    TableSetup.Dest dest = setup.get(id.toString(), sourceSchema);

    assertThat(dest.equalityFieldIds(), contains(1));
    assertThat(dest.partitionShardPlan(), nullValue());
  }

  /**
   * The two options that need the partition to be a pure function of the primary key still reject a
   * non-key partition source, each naming itself and the offending column.
   */
  @Test
  public void upsertAndShardCapStillRequireKeyDerivedPartitions() {
    Schema sourceSchema = dataSchemaFor(timestampSchema());

    // facet: upsert (before-images are dropped, so a moved row's old partition is unreachable).
    TableIdentifier upsertId = nonKeyDayPartitionedTable("nonkey_upsert");
    TableSetup upsertSetup =
        tableSetup(cfg().setUpsert(true).build(), new TestDestinations(sourceSchema, null, null));
    TableSetup.TableConfigException upsertError =
        assertThrows(
            TableSetup.TableConfigException.class,
            () -> upsertSetup.get(upsertId.toString(), sourceSchema));
    assertThat(upsertError.getMessage(), containsString("upsert"));
    assertThat(upsertError.getMessage(), containsString("'ts'"));

    // facet: shards_per_partition below num_shards (the shard is derived from the partition
    // tuple, which must therefore follow from the primary key).
    TableIdentifier cappedId = nonKeyDayPartitionedTable("nonkey_capped");
    TableSetup cappedSetup =
        tableSetup(
            cfg().setShardsPerPartition(2).build(), new TestDestinations(sourceSchema, null, null));
    TableSetup.TableConfigException cappedError =
        assertThrows(
            TableSetup.TableConfigException.class,
            () -> cappedSetup.get(cappedId.toString(), sourceSchema));
    assertThat(cappedError.getMessage(), containsString("shards_per_partition"));
    assertThat(cappedError.getMessage(), containsString("'ts'"));
  }

  /**
   * A memoized {@link TableSetup.Dest} is handed back unchanged after a live spec evolution: the
   * write path pins {@code specId()}, so no drift check runs on the default path. Only block
   * sharding re-checks ({@link #blockShardingStillRefusesSpecDrift}).
   */
  @Test
  public void memoHitUnderEvolvedSpecReturnsPinnedDest() {
    TableIdentifier id = v2Table("spec_evolution");
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.Dest dest = setup.get(id.toString(), DATA_SCHEMA);
    int resolvedSpecId = dest.spec().specId();
    assertThat(resolvedSpecId, equalTo(dest.table().spec().specId()));

    // An operator evolves the spec mid-run; the sink's shared Table instance picks it up.
    dest.table().updateSpec().addField(Expressions.bucket("id", 4)).commit();
    dest.table().refresh();
    assertThat(dest.table().spec().specId(), not(equalTo(resolvedSpecId)));

    TableSetup.Dest again = setup.get(id.toString(), DATA_SCHEMA);

    assertThat(again, sameInstance(dest));
    assertThat(again.spec().specId(), equalTo(resolvedSpecId));
  }

  /**
   * With a {@link PartitionShardPlan} present, memo-hit spec drift must still throw naming both
   * spec ids: workers on different specs would split one key across shards, silent duplicates.
   */
  @Test
  public void blockShardingStillRefusesSpecDrift() {
    TableIdentifier id = bucketPartitionedTable("block_spec_drift", "id", 4);
    TableSetup setup = tableSetup(cfg().setShardsPerPartition(2).build());

    TableSetup.Dest dest = setup.get(id.toString(), DATA_SCHEMA);
    assertThat(dest.partitionShardPlan(), notNullValue());
    int resolvedSpecId = dest.spec().specId();

    dest.table().updateSpec().addField(Expressions.bucket("id", 8)).commit();
    dest.table().refresh();
    int evolvedSpecId = dest.table().spec().specId();
    assertThat(evolvedSpecId, not(equalTo(resolvedSpecId)));

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString(id.toString()));
    assertThat(error.getMessage(), containsString("spec id " + resolvedSpecId));
    assertThat(error.getMessage(), containsString("spec id " + evolvedSpecId));
    assertThat(error.getMessage(), containsString("Drain the pipeline"));
  }

  // -------------------------------------------------------------------------------------------
  // Run-spec stamp adoption at resolution
  // -------------------------------------------------------------------------------------------

  /**
   * Commits an empty snapshot carrying the run-spec stamp as the committer writes it. Literal
   * strings on purpose: a contract pin, like the token keys.
   */
  private static void stampRunSpec(Table table, String runId, int specId) {
    table.newAppend().set("beam.cdc.run-spec.test-sink", runId + ":" + specId).commit();
    table.refresh();
  }

  /** Creates a {@code bucket(id, 4)}-partitioned table and resolves it once (warming the cache). */
  private TableSetup.Dest resolvedBucketDest(TableIdentifier id) {
    PartitionSpec spec = PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("id", 4).build();
    CdcSinkTestUtils.createTable(catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, spec);
    return tableSetup(cfg().build()).get(id.toString(), DATA_SCHEMA);
  }

  /**
   * A worker that first resolves a destination after a mid-run spec evolution adopts the spec the
   * committer stamped for this run's runId, not the live current spec, and validates against it.
   */
  @Test
  public void joiningWorkerAdoptsStampedSpec() {
    TableIdentifier id = uniqueId("joining_worker");
    TableSetup.Dest dest = resolvedBucketDest(id);
    int stampedSpecId = dest.spec().specId();
    stampRunSpec(dest.table(), "runId-n", stampedSpecId);

    dest.table().updateSpec().addField(Expressions.bucket("id", 8)).commit();
    dest.table().refresh();
    assertThat(dest.table().spec().specId(), not(equalTo(stampedSpecId)));

    TableSetup joining =
        new TableSetup(
            catalogConfig, cfg().build(), new TestDestinations(DATA_SCHEMA, null, null), "runId-n");

    assertThat(joining.get(id.toString(), DATA_SCHEMA).spec().specId(), equalTo(stampedSpecId));
  }

  /** A stamp from another run's runId is ignored: a fresh run resolves the current spec. */
  @Test
  public void freshRunAdoptsCurrentSpec() {
    TableIdentifier id = uniqueId("fresh_run");
    TableSetup.Dest dest = resolvedBucketDest(id);
    stampRunSpec(dest.table(), "runId-n", dest.spec().specId());

    dest.table().updateSpec().addField(Expressions.bucket("id", 8)).commit();
    dest.table().refresh();
    int currentSpecId = dest.table().spec().specId();

    TableSetup fresh =
        new TableSetup(
            catalogConfig, cfg().build(), new TestDestinations(DATA_SCHEMA, null, null), "runId-m");

    assertThat(fresh.get(id.toString(), DATA_SCHEMA).spec().specId(), equalTo(currentSpecId));
  }

  /** A stamp naming a spec id the table does not have falls back to the current spec, no throw. */
  @Test
  public void stampedSpecMissingFallsBackToCurrent() {
    TableIdentifier id = uniqueId("stamp_missing");
    TableSetup.Dest dest = resolvedBucketDest(id);
    int currentSpecId = dest.spec().specId();
    stampRunSpec(dest.table(), "runId-n", 99);

    TableSetup joining =
        new TableSetup(
            catalogConfig, cfg().build(), new TestDestinations(DATA_SCHEMA, null, null), "runId-n");

    assertThat(joining.get(id.toString(), DATA_SCHEMA).spec().specId(), equalTo(currentSpecId));
  }

  @Test
  public void rejectsEqualityOverrideColumnMissingFromTable() {
    TableIdentifier id = v2Table("missing_override");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("nonexistent")).build();
    TableSetup setup = tableSetup(config);

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("'nonexistent'"));
    assertThat(error.getMessage(), containsString("does not exist"));
  }

  /**
   * An EMPTY {@code equality_columns} override must be rejected at resolution too (not only by
   * {@code CdcWriteConfig#validate}): an empty pk schema encodes every row to the SAME key: one
   * shard takes the table and every equality delete matches every row, with nothing failing.
   */
  @Test
  public void rejectsEmptyEqualityColumnsOverrideAtResolution() {
    TableIdentifier id = v2Table("empty_eq_override");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.<String>of()).build();
    TableSetup setup = tableSetup(config);

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("equality_columns must be non-empty"));
    assertThat(error.getMessage(), containsString(id.toString()));
  }

  @Test
  public void rejectsMissingEqualityColumnsEverywhere() {
    TableIdentifier id = uniqueId("no_identifiers");
    // A V2 table with no identifier fields, and no equality_columns override configured.
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(), 2, PartitionSpec.unpartitioned());
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("identifier"));
    assertThat(error.getMessage(), containsString("equality_columns"));
  }

  @Test
  public void rejectsCdcDataSchemaMismatch() {
    TableIdentifier id = v2Table("mismatch");
    Schema withExtra =
        Schema.builder().addFields(DATA_SCHEMA.getFields()).addStringField("extra").build();
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(withExtra, null, null));

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), withExtra));

    assertThat(error.getMessage(), containsString("unexpected"));
    assertThat(error.getMessage(), containsString("extra"));
  }

  @Test
  public void rejectsDataColumnsInDifferentOrderThanTable() {
    TableIdentifier id = v2Table("reordered");
    // Same column names as the table (id, name, data) but in a different order: the written rows
    // and the shuffle coder are built positionally, so order must match, not just the name set.
    Schema reordered =
        Schema.builder()
            .addNullableField("name", Schema.FieldType.STRING)
            .addInt32Field("id")
            .addNullableField("data", Schema.FieldType.STRING)
            .build();
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(reordered, null, null));

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), reordered));

    assertThat(error.getMessage(), containsString("order"));
    assertThat(error.getMessage(), containsString(id.toString()));
  }

  /** A mismatched column type is rejected naming the column, both types, and the remedy. */
  @Test
  public void rejectsColumnTypeMismatch() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "num", Types.LongType.get()));
    TableIdentifier id = uniqueId("type_mismatch");
    CdcSinkTestUtils.createTable(
        catalog, id, schema, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    // 'num' declared INT32 in the input where the table column is a long.
    Schema mismatched =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("num", Schema.FieldType.INT32)
            .build();
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(mismatched, null, null));

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), mismatched));

    assertThat(error.getMessage(), containsString("'num'"));
    assertThat(error.getMessage(), containsString("INT32"));
    assertThat(error.getMessage(), containsString("INT64"));
    assertThat(error.getMessage(), containsString("Align the input schema with the table"));
  }

  /** A nullable-declared input column against a required table column is rejected. */
  @Test
  public void rejectsNullableInputColumnForRequiredTableColumn() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()));
    TableIdentifier id = uniqueId("nullable_input");
    CdcSinkTestUtils.createTable(
        catalog, id, schema, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    Schema nullableName =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("name", Schema.FieldType.STRING)
            .build();
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(nullableName, null, null));

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), nullableName));

    assertThat(error.getMessage(), containsString("'name'"));
    assertThat(error.getMessage(), containsString("nullable in the input"));
    assertThat(error.getMessage(), containsString("required in the table"));
  }

  /** Exactly matching types resolve, including a non-null input column on an OPTIONAL one. */
  @Test
  public void acceptsMatchingTypesAndNonNullInputForOptionalTableColumn() {
    TableIdentifier id = v2Table("types_ok");
    // 'name' non-null in the input against the table's optional column: the safe direction.
    Schema nonNullName =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("name")
            .addNullableField("data", Schema.FieldType.STRING)
            .build();
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(nonNullName, null, null));

    TableSetup.Dest dest = setup.get(id.toString(), nonNullName);

    assertThat(
        dest.cdcDataSchema(), equalTo(IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA)));
  }

  /**
   * A column type with no Beam conversion fails destination resolution rather than silently writing
   * null in every record. Pins the natural failure; the sink has no dedicated check.
   */
  @Test
  public void unconvertibleColumnTypeFailsResolution() {
    org.apache.iceberg.Schema withTimestampNano =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "ts_ns", Types.TimestampNanoType.withoutZone()));
    TableIdentifier id = uniqueId("ts_nano");
    // timestamp_ns is a format-version 3 type.
    CdcSinkTestUtils.createTable(
        catalog, id, withTimestampNano, ImmutableSet.of(1), 3, PartitionSpec.unpartitioned());
    TableSetup setup = tableSetup(cfg().build());

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString(id.toString()));
  }

  @Test
  public void rejectsNestedEqualityOverrideColumn() {
    TableIdentifier id = v2Table("nested_eq");
    CdcWriteConfig config = cfg().setEqualityColumns(ImmutableList.of("user.id")).build();
    TableSetup setup = tableSetup(config);

    TableSetup.TableConfigException error =
        assertThrows(
            TableSetup.TableConfigException.class, () -> setup.get(id.toString(), DATA_SCHEMA));

    assertThat(error.getMessage(), containsString("top-level"));
    assertThat(error.getMessage(), containsString("'user.id'"));
  }

  // -------------------------------------------------------------------------------------------
  // Partition transforms that must be ACCEPTED (all transforms are legal for the CDC sink)
  // -------------------------------------------------------------------------------------------

  private static org.apache.iceberg.Schema timestampSchema() {
    return new org.apache.iceberg.Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "ts", Types.TimestampType.withZone()));
  }

  private void assertPartitionAccepted(
      org.apache.iceberg.Schema schema, PartitionSpec spec, String prefix) {
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(catalog, id, schema, ImmutableSet.of(1, 2), 2, spec);
    Schema sourceSchema = dataSchemaFor(schema);
    TableSetup setup = tableSetup(cfg().build(), new TestDestinations(sourceSchema, null, null));

    TableSetup.Dest dest = setup.get(id.toString(), sourceSchema);

    assertThat(dest, notNullValue());
    assertThat(dest.equalityFieldIds(), containsInAnyOrder(1, 2));
  }

  @Test
  public void acceptsDayHourAndIdentityDatePartitions() {
    org.apache.iceberg.Schema tsSchema = timestampSchema();
    assertPartitionAccepted(
        tsSchema, PartitionSpec.builderFor(tsSchema).day("ts").build(), "day_ts");
    assertPartitionAccepted(
        tsSchema, PartitionSpec.builderFor(tsSchema).hour("ts").build(), "hour_ts");
    org.apache.iceberg.Schema dateSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "d", Types.DateType.get()));
    assertPartitionAccepted(
        dateSchema, PartitionSpec.builderFor(dateSchema).identity("d").build(), "identity_date");
  }

  // -------------------------------------------------------------------------------------------
  // shardFor
  // -------------------------------------------------------------------------------------------

  /** Pins the exact hash function (murmur3_32_fixed + floorMod) against accidental change. */
  @Test
  public void shardForPinsMurmur3FixedFloorMod() {
    assertThat(TableSetup.shardFor(new byte[] {0, 0, 0, 1}, 8), equalTo(4));
    // Hashes to a negative int: floorMod maps it to 7, while abs(hash % n) would give 1. Freezing
    // this prevents a silent resharding of half the keyspace.
    assertThat(TableSetup.shardFor(new byte[] {0, 0, 0, 2}, 8), equalTo(7));
  }

  /**
   * Deterministic, in range, spreading distinct keys, and covering EVERY shard of a
   * non-power-of-two count, which pins {@code floorMod} against the {@code hash & (n - 1)}
   * "optimization" (at 10 shards the mask can only produce 0, 1, 8, 9).
   */
  @Test
  public void shardForIsDeterministicSpreadsKeysAndCoversNonPowerOfTwoCounts() {
    // facet: determinism and spread at 8 shards.
    int numShards = 8;
    Set<Integer> shards = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      byte[] pk = ("pk-" + i).getBytes(StandardCharsets.UTF_8);
      int shard = TableSetup.shardFor(pk, numShards);
      assertThat(TableSetup.shardFor(pk, numShards), equalTo(shard));
      assertThat(shard, greaterThanOrEqualTo(0));
      assertThat(shard, lessThan(numShards));
      shards.add(shard);
    }
    assertThat(shards.size(), greaterThan(1));

    // facet: full coverage at the non-power-of-two 10.
    Set<Integer> tenShards = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      byte[] pk = ("pk-" + i).getBytes(StandardCharsets.UTF_8);
      int shard = TableSetup.shardFor(pk, 10);
      assertThat(shard, greaterThanOrEqualTo(0));
      assertThat(shard, lessThan(10));
      tenShards.add(shard);
    }
    assertThat(tenShards, containsInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  // -------------------------------------------------------------------------------------------
  // shardForHash (the partition-tuple block base)
  // -------------------------------------------------------------------------------------------

  /**
   * Pins the partition-tuple shard reduction as {@link #shardForPinsMurmur3FixedFloorMod} pins the
   * primary-key one: a silent change here reshards every partitioned destination.
   */
  @Test
  public void shardForHashPinsMurmur3FixedAvalanche() {
    assertThat(TableSetup.shardForHash(0, 8), equalTo(6));
    assertThat(TableSetup.shardForHash(1, 8), equalTo(2));
    // Hashes to a negative int, so this also pins floorMod over abs(hash % n) (which gives 4).
    assertThat(TableSetup.shardForHash(-1, 8), equalTo(0));
  }

  /**
   * The avalanche is the point: the tuple hash of a single INTEGER partition field IS the value, so
   * without the mix these 32 values striding by 8 would collapse onto one of 8 shards.
   */
  @Test
  public void shardForHashSpreadsAStridedValueSpace() {
    Set<Integer> shards = new HashSet<>();
    for (int i = 0; i < 32; i++) {
      int shard = TableSetup.shardForHash(i * 8, 8);
      assertThat(shard, greaterThanOrEqualTo(0));
      assertThat(shard, lessThan(8));
      shards.add(shard);
    }
    assertThat(shards, containsInAnyOrder(0, 1, 2, 3, 4, 5, 6, 7));
  }

  @Test
  public void shardForHashIsDeterministicAndInRange() {
    for (int i = -50; i < 50; i++) {
      int shard = TableSetup.shardForHash(i, 10);
      assertThat(TableSetup.shardForHash(i, 10), equalTo(shard));
      assertThat(shard, greaterThanOrEqualTo(0));
      assertThat(shard, lessThan(10));
    }
  }

  // -------------------------------------------------------------------------------------------
  // partitionShardPlan gate: built iff shards_per_partition < num_shards AND spec is partitioned
  // -------------------------------------------------------------------------------------------

  /** Resolves a fresh {@code day(ts)}-partitioned destination under {@code config}. */
  private TableSetup.Dest partitionedDest(CdcWriteConfig config, String prefix) {
    org.apache.iceberg.Schema schema = timestampSchema();
    TableIdentifier id = uniqueId(prefix);
    CdcSinkTestUtils.createTable(
        catalog,
        id,
        schema,
        ImmutableSet.of(1, 2),
        2,
        PartitionSpec.builderFor(schema).day("ts").build());
    Schema sourceSchema = dataSchemaFor(schema);
    TableSetup setup = tableSetup(config, new TestDestinations(sourceSchema, null, null));
    return setup.get(id.toString(), sourceSchema);
  }

  /**
   * The gate matrix: a plan is built iff the cap is below {@code num_shards} AND the spec is
   * partitioned; the default (equal) and an unpartitioned table both bypass it.
   */
  @Test
  public void partitionShardPlanBuiltOnlyWhenCappedAndPartitioned() {
    // facet: cap below num_shards on a partitioned spec => plan.
    assertThat(
        partitionedDest(cfg().setShardsPerPartition(4).build(), "gate_on").partitionShardPlan(),
        notNullValue());

    // facet: cap == num_shards (today's default exactly) => no plan.
    assertThat(
        partitionedDest(cfg().build(), "gate_off_default").partitionShardPlan(), nullValue());

    // facet: unpartitioned table ignores the cap => no plan.
    TableIdentifier id = v2Table("gate_unpartitioned");
    TableSetup setup = tableSetup(cfg().setShardsPerPartition(1).build());
    assertThat(setup.get(id.toString(), DATA_SCHEMA).partitionShardPlan(), nullValue());
  }

  // -------------------------------------------------------------------------------------------
  // Test DynamicDestinations
  // -------------------------------------------------------------------------------------------

  /**
   * A single-table {@link DynamicDestinations} for tests, with an optional create config built from
   * partition and sort field lists plus table properties (mirroring {@code
   * OneTableDynamicDestinations}).
   */
  private static final class TestDestinations implements DynamicDestinations {

    private final Schema dataSchema;
    private final @Nullable List<String> partitionFields;
    private final @Nullable List<String> sortFields;
    private final @Nullable Map<String, String> tableProperties;

    TestDestinations(
        Schema dataSchema,
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields) {
      this(dataSchema, partitionFields, sortFields, null);
    }

    TestDestinations(
        Schema dataSchema,
        @Nullable List<String> partitionFields,
        @Nullable List<String> sortFields,
        @Nullable Map<String, String> tableProperties) {
      this.dataSchema = dataSchema;
      this.partitionFields = partitionFields;
      this.sortFields = sortFields;
      this.tableProperties = tableProperties;
    }

    @Override
    public Schema getDataSchema() {
      return dataSchema;
    }

    @Override
    public Row getData(Row element) {
      return element;
    }

    @Override
    public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
      throw new UnsupportedOperationException("not used by TableSetup");
    }

    @Override
    public IcebergDestination instantiateDestination(String destination) {
      @Nullable IcebergTableCreateConfig createConfig = null;
      if (partitionFields != null || sortFields != null || tableProperties != null) {
        createConfig =
            IcebergTableCreateConfig.builder()
                .setSchema(dataSchema)
                .setPartitionFields(partitionFields)
                .setSortFields(sortFields)
                .setTableProperties(tableProperties)
                .build();
      }
      return IcebergDestination.builder()
          .setTableIdentifier(IcebergUtils.parseTableIdentifier(destination))
          .setFileFormat(FileFormat.PARQUET)
          .setTableCreateConfig(createConfig)
          .build();
    }
  }
}
