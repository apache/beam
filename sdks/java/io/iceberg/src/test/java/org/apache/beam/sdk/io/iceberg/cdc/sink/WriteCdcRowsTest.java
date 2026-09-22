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
import static org.apache.beam.sdk.values.ValueKind.DELETE;
import static org.apache.beam.sdk.values.ValueKind.INSERT;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_AFTER;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_BEFORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.io.iceberg.IcebergWriteResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link WriteCdcRows}, the top-level CDC sink {@code PTransform} exposed via {@link
 * IcebergIO#writeCdcRows}. End-to-end tests drive a real {@link HadoopCatalog} V2 table on the
 * DirectRunner; each test uses a unique table identifier so the process-wide TableCache never sees
 * a repeat.
 */
@RunWith(JUnit4.class)
public class WriteCdcRowsTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();
  @Rule public transient ExpectedLogs expectedLogs = ExpectedLogs.none(WriteCdcRows.class);

  /** Canonical test table schema. */
  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final Schema DATA_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("data", Schema.FieldType.STRING)
          .build();

  /** Input schema = data schema + a sequence-number column named {@code seq}. */
  private static final Schema INPUT_SCHEMA =
      Schema.builder().addFields(DATA_SCHEMA.getFields()).addInt64Field("seq").build();

  /** The default sequence-number column name (see {@code CdcWriteConfig}). */
  private static final String DEFAULT_SEQ_COL = "_commit_snapshot_sequence_number";

  /** Replayed dead letters are re-stamped here so the replay lands in an open window. */
  private static final Instant REPLAY_EVENT_TIME = new Instant(0).plus(Duration.standardMinutes(2));

  private Catalog catalog;

  private void setUpCatalog() {
    catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
  }

  private IcebergCatalogConfig catalogConfig() {
    return CdcSinkTestUtils.catalogConfig(tmp.getRoot());
  }

  /** A catalog config pointing at an unreachable warehouse (for no-catalog-at-expand tests). */
  private static IcebergCatalogConfig bogusCatalogConfig() {
    return IcebergCatalogConfig.builder()
        .setCatalogProperties(
            ImmutableMap.of(
                "type", "hadoop", "warehouse", "file:/nonexistent-cdc-" + System.nanoTime()))
        .build();
  }

  /** Creates a fresh unpartitioned V2 table (PK = {@code id}) with a unique identifier. */
  private TableIdentifier v2Table() {
    TableIdentifier id = TableIdentifier.of("db", "t" + System.nanoTime());
    createV2Table(id);
    return id;
  }

  /** Creates a fresh unpartitioned V2 table with the given identifier (PK = {@code id}). */
  private void createV2Table(TableIdentifier id) {
    CdcSinkTestUtils.createTable(
        catalog, id, ICEBERG_SCHEMA, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
  }

  /** Builds an input data+seq {@link Row}. */
  private static Row row(int id, String name, String data, long seq) {
    return Row.withSchema(INPUT_SCHEMA).addValues(id, name, data, seq).build();
  }

  /** Builds an input data-only {@link Row} (no {@code seq} column). */
  private static Row rowNoSeq(int id, String name, String data) {
    return Row.withSchema(DATA_SCHEMA).addValues(id, name, data).build();
  }

  /** Reads all live rows of {@code table} as sorted {@code id:name:data} strings. */
  private static List<String> readRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(r -> r.getField("id") + ":" + r.getField("name") + ":" + r.getField("data"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  /** A bounded CDC input of the given kind-tagged rows over {@code schema}. */
  @SafeVarargs
  private final PCollection<Row> boundedInput(Schema schema, KV<ValueKind, Row>... rows) {
    return CdcSinkTestUtils.withKinds(p.apply(Create.of(ImmutableList.copyOf(rows))))
        .setRowSchema(schema);
  }

  /** An unbounded single-INSERT CDC input over {@link #INPUT_SCHEMA}. */
  private PCollection<Row> unboundedInput() {
    TestStream<Row> stream =
        TestStream.create(RowCoder.of(INPUT_SCHEMA))
            .addElements(row(1, "a", "x", 1L))
            .advanceWatermarkToInfinity();
    PCollection<KV<ValueKind, Row>> tagged =
        p.apply(stream)
            .setRowSchema(INPUT_SCHEMA)
            .apply(
                MapElements.into(
                        TypeDescriptors.kvs(
                            TypeDescriptor.of(ValueKind.class), TypeDescriptors.rows()))
                    .via(r -> KV.of(INSERT, r)))
            .setCoder(KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)));
    return CdcSinkTestUtils.withKinds(tagged).setRowSchema(INPUT_SCHEMA);
  }

  /**
   * The {@code beam.cdc.committed-through-ms.<sinkId>} token of each of {@code table}'s snapshots,
   * in snapshot order.
   */
  private static List<Long> committedThroughTokens(Table table, String sinkId) {
    table.refresh();
    List<Long> tokens = new ArrayList<>();
    for (Snapshot s : table.snapshots()) {
      tokens.add(Long.parseLong(s.summary().get("beam.cdc.committed-through-ms." + sinkId)));
    }
    return tokens;
  }

  /** Sums the named {@link CommitDeltas} counter committed by the pipeline. */
  private static long committerCounter(PipelineResult result, String name) {
    long total = 0;
    for (MetricResult<Long> c :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(CommitDeltas.class, name))
                    .build())
            .getCounters()) {
      total += c.getCommitted();
    }
    return total;
  }

  // ---------------------------------------------------------------------------------------------
  // 1. Construction-time validation matrix
  // ---------------------------------------------------------------------------------------------

  /** Exactly one destination: none at all and both kinds at once are each rejected. */
  @Test
  public void constructionRequiresExactlyOneDestination() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "both_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));

    // facet: no destination.
    IllegalArgumentException none =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "NoDestination",
                    IcebergIO.writeCdcRows(bogusCatalogConfig()).withSequenceNumberColumn("seq")));
    assertThat(none.getMessage(), containsString("exactly one"));

    // facet: both destination kinds.
    IllegalArgumentException both =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "BothDestinations",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .to(
                            CdcSinkTestUtils.templatedDestinations(
                                "db.{name}", INPUT_SCHEMA, "seq"))
                        .withSequenceNumberColumn("seq")));
    assertThat(both.getMessage(), containsString("exactly one"));
  }

  /** An unbounded input requires a triggering frequency, and it must be positive. */
  @Test
  public void unboundedInputRequiresPositiveTriggeringFrequency() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "stream_tf_" + System.nanoTime());
    PCollection<Row> in = unboundedInput();

    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "NoFrequency",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")));
    assertThat(missing.getMessage(), containsString("withTriggeringFrequency"));
    IllegalArgumentException zero =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ZeroFrequency",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withTriggeringFrequency(Duration.ZERO)));
    assertThat(zero.getMessage(), containsString("must be positive"));
    IllegalArgumentException negative =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "NegativeFrequency",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withTriggeringFrequency(Duration.standardSeconds(-30))));
    assertThat(negative.getMessage(), containsString("must be positive"));
  }

  /**
   * Dead-letter metadata is nested under {@code record}, so data columns named like it ({@code
   * _cdc_change_type}, {@code change_type}) are ordinary columns and write end-to-end.
   */
  @Test
  public void deadLetterMetadataNamedDataColumnsWriteFine() {
    setUpCatalog();
    TableIdentifier id = TableIdentifier.of("db", "dlname_" + System.nanoTime());
    CdcSinkTestUtils.createTable(
        catalog,
        id,
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "_cdc_change_type", Types.StringType.get()),
            Types.NestedField.optional(3, "change_type", Types.StringType.get())),
        ImmutableSet.of(1),
        2,
        PartitionSpec.unpartitioned());

    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("_cdc_change_type", Schema.FieldType.STRING)
            .addNullableField("change_type", Schema.FieldType.STRING)
            .addInt64Field("seq")
            .build();
    Row r = Row.withSchema(inputSchema).addValues(1, "c", "u", 1L).build();
    boundedInput(inputSchema, KV.of(INSERT, r))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    Table t = catalog.loadTable(id);
    Record written =
        Iterables.getOnlyElement(ImmutableList.copyOf(IcebergGenerics.read(t).build()));
    assertThat(written.getField("id"), equalTo(1));
    assertThat(written.getField("_cdc_change_type"), equalTo("c"));
    assertThat(written.getField("change_type"), equalTo("u"));
  }

  /** The sequence-number column must exist and be a non-nullable INT64. */
  @Test
  public void constructionRejectsInvalidSequenceColumnConfigs() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "badseq_" + System.nanoTime());

    // facet: missing (the default name is absent from the input schema).
    PCollection<Row> noSeq = boundedInput(DATA_SCHEMA, KV.of(INSERT, rowNoSeq(1, "a", "x")));
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> noSeq.apply("MissingSeq", IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id)));
    assertThat(missing.getMessage(), containsString(DEFAULT_SEQ_COL));
    assertThat(missing.getMessage(), containsString("sequence-number column"));

    // facet: present but STRING.
    Schema stringSeq =
        Schema.builder().addFields(DATA_SCHEMA.getFields()).addStringField("seq").build();
    PCollection<Row> badSeq =
        boundedInput(
            stringSeq,
            KV.of(
                INSERT, Row.withSchema(stringSeq).addValues(1, "a", "x", "not-a-number").build()));
    IllegalArgumentException nonInt64 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                badSeq.apply(
                    "StringSeq",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")));
    assertThat(nonInt64.getMessage(), containsString("sequence-number column 'seq'"));
    assertThat(nonInt64.getMessage(), containsString("must be INT64"));
    assertThat(nonInt64.getMessage(), containsString("STRING"));

    // facet: INT64 but declared nullable.
    Schema nullableSeq =
        Schema.builder()
            .addFields(DATA_SCHEMA.getFields())
            .addNullableField("seq", Schema.FieldType.INT64)
            .build();
    PCollection<Row> nullableIn =
        boundedInput(
            nullableSeq,
            KV.of(INSERT, Row.withSchema(nullableSeq).addValues(1, "a", "x", 1L).build()));
    IllegalArgumentException nullable =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                nullableIn.apply(
                    "NullableSeq",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")));
    assertThat(nullable.getMessage(), containsString("sequence-number column 'seq'"));
    assertThat(nullable.getMessage(), containsString("must be non-nullable"));
  }

  /**
   * The change-type column must exist (it is the cross-language default path, so a typo otherwise
   * fails every record at runtime), be a non-nullable STRING, and be distinct from the
   * sequence-number column.
   */
  @Test
  public void constructionRejectsInvalidChangeTypeColumnConfigs() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "badct_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));

    // facet: names a column the input does not have.
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "MissingChangeType",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withChangeTypeColumn("op_typo")));
    assertThat(missing.getMessage(), containsString("change-type column 'op_typo'"));
    assertThat(missing.getMessage(), containsString("change_type_column"));

    // facet: present but INT32.
    Schema intOp = Schema.builder().addFields(INPUT_SCHEMA.getFields()).addInt32Field("op").build();
    PCollection<Row> badOp =
        boundedInput(
            intOp, KV.of(INSERT, Row.withSchema(intOp).addValues(1, "a", "x", 1L, 7).build()));
    IllegalArgumentException nonString =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                badOp.apply(
                    "IntChangeType",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withChangeTypeColumn("op")));
    assertThat(nonString.getMessage(), containsString("change-type column 'op'"));
    assertThat(nonString.getMessage(), containsString("must be STRING"));
    assertThat(nonString.getMessage(), containsString("INT32"));

    // facet: STRING but declared nullable.
    Schema nullableOp =
        Schema.builder()
            .addFields(INPUT_SCHEMA.getFields())
            .addNullableField("op", Schema.FieldType.STRING)
            .build();
    PCollection<Row> nullableIn =
        boundedInput(
            nullableOp,
            KV.of(INSERT, Row.withSchema(nullableOp).addValues(1, "a", "x", 1L, "I").build()));
    IllegalArgumentException nullable =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                nullableIn.apply(
                    "NullableChangeType",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withChangeTypeColumn("op")));
    assertThat(nullable.getMessage(), containsString("change-type column 'op'"));
    assertThat(nullable.getMessage(), containsString("must be non-nullable"));

    // facet: collides with the sequence-number column.
    IllegalArgumentException collides =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ChangeTypeIsSeq",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withChangeTypeColumn("seq")));
    assertThat(collides.getMessage(), containsString("must be distinct"));
  }

  /**
   * The shard-config bounds: {@code num_shards >= 1}, {@code 1 <= shards_per_partition <=
   * num_shards}, with the boundary {@code shards_per_partition == num_shards} (the uncapped
   * default) accepted.
   */
  @Test
  public void constructionValidatesShardConfigBounds() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "shards_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));

    // facet: num_shards = 0.
    IllegalArgumentException zeroShards =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ZeroShards",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withNumShards(0)));
    assertThat(zeroShards.getMessage(), containsString("num_shards must be >= 1"));

    // facet: shards_per_partition = 0, which names the option and both bounds (num_shards
    // defaults to 16 here).
    IllegalArgumentException sppZero =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ZeroShardsPerPartition",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withShardsPerPartition(0)));
    assertThat(
        sppZero.getMessage(),
        containsString("shards_per_partition must be between 1 and num_shards"));
    assertThat(sppZero.getMessage(), containsString("(16)"));
    assertThat(sppZero.getMessage(), containsString("got 0"));

    // facet: shards_per_partition above num_shards.
    IllegalArgumentException sppAbove =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ShardsPerPartitionAboveNumShards",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withNumShards(16)
                        .withShardsPerPartition(32)));
    assertThat(sppAbove.getMessage(), containsString("shards_per_partition"));
    assertThat(sppAbove.getMessage(), containsString("32"));
    assertThat(sppAbove.getMessage(), containsString("16"));

    // facet: shards_per_partition == num_shards must build (never run).
    in.apply(
        "ShardsPerPartitionEqualNumShards",
        IcebergIO.writeCdcRows(bogusCatalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withNumShards(16)
            .withShardsPerPartition(16));
  }

  /**
   * A reserved {@code beam.cdc.} snapshot-property key and an explicitly empty equality-column list
   * are each rejected at construction.
   */
  @Test
  public void constructionRejectsReservedSnapshotPropertiesAndEmptyEqualityColumns() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "props_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));

    IllegalArgumentException reserved =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "ReservedProperty",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withSnapshotProperties(ImmutableMap.of("beam.cdc.foo", "x"))));
    assertThat(reserved.getMessage(), containsString("beam.cdc."));

    IllegalArgumentException emptyEquality =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                in.apply(
                    "EmptyEqualityColumns",
                    IcebergIO.writeCdcRows(bogusCatalogConfig())
                        .to(id)
                        .withSequenceNumberColumn("seq")
                        .withEqualityColumns(ImmutableList.of())));
    assertThat(emptyEquality.getMessage(), containsString("non-empty or unset"));
  }

  /** {@code withSorterMemoryMB} accepts a positive value and rejects a non-positive one eagerly. */
  @Test
  public void sorterMemoryMbValidatedAtConstruction() {
    TableIdentifier id = TableIdentifier.of("db", "sorter_" + System.nanoTime());
    IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id).withSorterMemoryMB(50);
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id).withSorterMemoryMB(0));
    assertThat(ex.getMessage(), containsString("sorter_memory_mb"));
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Launcher touches no catalog
  // ---------------------------------------------------------------------------------------------

  /**
   * Neither the single-table nor the dynamic-destination branch may touch the catalog at {@code
   * expand()}: building against an unreachable warehouse (and a nonexistent table) must succeed;
   * schema resolution is deferred to the workers. The pipeline is never run.
   */
  @Test
  public void expandTouchesNoCatalog() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "no_such_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));
    in.apply(
        "SingleTable",
        IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id).withSequenceNumberColumn("seq"));
    in.apply(
        "DynamicDestinations",
        IcebergIO.writeCdcRows(bogusCatalogConfig())
            .to(CdcSinkTestUtils.templatedDestinations("db.{name}", INPUT_SCHEMA, "seq"))
            .withSequenceNumberColumn("seq"));
  }

  // ---------------------------------------------------------------------------------------------
  // 3. Batch end-to-end
  // ---------------------------------------------------------------------------------------------

  @Test
  public void batchCdcAppliesInsertsUpdatesDeletes() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    PCollection<Row> in =
        boundedInput(
            INPUT_SCHEMA,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(UPDATE_BEFORE, row(1, "a", "x", 2L)),
            KV.of(UPDATE_AFTER, row(1, "a", "z", 2L)),
            KV.of(DELETE, row(2, "b", "y", 3L)));
    IcebergWriteResult result =
        in.apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));

    PAssert.that(result.getSnapshots())
        .satisfies(
            snapshots -> {
              assertFalse(Iterables.isEmpty(snapshots));
              return null;
            });
    p.run().waitUntilFinish();

    // id=1 updated to (a,z); id=2 deleted.
    assertThat(readRows(t), contains("1:a:z"));
  }

  /**
   * A bounded input carrying a NON-default upstream trigger must still commit every row exactly
   * once: {@code CommitWindows}' bounded branch has to pin its trigger explicitly, or the inherited
   * trigger fires several panes per key and every pane past the first is skipped as a same-end
   * twin: rows in neither the table nor the dead letters. {@code withNumShards(1)} funnels all rows
   * through one key so a pane split cannot hide in the sharding.
   */
  @Test
  public void boundedInputWithUpstreamTriggerCommitsAllRowsExactlyOnce() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    PCollection<Row> in =
        boundedInput(
                INPUT_SCHEMA,
                KV.of(INSERT, row(1, "a", "x1", 1L)),
                KV.of(INSERT, row(2, "b", "x2", 1L)),
                KV.of(INSERT, row(3, "c", "x3", 1L)),
                KV.of(INSERT, row(4, "d", "x4", 1L)),
                KV.of(INSERT, row(5, "e", "x5", 1L)),
                KV.of(INSERT, row(6, "f", "x6", 1L)),
                KV.of(INSERT, row(7, "g", "x7", 1L)),
                KV.of(INSERT, row(8, "h", "x8", 1L)))
            .apply(
                "NonDefaultUpstreamTrigger",
                Window.<Row>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .discardingFiredPanes());

    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withNumShards(1));
    PipelineResult result = p.run();
    result.waitUntilFinish();

    // readRows lists ALL live rows: a lost pane or a double-commit both fail this.
    assertThat(
        readRows(t),
        contains("1:a:x1", "2:b:x2", "3:c:x3", "4:d:x4", "5:e:x5", "6:f:x6", "7:g:x7", "8:h:x8"));
    assertThat(committerCounter(result, "alreadyCommittedWindowsSkipped"), equalTo(0L));
  }

  // ---------------------------------------------------------------------------------------------
  // 4. Streaming two-window end-to-end
  // ---------------------------------------------------------------------------------------------

  /** Two event-time windows commit as two snapshots with ascending tokens; final state correct. */
  @Test
  public void streamingTwoWindowsCommitsPerWindow() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);
    String sinkId = "sink-" + System.nanoTime();
    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(1, "a", "x", 1L)), base.plus(Duration.standardSeconds(1))),
                TimestampedValue.of(
                    KV.of(INSERT, row(2, "b", "y", 1L)), base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(70))) // window 0 commits
            .addElements(
                TimestampedValue.of(
                    KV.of(UPDATE_BEFORE, row(1, "a", "x", 2L)),
                    base.plus(Duration.standardSeconds(61))),
                TimestampedValue.of(
                    KV.of(UPDATE_AFTER, row(1, "a2", "z", 2L)),
                    base.plus(Duration.standardSeconds(61))),
                TimestampedValue.of(
                    KV.of(DELETE, row(2, "b", "y", 2L)), base.plus(Duration.standardSeconds(62))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(130))) // window 1 commits
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withTriggeringFrequency(window)
            .withSinkId(sinkId));
    p.run().waitUntilFinish();

    List<Long> tokens = committedThroughTokens(t, sinkId);
    assertThat(tokens, hasSize(2));
    assertThat(tokens.get(0), lessThan(tokens.get(1)));
    assertThat(readRows(t), contains("1:a2:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // 5. Dynamic destinations (template) + control-column strip
  // ---------------------------------------------------------------------------------------------

  /**
   * A {@code db.{dest}} template routes rows to two tables; the default sequence-number column is
   * stripped from the written rows, so neither table needs (or gains) it.
   */
  @Test
  public void dynamicDestinationsRouteAndStripSequenceColumn() {
    setUpCatalog();
    long suffix = System.nanoTime();
    String tableA = "tmpl_a" + suffix;
    String tableB = "tmpl_b" + suffix;
    // Tables whose columns are (id, dest): the routing column is also a data column.
    CdcSinkTestUtils.createDestTables(catalog, tableA, tableB);

    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("dest", Schema.FieldType.STRING)
            .addInt64Field(DEFAULT_SEQ_COL)
            .build();
    Row rowA = Row.withSchema(inputSchema).addValues(1, tableA, 1L).build();
    Row rowB = Row.withSchema(inputSchema).addValues(2, tableB, 1L).build();

    PCollection<Row> in = boundedInput(inputSchema, KV.of(INSERT, rowA), KV.of(INSERT, rowB));
    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(CdcSinkTestUtils.templatedDestinations("db.{dest}", inputSchema, DEFAULT_SEQ_COL)));
    p.run().waitUntilFinish();

    Table a = catalog.loadTable(TableIdentifier.of("db", tableA));
    Table b = catalog.loadTable(TableIdentifier.of("db", tableB));
    assertThat(
        ImmutableList.copyOf(IcebergGenerics.read(a).build()).stream()
            .map(r -> r.getField("id") + ":" + r.getField("dest"))
            .collect(ImmutableList.toImmutableList()),
        contains("1:" + tableA));
    assertThat(
        ImmutableList.copyOf(IcebergGenerics.read(b).build()).stream()
            .map(r -> r.getField("id") + ":" + r.getField("dest"))
            .collect(ImmutableList.toImmutableList()),
        contains("2:" + tableB));
    a.refresh();
    b.refresh();
    assertNull(a.schema().findField(DEFAULT_SEQ_COL));
    assertNull(b.schema().findField(DEFAULT_SEQ_COL));
  }

  /**
   * Streaming + dynamic destinations, the combination the suite otherwise never runs. A receives
   * data in windows 0 and 2, B in 1 and 2, so each destination's committer state and commit timer
   * must advance on its own schedule; state shared across destinations would skip a window as
   * "already committed" for the OTHER destination's end and silently lose its rows.
   */
  @Test
  public void streamingDynamicDestinationsCommitPerDestinationWindows() {
    setUpCatalog();
    long suffix = System.nanoTime();
    String tableA = "stream_a" + suffix;
    String tableB = "stream_b" + suffix;
    CdcSinkTestUtils.createDestTables(catalog, tableA, tableB);

    Schema inputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addNullableField("dest", Schema.FieldType.STRING)
            .addInt64Field(DEFAULT_SEQ_COL)
            .build();
    String sinkId = "sink-" + suffix;
    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(inputSchema)))
            .advanceWatermarkTo(base)
            // Window 0: destination A only.
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, destRow(inputSchema, 1, tableA, 1L)),
                    base.plus(Duration.standardSeconds(1))),
                TimestampedValue.of(
                    KV.of(INSERT, destRow(inputSchema, 5, tableA, 1L)),
                    base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(70)))
            // Window 1: destination B only.
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, destRow(inputSchema, 2, tableB, 2L)),
                    base.plus(Duration.standardSeconds(61))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(130)))
            // Window 2: both destinations. A's DELETE removes a key committed in window 0, so it
            // also proves A's cross-window equality delete landed in the right order.
            .addElements(
                TimestampedValue.of(
                    KV.of(DELETE, destRow(inputSchema, 1, tableA, 3L)),
                    base.plus(Duration.standardSeconds(121))),
                TimestampedValue.of(
                    KV.of(INSERT, destRow(inputSchema, 3, tableB, 3L)),
                    base.plus(Duration.standardSeconds(122))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(190)))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(inputSchema);
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(
                    CdcSinkTestUtils.templatedDestinations(
                        "db.{dest}", inputSchema, DEFAULT_SEQ_COL))
                .withTriggeringFrequency(window)
                .withSinkId(sinkId));
    PAssert.that(result.getDeadLetterRows()).empty();
    p.run().waitUntilFinish();

    Table a = catalog.loadTable(TableIdentifier.of("db", tableA));
    Table b = catalog.loadTable(TableIdentifier.of("db", tableB));

    // Each destination committed exactly its OWN windows, ascending: A's first is window 0, B's
    // is window 1 (not in lockstep), and both saw window 2.
    List<Long> tokensA = committedThroughTokens(a, sinkId);
    List<Long> tokensB = committedThroughTokens(b, sinkId);
    assertThat(tokensA, hasSize(2));
    assertThat(tokensB, hasSize(2));
    assertThat(tokensA.get(0), lessThan(tokensA.get(1)));
    assertThat(tokensB.get(0), lessThan(tokensB.get(1)));
    assertThat(tokensA.get(0), lessThan(tokensB.get(0)));
    assertThat(tokensA.get(1), equalTo(tokensB.get(1)));

    // A: window 0's id=5 survives, window 2's DELETE removed window 0's id=1.
    assertThat(destRows(a), contains("5:" + tableA));
    assertThat(destRows(b), contains("2:" + tableB, "3:" + tableB));
  }

  /** A routing input row for the {@code (id, dest, seq)} dynamic-destination schema. */
  private static Row destRow(Schema schema, int id, String dest, long seq) {
    return Row.withSchema(schema).addValues(id, dest, seq).build();
  }

  /** Reads all live rows of a dynamic-destination table as sorted {@code id:dest} strings. */
  private static List<String> destRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(r -> r.getField("id") + ":" + r.getField("dest"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  // ---------------------------------------------------------------------------------------------
  // 6. Upsert end-to-end
  // ---------------------------------------------------------------------------------------------

  @Test
  public void upsertModeAppliesAfterImageOnly() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    // Upsert mode: only INSERT / UPDATE_AFTER, no before-images at all.
    PCollection<Row> in =
        boundedInput(
            INPUT_SCHEMA,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(UPDATE_AFTER, row(1, "a", "z", 2L)));
    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withUpsert(true));
    p.run().waitUntilFinish();

    // id=1's UPDATE_AFTER (seq 2) replaces its INSERT (seq 1); id=2 unaffected.
    assertThat(readRows(t), contains("1:a:z", "2:b:y"));
  }

  /**
   * Upsert mode still applies DELETEs: widening the upsert drop predicate from "UPDATE_BEFORE" to
   * "anything not an after-image" would swallow every DELETE, and nothing else in the suite fails.
   */
  @Test
  public void upsertModeStillAppliesDeletes() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    PCollection<Row> in =
        boundedInput(
            INPUT_SCHEMA,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(UPDATE_AFTER, row(1, "a", "z", 2L)),
            KV.of(DELETE, row(2, "b", "y", 3L)));
    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withUpsert(true));
    p.run().waitUntilFinish();

    // id=2 is genuinely gone; id=1 carries its after-image.
    assertThat(readRows(t), contains("1:a:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // 8. Dead letters: exposure, first-late diversion, replayability
  // ---------------------------------------------------------------------------------------------

  /**
   * A FIRST-late pane (the shard-window's only record arrives after the watermark passed) is
   * dead-lettered, not committed: pane timing is per shard-window, the committer's skip is per
   * destination-window, so the pane cannot prove its window uncommitted.
   */
  @Test
  public void firstLatePaneIsDeadLetteredNotCommitted() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            // Advance PAST window [0, 60s) with no data, then deliver its first (late) element.
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(120)))
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(1, "a", "x", 1L)), base.plus(Duration.standardSeconds(1))))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(window)
                .withAllowedLateness(Duration.standardDays(7)));

    // Captured by the PAssert lambda: TableIdentifier itself is not Serializable.
    String destString = id.toString();
    PAssert.that(result.getDeadLetterRows())
        .satisfies(
            rows -> {
              Row dl = Iterables.getOnlyElement(rows);
              assertThat(dl.getRow("record"), equalTo(rowNoSeq(1, "a", "x")));
              assertThat(dl.getString("change_type"), equalTo("INSERT"));
              assertThat(dl.getInt64("sequence_number"), equalTo(1L));
              assertThat(dl.getString("destination"), equalTo(destString));
              return null;
            });
    p.run().waitUntilFinish();

    // The first-late record was dead-lettered (replayable), not committed.
    assertThat(readRows(t), hasSize(0));
  }

  /**
   * A late record whose key hashes to a previously-untouched shard of a window that already
   * committed opens a fresh key+window whose first pane is late: the case a {@code !pane.isFirst()}
   * test lets through, after which the committer discards it and the row reaches no output. It must
   * be dead-lettered.
   */
  @Test
  public void firstLatePaneOnAnUntouchedShardIsDeadLettered() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    int onTimeId = 1;
    int lateId = idOnADifferentShardThan(onTimeId, CdcWriteConfig.DEFAULT_NUM_SHARDS);

    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            // On-time data commits window [0, 60s); then a late record for the SAME window on a
            // shard that saw nothing.
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(onTimeId, "a", "x", 1L)),
                    base.plus(Duration.standardSeconds(1))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(70)))
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(lateId, "late", "z", 2L)),
                    base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(window)
                .withAllowedLateness(Duration.standardDays(7)));

    // Captured by the PAssert lambda: TableIdentifier itself is not Serializable.
    String destString = id.toString();
    PAssert.that(result.getDeadLetterRows())
        .satisfies(
            rows -> {
              Row dl = Iterables.getOnlyElement(rows);
              assertThat(dl.getRow("record"), equalTo(rowNoSeq(lateId, "late", "z")));
              assertThat(dl.getString("change_type"), equalTo("INSERT"));
              assertThat(dl.getInt64("sequence_number"), equalTo(2L));
              assertThat(dl.getString("destination"), equalTo(destString));
              return null;
            });
    p.run().waitUntilFinish();

    // NOT silently lost: it is absent from the table (above) and present in the dead letters.
    assertThat(readRows(t), contains(onTimeId + ":a:x"));
  }

  /**
   * The lowest id above {@code id} mapping to a different shard: computed, not hard-coded, so the
   * test survives hash or pk-encoding changes.
   */
  private static int idOnADifferentShardThan(int id, int numShards) {
    int target = shardForId(id, numShards);
    for (int candidate = id + 1; candidate < id + 10_000; candidate++) {
      if (shardForId(candidate, numShards) != target) {
        return candidate;
      }
    }
    throw new AssertionError("no id maps to a shard other than " + target);
  }

  /** The shard {@code AssignCdcKeys} assigns to a row with primary key {@code id}. */
  private static int shardForId(int id, int numShards) {
    Schema pkSchema = Schema.builder().addInt32Field("id").build();
    try {
      byte[] pkBytes =
          CoderUtils.encodeToByteArray(
              RowCoder.of(pkSchema), Row.withSchema(pkSchema).addValue(id).build());
      return TableSetup.shardFor(pkBytes, numShards);
    } catch (CoderException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Pins that {@link WriteCdcRows#withAllowedLateness} is honored: a record late but within the
   * bound is dead-lettered, one past the bound is dropped ENTIRELY by the runner before {@code
   * SplitLateData} sees it: no dead letter, no counter, no row. That documented asymmetry is
   * exactly what a wrong or ignored lateness value would change.
   */
  @Test
  public void recordsBeyondAllowedLatenessAreDroppedNotDeadLettered() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    Duration window = Duration.standardSeconds(60);
    Duration allowedLateness = Duration.standardMinutes(10);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            // Window [0, 60s) closes empty; ~5 min late is inside the 10-min bound.
            .advanceWatermarkTo(base.plus(Duration.standardMinutes(5)))
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(1, "within", "x", 1L)),
                    base.plus(Duration.standardSeconds(1))))
            // Past window end + the whole allowed lateness: dropped outright.
            .advanceWatermarkTo(base.plus(Duration.standardMinutes(30)))
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(2, "beyond", "y", 2L)),
                    base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(window)
                .withAllowedLateness(allowedLateness));

    PAssert.that(result.getDeadLetterRows())
        .satisfies(
            rows -> {
              // Exactly the within-bound record; the beyond-bound one never reaches the sink.
              Row dl = Iterables.getOnlyElement(rows);
              assertThat(dl.getRow("record"), equalTo(rowNoSeq(1, "within", "x")));
              return null;
            });
    p.run().waitUntilFinish();

    assertThat(readRows(t), hasSize(0));
  }

  /**
   * A NON-first late pane (its shard-window already fired on time) is diverted to the replayable
   * dead-letter output with the three metadata columns, and is NOT committed.
   */
  @Test
  public void deadLetterRowsAreExposed() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(1, "a", "x", 1L)), base.plus(Duration.standardSeconds(1))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(70))) // close window [0, 60s)
            // The same key again, timestamped INSIDE the closed window: a NON-first late pane.
            .addElements(
                TimestampedValue.of(
                    KV.of(UPDATE_AFTER, row(1, "late", "z", 2L)),
                    base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(window)
                .withAllowedLateness(Duration.standardDays(7)));

    // Captured by the PAssert lambda: TableIdentifier itself is not Serializable.
    String destString = id.toString();
    PAssert.that(result.getDeadLetterRows())
        .satisfies(
            rows -> {
              Row dl = Iterables.getOnlyElement(rows);
              assertThat(dl.getRow("record"), equalTo(rowNoSeq(1, "late", "z")));
              assertThat(dl.getString("change_type"), equalTo("UPDATE_AFTER"));
              assertThat(dl.getInt64("sequence_number"), equalTo(2L));
              assertThat(dl.getString("destination"), equalTo(destString));
              return null;
            });
    p.run().waitUntilFinish();

    // Invariant: dead-lettered => NOT committed. The table has only the on-time INSERT (1:a:x).
    assertThat(readRows(t), contains("1:a:x"));
  }

  /**
   * The dead letters are REPLAYABLE through the real API: unnest {@code record}, map {@code
   * change_type}/{@code sequence_number} as the replay sink's control columns, drop {@code
   * destination}, and the row lands. The late record uses a different key than the on-time one so
   * the final state is independent of commit interleaving.
   */
  @Test
  public void deadLettersAreReplayable() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    Duration window = Duration.standardSeconds(60);
    Instant base = new Instant(0);

    TestStream<KV<ValueKind, Row>> stream =
        TestStream.create(
                KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(INPUT_SCHEMA)))
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(
                    KV.of(INSERT, row(1, "a", "x", 1L)), base.plus(Duration.standardSeconds(1))))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(70))) // window [0,60s) fires
            .addElements(
                TimestampedValue.of(
                    KV.of(UPDATE_AFTER, row(7, "late", "z", 2L)),
                    base.plus(Duration.standardSeconds(2))))
            .advanceWatermarkToInfinity();

    PCollection<Row> in = CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(INPUT_SCHEMA);
    IcebergWriteResult result =
        in.apply(
            "OriginalSink",
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(window)
                .withAllowedLateness(Duration.standardDays(7)));

    PCollection<Row> deadLetters = result.getDeadLetterRows();
    assertNotNull(deadLetters);
    // Captured by the PAssert lambda: TableIdentifier itself is not Serializable.
    String destString = id.toString();
    PAssert.that(deadLetters)
        .satisfies(
            rows -> {
              Row dl = Iterables.getOnlyElement(rows);
              assertThat(dl.getRow("record"), equalTo(rowNoSeq(7, "late", "z")));
              assertThat(dl.getString("change_type"), equalTo("UPDATE_AFTER"));
              assertThat(dl.getInt64("sequence_number"), equalTo(2L));
              assertThat(dl.getString("destination"), equalTo(destString));
              return null;
            });

    // Re-feed the ACTUAL dead letters.
    Schema replaySchema =
        Schema.builder()
            .addFields(DATA_SCHEMA.getFields())
            .addStringField("change_type")
            .addInt64Field("sequence_number")
            .build();
    PCollection<Row> replayInput =
        deadLetters
            .apply(
                "UnnestRecord",
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        dl ->
                            Row.withSchema(replaySchema)
                                .addValues(checkStateNotNull(dl.getRow("record")).getValues())
                                .addValue(dl.getString("change_type"))
                                .addValue(dl.getInt64("sequence_number"))
                                .build()))
            .setRowSchema(replaySchema)
            // A dead letter's own event time is behind the watermark by definition; re-fed as-is
            // it would just be late again, so a replay re-stamps it as fresh input.
            .apply("RestampForReplay", ParDo.of(new RestampTo(REPLAY_EVENT_TIME)))
            .setRowSchema(replaySchema);
    replayInput.apply(
        "ReplaySink",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withChangeTypeColumn("change_type")
            .withSequenceNumberColumn("sequence_number")
            .withTriggeringFrequency(window)
            .withAllowedLateness(Duration.standardDays(7)));

    p.run().waitUntilFinish();

    assertThat(readRows(t), containsInAnyOrder("1:a:x", "7:late:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // 9. Error handling (failed rows)
  // ---------------------------------------------------------------------------------------------

  /**
   * With {@code withErrorHandling()}, a poison record (an unknown change-type value) is diverted to
   * {@link IcebergWriteResult#getFailedRows()}; the other records commit normally.
   */
  @Test
  public void errorHandlingDivertsPoisonRecords() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    Schema inputWithOp =
        Schema.builder()
            .addFields(DATA_SCHEMA.getFields())
            .addInt64Field("seq")
            .addStringField("op")
            .build();
    Row r1 = Row.withSchema(inputWithOp).addValues(1, "a", "x", 1L, "INSERT").build();
    Row r2 = Row.withSchema(inputWithOp).addValues(2, "b", "y", 1L, "BOGUS").build(); // poison
    Row r3 = Row.withSchema(inputWithOp).addValues(3, "c", "z", 1L, "INSERT").build();

    PCollection<Row> in =
        boundedInput(inputWithOp, KV.of(INSERT, r1), KV.of(INSERT, r2), KV.of(INSERT, r3));
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withChangeTypeColumn("op")
                .withErrorHandling());

    PCollection<Row> failedRows = result.getFailedRows();
    assertNotNull(failedRows);
    PAssert.that(failedRows)
        .satisfies(
            rows -> {
              Row err = Iterables.getOnlyElement(rows);
              Row failedRow = err.getRow("failed_row");
              assertThat(failedRow.getInt32("id"), equalTo(2));
              assertThat(failedRow.getString("op"), equalTo("BOGUS"));
              // Operators must see WHICH change-type value was bad, not just that something failed.
              assertThat(err.getString("error_message"), containsString("BOGUS"));
              return null;
            });
    p.run().waitUntilFinish();

    assertThat(readRows(t), contains("1:a:x", "3:c:z"));
  }

  /** Without {@code withErrorHandling()}, the failed-rows accessor is {@code null}. */
  @Test
  public void failedRowsNullWithoutErrorHandling() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "nofail_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id).withSequenceNumberColumn("seq"));
    assertThrows(IllegalStateException.class, result::getFailedRows);
    assertNotNull(result.getDeadLetterRows());
    assertNotNull(result.getSnapshots());
  }

  // ---------------------------------------------------------------------------------------------
  // 10. Dead-letter column names pinned
  // ---------------------------------------------------------------------------------------------

  /** The dead-letter metadata column names and types are a public contract: pinned literally. */
  @Test
  public void deadLetterColumnNamesPinned() {
    p.enableAbandonedNodeEnforcement(false);
    TableIdentifier id = TableIdentifier.of("db", "dlpin_" + System.nanoTime());
    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));
    IcebergWriteResult result =
        in.apply(
            IcebergIO.writeCdcRows(bogusCatalogConfig()).to(id).withSequenceNumberColumn("seq"));

    PCollection<Row> deadLetters = result.getDeadLetterRows();
    assertNotNull(deadLetters);
    Schema dl = deadLetters.getSchema();
    assertThat(
        dl.getFieldNames(), contains("record", "change_type", "sequence_number", "destination"));
    assertThat(dl.getField("record").getType(), equalTo(Schema.FieldType.row(DATA_SCHEMA)));
    assertThat(dl.getField("change_type").getType(), equalTo(Schema.FieldType.STRING));
    assertThat(dl.getField("sequence_number").getType(), equalTo(Schema.FieldType.INT64));
    assertThat(dl.getField("destination").getType(), equalTo(Schema.FieldType.STRING));
  }

  // ---------------------------------------------------------------------------------------------
  // 12. Two-pipeline batch rerun with stable sink id
  // ---------------------------------------------------------------------------------------------

  /**
   * A true two-pipeline batch rerun (not a hand-seeded token): the second pipeline, same table and
   * {@code sink_id} but a different row, must write nothing: the batch token is recovered from the
   * real prior commit and the rerun's window skipped loudly.
   */
  @Test
  public void secondBatchRunWithStableSinkIdWritesNothing() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);
    String sinkId = "stable-rerun";

    // P1: first batch run, one INSERT under the stable sink id.
    boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId));
    p.run().waitUntilFinish();

    t.refresh();
    assertThat(Lists.newArrayList(t.snapshots()), hasSize(1));
    assertThat(readRows(t), contains("1:a:x"));

    // P2: a fresh batch pipeline, SAME table + SAME sink id, DIFFERENT row.
    Pipeline p2 = Pipeline.create();
    CdcSinkTestUtils.withKinds(p2.apply(Create.of(KV.of(INSERT, row(2, "b", "y", 2L)))))
        .setRowSchema(INPUT_SCHEMA)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId));
    PipelineResult r2 = p2.run();
    r2.waitUntilFinish();

    t.refresh();
    // Still one snapshot, still only the first row, and the skip was loud.
    assertThat(Lists.newArrayList(t.snapshots()), hasSize(1));
    assertThat(readRows(t), contains("1:a:x"));
    assertThat(committerCounter(r2, "alreadyCommittedWindowsSkipped"), greaterThanOrEqualTo(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // 13. Token heartbeat gated to streaming
  // ---------------------------------------------------------------------------------------------

  /**
   * A heartbeat configured on BOUNDED input must be dropped with a warn at {@code expand()}: a
   * self-rescheduling processing-time timer can keep a runner from quiescing. The run terminates
   * promptly with exactly one snapshot.
   */
  @Test(timeout = 120_000)
  public void tokenHeartbeatIgnoredForBoundedInput() {
    setUpCatalog();
    TableIdentifier id = v2Table();
    Table t = catalog.loadTable(id);

    PCollection<Row> in = boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)));
    in.apply(
        IcebergIO.writeCdcRows(catalogConfig())
            .to(id)
            .withSequenceNumberColumn("seq")
            .withTokenHeartbeat(Duration.millis(1)));

    // expand() runs during apply(); on bounded input it must warn that the heartbeat is dropped.
    expectedLogs.verifyWarn("heartbeat is ignored for bounded input");

    p.run().waitUntilFinish();

    t.refresh();
    assertThat(Lists.newArrayList(t.snapshots()), hasSize(1));
  }

  /** Re-stamps every element to one fixed event time (a replay assigns fresh event times). */
  private static final class RestampTo extends DoFn<Row, Row> {
    private final Instant timestamp;

    RestampTo(Instant timestamp) {
      this.timestamp = timestamp;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> out) {
      out.outputWithTimestamp(row, timestamp);
    }
  }
}
