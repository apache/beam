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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.iceberg.DynamicDestinations;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Read-back end-to-end tests for the assembled CDC sink: each case drives the full public transform
 * against a real HadoopCatalog table and asserts the table's final committed contents via {@link
 * IcebergGenerics#read}: the sink's ground truth, where a composition bug the per-stage suites
 * cannot see shows up as wrong rows. Every case asserts the COMPLETE expected row set, all columns,
 * never a count. The partition-type cases pin ground the path-rendered predecessor banned or
 * crashed on; the typed-JSON transport carries partition tuples with no rendered path to parse.
 * Every test creates a uniquely named table (TableCache is process-wide).
 */
@RunWith(JUnit4.class)
public class WriteCdcRowsReadBackTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  /** Canonical test table schema, shared with the other {@code cdc/sink} suites. */
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

  /** Input schema = the canonical data schema + a sequence-number column named {@code seq}. */
  private static final Schema INPUT_SCHEMA =
      Schema.builder().addFields(DATA_SCHEMA.getFields()).addInt64Field("seq").build();

  /** Event-time origin for the streaming (TestStream) cases. */
  private static final org.joda.time.Instant BASE = new org.joda.time.Instant(0);

  private static final Duration WINDOW = Duration.standardSeconds(60);

  private Catalog catalog;

  @Before
  public void setUp() {
    catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
  }

  private IcebergCatalogConfig catalogConfig() {
    return CdcSinkTestUtils.catalogConfig(tmp.getRoot());
  }

  // -----------------------------------------------------------------------------------------------
  // Fixtures
  // -----------------------------------------------------------------------------------------------

  /** Creates a uniquely named table and returns its identifier. */
  private TableIdentifier createTable(
      String prefix,
      org.apache.iceberg.Schema schema,
      Set<Integer> identifierFieldIds,
      int formatVersion,
      PartitionSpec spec) {
    TableIdentifier id = TableIdentifier.of("db", prefix + "_" + System.nanoTime());
    CdcSinkTestUtils.createTable(catalog, id, schema, identifierFieldIds, formatVersion, spec);
    return id;
  }

  /** A uniquely named unpartitioned table over {@link #ICEBERG_SCHEMA} (PK = {@code id}). */
  private TableIdentifier createCanonicalTable(String prefix, int formatVersion) {
    return createTable(
        prefix, ICEBERG_SCHEMA, ImmutableSet.of(1), formatVersion, PartitionSpec.unpartitioned());
  }

  /** The sink's input schema for {@code table}: its Beam schema plus the {@code seq} column. */
  private static Schema inputSchemaFor(Table table) {
    return Schema.builder()
        .addFields(IcebergUtils.icebergSchemaToBeamSchema(table.schema()).getFields())
        .addInt64Field("seq")
        .build();
  }

  /** Builds an input row over the canonical {@link #INPUT_SCHEMA}. */
  private static Row row(int id, String name, String data, long seq) {
    return Row.withSchema(INPUT_SCHEMA).addValues(id, name, data, seq).build();
  }

  /** Builds an input row over an arbitrary schema (the per-test partitioned/timestamp tables). */
  private static Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  /** A bounded CDC input of the given kind-tagged rows. */
  @SafeVarargs
  private final PCollection<Row> boundedInput(Schema schema, KV<ValueKind, Row>... rows) {
    return boundedInput(schema, ImmutableList.copyOf(rows));
  }

  /** A bounded CDC input of the given kind-tagged rows. */
  private PCollection<Row> boundedInput(Schema schema, List<KV<ValueKind, Row>> rows) {
    return CdcSinkTestUtils.withKinds(p.apply(Create.of(rows).withCoder(taggedRowCoder(schema))))
        .setRowSchema(schema);
  }

  /** An unbounded CDC input driven by {@code stream}. */
  private PCollection<Row> streamingInput(Schema schema, TestStream<KV<ValueKind, Row>> stream) {
    return CdcSinkTestUtils.withKinds(p.apply(stream)).setRowSchema(schema);
  }

  private static KvCoder<ValueKind, Row> taggedRowCoder(Schema schema) {
    return KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(schema));
  }

  private static TestStream.Builder<KV<ValueKind, Row>> testStream(Schema schema) {
    return TestStream.create(taggedRowCoder(schema));
  }

  private static TimestampedValue<KV<ValueKind, Row>> at(ValueKind kind, Row row, int atSeconds) {
    return TimestampedValue.of(KV.of(kind, row), BASE.plus(Duration.standardSeconds(atSeconds)));
  }

  // -----------------------------------------------------------------------------------------------
  // Ground truth: read the committed table back
  // -----------------------------------------------------------------------------------------------

  /**
   * Every live row, the named columns joined by {@code ':'}, sorted; {@code timestamptz} rendered
   * as micros-since-epoch so a truncated or shifted timestamp cannot hide behind formatting.
   */
  private static List<String> readColumns(Table table, String... columns) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(
            record ->
                Arrays.stream(columns)
                    .map(column -> render(record.getField(column)))
                    .collect(Collectors.joining(":")))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  /** {@link #readColumns} over the canonical {@code (id, name, data)} schema. */
  private static List<String> readRows(Table table) {
    return readColumns(table, "id", "name", "data");
  }

  private static String render(@Nullable Object value) {
    if (value instanceof OffsetDateTime) {
      return String.valueOf(DateTimeUtil.microsFromTimestamptz((OffsetDateTime) value));
    }
    return String.valueOf(value);
  }

  /** {@code instant} as micros since epoch, matching how {@link #readColumns} renders it. */
  private static String micros(Instant instant) {
    return String.valueOf(ChronoUnit.MICROS.between(Instant.EPOCH, instant));
  }

  /**
   * The distinct partition tuples of the live data files: proves WHERE rows physically landed,
   * which the row contents alone cannot show. Binary values render as hex so a byte-lossy
   * round-trip is visible.
   */
  private static List<String> scannedPartitions(Table table, int numFields) throws IOException {
    table.refresh();
    Set<String> partitions = new TreeSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        StructLike partition = task.file().partition();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
          Object value = partition.get(i, Object.class);
          values.add(
              value instanceof ByteBuffer || value instanceof byte[]
                  ? hex(value)
                  : String.valueOf(value));
        }
        partitions.add(String.join(":", values));
      }
    }
    return ImmutableList.copyOf(partitions);
  }

  private static int snapshotCount(Table table) {
    table.refresh();
    return Lists.newArrayList(table.snapshots()).size();
  }

  private static List<DeleteFile> addedDeleteFiles(Table table) {
    table.refresh();
    return Lists.newArrayList(
        checkStateNotNull(table.currentSnapshot()).addedDeleteFiles(table.io()));
  }

  // -----------------------------------------------------------------------------------------------
  // 1. Same-commit insert + delete (V2)
  // -----------------------------------------------------------------------------------------------

  /**
   * A key inserted then deleted inside one commit window leaves no trace, and the same-commit dedup
   * touches nothing else.
   */
  @Test
  public void v2SameCommitInsertThenDeleteLeavesNoRowForTheKey() {
    TableIdentifier id = createCanonicalTable("v2dedup", 2);
    Table t = catalog.loadTable(id);

    boundedInput(
            INPUT_SCHEMA,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(DELETE, row(1, "a", "x", 2L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(INSERT, row(3, "c", "z", 1L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(readRows(t), containsInAnyOrder("2:b:y", "3:c:z"));
  }

  // -----------------------------------------------------------------------------------------------
  // 2. Same-commit dedup on a format-version-3 table
  // -----------------------------------------------------------------------------------------------

  /**
   * Same-commit churn on V3 collapses inside the writer: the commit carries only each key's final
   * row and no delete files at all. Structurally case 1 on a V3 table, kept because V3 is the
   * format version the sink used to special-case, and it must keep working end to end.
   */
  @Test
  public void v3SameCommitChangesCollapseWithoutDeleteFiles() {
    TableIdentifier id = createCanonicalTable("v3dedup", 3);
    Table t = catalog.loadTable(id);

    boundedInput(
            INPUT_SCHEMA,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(UPDATE_BEFORE, row(1, "a", "x", 2L)),
            KV.of(UPDATE_AFTER, row(1, "a2", "z", 2L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(DELETE, row(2, "b", "y", 2L)),
            KV.of(INSERT, row(3, "c", "w", 1L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(readRows(t), containsInAnyOrder("1:a2:z", "3:c:w"));

    // Every change here is same-window churn, so the collapse leaves nothing to delete.
    assertThat(addedDeleteFiles(t), empty());
  }

  // -----------------------------------------------------------------------------------------------
  // 3. Cross-commit delete
  // -----------------------------------------------------------------------------------------------

  /**
   * A later window's delete removes a row committed by an earlier snapshot: the cross-commit
   * equality delete reaches back, and only the deleted key disappears.
   */
  @Test
  public void crossCommitDeleteRemovesRowCommittedByEarlierWindow() {
    TableIdentifier id = createCanonicalTable("xcommit", 2);
    Table t = catalog.loadTable(id);

    TestStream<KV<ValueKind, Row>> stream =
        testStream(INPUT_SCHEMA)
            .advanceWatermarkTo(BASE)
            .addElements(
                at(INSERT, row(1, "a", "x", 1L), 1),
                at(INSERT, row(2, "b", "y", 1L), 2),
                at(INSERT, row(3, "c", "z", 1L), 3))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70))) // window 0 commits
            .addElements(at(DELETE, row(1, "a", "x", 2L), 61))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130))) // window 1 commits
            .advanceWatermarkToInfinity();

    streamingInput(INPUT_SCHEMA, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    // Two windows really did commit separately; otherwise this would be same-commit dedup again.
    assertThat(snapshotCount(t), equalTo(2));
    assertThat(readRows(t), containsInAnyOrder("2:b:y", "3:c:z"));
  }

  // -----------------------------------------------------------------------------------------------
  // 4. Upsert across commit windows
  // -----------------------------------------------------------------------------------------------

  /**
   * In upsert mode, a later window's image of a key replaces the one an earlier window committed:
   * one row survives, carrying the later values.
   */
  @Test
  public void upsertCrossWindowReplacesEarlierCommittedImage() {
    TableIdentifier id = createCanonicalTable("upsertxwin", 2);
    Table t = catalog.loadTable(id);

    TestStream<KV<ValueKind, Row>> stream =
        testStream(INPUT_SCHEMA)
            .advanceWatermarkTo(BASE)
            .addElements(
                at(INSERT, row(1, "a", "v1", 1L), 1), at(INSERT, row(2, "b", "keep", 1L), 2))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70))) // window 0 commits
            .addElements(at(UPDATE_AFTER, row(1, "a2", "v2", 2L), 61))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130))) // window 1 commits
            .advanceWatermarkToInfinity();

    streamingInput(INPUT_SCHEMA, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withUpsert(true)
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    assertThat(snapshotCount(t), equalTo(2));
    assertThat(readRows(t), containsInAnyOrder("1:a2:v2", "2:b:keep"));
  }

  // -----------------------------------------------------------------------------------------------
  // 5. Adversarial element order within one window
  // -----------------------------------------------------------------------------------------------

  /**
   * Element arrival order inside a commit window is irrelevant: a DELETE fed before its own INSERT
   * and out-of-order after-images still produce the sequence-order final state.
   */
  @Test
  public void streamingShuffledWithinWindowAppliesSequenceOrder() {
    TableIdentifier id = createCanonicalTable("shuffled", 2);
    Table t = catalog.loadTable(id);

    // All eight changes land in window [0s, 60s), arrival order deliberately adversarial. Event
    // times are equal, so the per-key event-time/sequence source contract still holds.
    TestStream<KV<ValueKind, Row>> stream =
        testStream(INPUT_SCHEMA)
            .advanceWatermarkTo(BASE)
            .addElements(
                at(DELETE, row(1, "a2", "x2", 3L), 5),
                at(UPDATE_AFTER, row(2, "v3", "c", 3L), 5),
                at(UPDATE_AFTER, row(1, "a2", "x2", 2L), 5),
                at(INSERT, row(3, "c", "w", 1L), 5),
                at(INSERT, row(2, "v1", "a", 1L), 5),
                at(UPDATE_BEFORE, row(1, "a", "x", 2L), 5),
                at(UPDATE_AFTER, row(2, "v2", "b", 2L), 5),
                at(INSERT, row(1, "a", "x", 1L), 5))
            .advanceWatermarkTo(
                BASE.plus(Duration.standardSeconds(70))) // the single window commits
            .advanceWatermarkToInfinity();

    streamingInput(INPUT_SCHEMA, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    // One window => one snapshot, so this really is intra-window ordering, not commit ordering.
    assertThat(snapshotCount(t), equalTo(1));
    // In sequence order id=1 ends deleted, id=2's seq-3 after-image wins, id=3 is untouched.
    assertThat(readRows(t), containsInAnyOrder("2:v3:c", "3:c:w"));
  }

  // -----------------------------------------------------------------------------------------------
  // 7. day(timestamptz) partitioning, through the full sink
  // -----------------------------------------------------------------------------------------------

  /**
   * A {@code day(timestamptz)}-partitioned table takes inserts, updates and deletes through the
   * whole sink, and every row lands in the day partition its timestamp implies, with no rendered
   * partition path anywhere (the predecessor crashed here until DATE got a renderer special-case).
   */
  @Test
  public void dayPartitionedTableAppliesChangesAndLandsInExpectedPartitions() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "name", Types.StringType.get()));
    // ts is also an equality column: the key-derived-partition shape (case 16 covers non-key).
    TableIdentifier id =
        createTable(
            "daypart",
            icebergSchema,
            ImmutableSet.of(1, 2),
            2,
            PartitionSpec.builderFor(icebergSchema).day("ts").build());
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    Instant march = Instant.parse("2024-03-15T10:30:00.123456Z");
    Instant june = Instant.parse("2024-06-01T00:00:00Z");
    Instant marchLate = Instant.parse("2024-03-15T23:59:59.999999Z");

    boundedInput(
            inputSchema,
            KV.of(INSERT, row(inputSchema, 1, march, "a", 1L)),
            KV.of(INSERT, row(inputSchema, 2, june, "b", 1L)),
            KV.of(INSERT, row(inputSchema, 3, marchLate, "c", 1L)),
            // Update inside the March partition (ts unchanged, so the key is unchanged).
            KV.of(UPDATE_BEFORE, row(inputSchema, 1, march, "a", 2L)),
            KV.of(UPDATE_AFTER, row(inputSchema, 1, march, "a2", 2L)),
            // Delete the other March row.
            KV.of(DELETE, row(inputSchema, 3, marchLate, "c", 2L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(
        readColumns(t, "id", "ts", "name"),
        containsInAnyOrder("1:" + micros(march) + ":a2", "2:" + micros(june) + ":b"));

    // Rows landed in the day partitions their timestamps imply, not in one lump.
    assertThat(
        scannedPartitions(t, 1),
        containsInAnyOrder(
            String.valueOf((int) LocalDate.of(2024, 3, 15).toEpochDay()),
            String.valueOf((int) LocalDate.of(2024, 6, 1).toEpochDay())));
  }

  // -----------------------------------------------------------------------------------------------
  // 8. Multi-transform partition spec, through the full sink
  // -----------------------------------------------------------------------------------------------

  /**
   * A spec combining {@code hour(ts)} and {@code identity(DATE)} applies end to end: contents
   * right, each row in its {@code (hour, date)} partition pair.
   */
  @Test
  public void multiTransformPartitionSpecAppliesChangesEndToEnd() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "d", Types.DateType.get()),
            Types.NestedField.optional(4, "name", Types.StringType.get()));
    TableIdentifier id =
        createTable(
            "multipart",
            icebergSchema,
            ImmutableSet.of(1, 2, 3),
            2,
            PartitionSpec.builderFor(icebergSchema).hour("ts").identity("d").build());
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    Instant hour10 = Instant.parse("2024-03-15T10:30:00Z");
    Instant hour10b = Instant.parse("2024-03-15T10:45:00Z");
    Instant hour11 = Instant.parse("2024-03-15T11:05:00Z");
    LocalDate day15 = LocalDate.of(2024, 3, 15);
    LocalDate day16 = LocalDate.of(2024, 3, 16);

    boundedInput(
            inputSchema,
            KV.of(INSERT, row(inputSchema, 1, hour10, day15, "a", 1L)),
            KV.of(INSERT, row(inputSchema, 2, hour11, day16, "b", 1L)),
            KV.of(INSERT, row(inputSchema, 3, hour10b, day15, "c", 1L)),
            KV.of(UPDATE_BEFORE, row(inputSchema, 1, hour10, day15, "a", 2L)),
            KV.of(UPDATE_AFTER, row(inputSchema, 1, hour10, day15, "a2", 2L)),
            KV.of(DELETE, row(inputSchema, 3, hour10b, day15, "c", 2L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(
        readColumns(t, "id", "ts", "d", "name"),
        containsInAnyOrder(
            "1:" + micros(hour10) + ":" + day15 + ":a2",
            "2:" + micros(hour11) + ":" + day16 + ":b"));

    assertThat(
        scannedPartitions(t, 2),
        containsInAnyOrder(
            hoursFromEpoch(hour10) + ":" + (int) day15.toEpochDay(),
            hoursFromEpoch(hour11) + ":" + (int) day16.toEpochDay()));
  }

  private static long hoursFromEpoch(Instant instant) {
    return ChronoUnit.HOURS.between(Instant.EPOCH, instant);
  }

  // -----------------------------------------------------------------------------------------------
  // 9. timestamptz round-trip at microsecond precision
  // -----------------------------------------------------------------------------------------------

  /**
   * {@code timestamptz} survives the whole sink at microsecond precision, including pre-epoch
   * instants (negative seconds, non-negative sub-second part).
   */
  @Test
  public void timestamptzRoundTripsAtMicrosecondPrecision() {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
            Types.NestedField.optional(3, "name", Types.StringType.get()));
    TableIdentifier id =
        createTable("tstz", icebergSchema, ImmutableSet.of(1), 2, PartitionSpec.unpartitioned());
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    Instant afterEpoch = Instant.parse("2024-03-15T10:30:00.123456Z");
    Instant beforeEpoch = Instant.parse("1969-07-20T20:17:40.000001Z");
    Instant longBeforeEpoch = Instant.parse("1901-12-13T20:45:52.654321Z");
    Instant updated = Instant.parse("1955-11-05T06:15:00.000123Z");
    Instant deleted = Instant.parse("1962-10-16T12:00:00.999999Z");

    boundedInput(
            inputSchema,
            KV.of(INSERT, row(inputSchema, 1, afterEpoch, "after", 1L)),
            KV.of(INSERT, row(inputSchema, 2, beforeEpoch, "before", 1L)),
            KV.of(INSERT, row(inputSchema, 3, longBeforeEpoch, "long-before", 1L)),
            KV.of(INSERT, row(inputSchema, 4, deleted, "deleted", 1L)),
            // The timestamp itself is updated (ts is not part of the key here).
            KV.of(UPDATE_BEFORE, row(inputSchema, 2, beforeEpoch, "before", 2L)),
            KV.of(UPDATE_AFTER, row(inputSchema, 2, updated, "before-updated", 2L)),
            // The DELETE is what fails a sink that ignores change kinds: same-commit dedup would
            // reproduce id=2's expected row regardless, but a DELETE has no such cover.
            KV.of(DELETE, row(inputSchema, 4, deleted, "deleted", 2L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(
        readColumns(t, "id", "ts", "name"),
        containsInAnyOrder(
            "1:" + micros(afterEpoch) + ":after",
            "2:" + micros(updated) + ":before-updated",
            "3:" + micros(longBeforeEpoch) + ":long-before"));
  }

  // -----------------------------------------------------------------------------------------------
  // 10. '/' inside a string partition value
  // -----------------------------------------------------------------------------------------------

  /**
   * A {@code '/'} inside an identity-partitioned string value round-trips: the tuple travels as
   * typed JSON, so a path-separator lookalike is just a value. The predecessor banned this outright
   * ({@code SerializableDataFileTest} still pins the NaN path-fallback's encoded form).
   */
  @Test
  public void slashInStringPartitionValueAppliesChangesEndToEnd() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "data", Types.StringType.get()));
    TableIdentifier id =
        createTable(
            "slashpart",
            icebergSchema,
            ImmutableSet.of(1, 2),
            2,
            PartitionSpec.builderFor(icebergSchema).identity("name").build());
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    boundedInput(
            inputSchema,
            KV.of(INSERT, row(inputSchema, 1, "a/b", "x", 1L)),
            KV.of(INSERT, row(inputSchema, 2, "c/d/e", "y", 1L)),
            KV.of(INSERT, row(inputSchema, 3, "plain", "z", 1L)),
            KV.of(INSERT, row(inputSchema, 4, "a/b", "w", 1L)),
            // Update and delete inside the slash-bearing partition.
            KV.of(UPDATE_BEFORE, row(inputSchema, 1, "a/b", "x", 2L)),
            KV.of(UPDATE_AFTER, row(inputSchema, 1, "a/b", "x2", 2L)),
            KV.of(DELETE, row(inputSchema, 4, "a/b", "w", 2L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(readRows(t), containsInAnyOrder("1:a/b:x2", "2:c/d/e:y", "3:plain:z"));
    assertThat(scannedPartitions(t, 1), containsInAnyOrder("a/b", "c/d/e", "plain"));
  }

  // -----------------------------------------------------------------------------------------------
  // 11. shards_per_partition
  // -----------------------------------------------------------------------------------------------

  /** {@code (id INT, ts TIMESTAMPTZ)} both required and both identifier fields, plus a name. */
  private static final org.apache.iceberg.Schema DAY_PARTITIONED_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "ts", Types.TimestampType.withZone()),
          Types.NestedField.optional(3, "name", Types.StringType.get()));

  /** A uniquely named {@code day(ts)}-partitioned table over {@link #DAY_PARTITIONED_SCHEMA}. */
  private TableIdentifier createDayPartitionedTable(String prefix) {
    return createTable(
        prefix,
        DAY_PARTITIONED_SCHEMA,
        ImmutableSet.of(1, 2),
        2,
        PartitionSpec.builderFor(DAY_PARTITIONED_SCHEMA).day("ts").build());
  }

  /** The number of live data files in {@code table}: the quantity the option exists to reduce. */
  private static int dataFileCount(Table table) throws IOException {
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Lists.newArrayList(tasks).size();
    }
  }

  /**
   * The payoff, measured: 48 rows over 8 day partitions written twice at {@code num_shards = 8}.
   * {@code shards_per_partition = 1} yields exactly one data file per day, the uncapped default
   * several per day. Same rows, same partitions; only the file layout differs.
   */
  @Test
  public void shardsPerPartitionOneCollapsesFilesToThePartitionCount() throws IOException {
    int numDays = 8;
    int rowsPerDay = 6;
    int numShards = 8;

    TableIdentifier affineId = createDayPartitionedTable("pas_files_affine");
    TableIdentifier pkId = createDayPartitionedTable("pas_files_pk");
    Table affine = catalog.loadTable(affineId);
    Table pk = catalog.loadTable(pkId);
    Schema inputSchema = inputSchemaFor(affine);

    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    int id = 0;
    for (int day = 0; day < numDays; day++) {
      Instant ts = Instant.parse("2024-03-01T12:00:00Z").plus(day, ChronoUnit.DAYS);
      for (int i = 0; i < rowsPerDay; i++) {
        id++;
        rows.add(KV.of(INSERT, row(inputSchema, id, ts, "n" + id, 1L)));
      }
    }

    // One input, two sinks: the only difference between them is the sharding mode.
    PCollection<Row> in = boundedInput(inputSchema, rows);
    in.apply(
        "WriteAffine",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(affineId)
            .withSequenceNumberColumn("seq")
            .withNumShards(numShards)
            .withShardsPerPartition(1));
    in.apply(
        "WritePk",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(pkId)
            .withSequenceNumberColumn("seq")
            .withNumShards(numShards));
    p.run().waitUntilFinish();

    // Same rows, same partitions: the tables are indistinguishable to a reader.
    assertThat(readColumns(affine, "id", "ts", "name"), hasSize(numDays * rowsPerDay));
    assertThat(
        readColumns(affine, "id", "ts", "name"), equalTo(readColumns(pk, "id", "ts", "name")));
    assertThat(scannedPartitions(affine, 1), hasSize(numDays));
    assertThat(scannedPartitions(affine, 1), equalTo(scannedPartitions(pk, 1)));

    // Exactly one data file per partition, whatever num_shards is.
    assertThat(dataFileCount(affine), equalTo(numDays));
    // Uncapped, several shards write each partition (36 files here, deterministically); the bound
    // is deliberately loose so this asserts the scaling claim, not a hash fixture.
    assertThat(dataFileCount(pk), greaterThanOrEqualTo(2 * numDays));
  }

  /** The {@code (id, ts)} primary-key schema exactly as {@code TableSetup} derives it. */
  private static Schema dayPartitionedPkSchema(Table table) {
    Schema dataSchema = IcebergUtils.icebergSchemaToBeamSchema(table.schema());
    return Schema.builder()
        .addField(dataSchema.getField("id"))
        .addField(dataSchema.getField("ts"))
        .build();
  }

  /** {@code pkBytes} for one {@code (id, ts)} key, byte-identical to the sink's own encoding. */
  private static byte[] encodePk(Schema pkSchema, int id, Instant ts) throws CoderException {
    return CoderUtils.encodeToByteArray(
        RowCoder.of(pkSchema), Row.withSchema(pkSchema).addValues(id, ts).build());
  }

  /**
   * Picks 16 ids whose pk hashes cover all 12 {@code mod 12} residues plus all 4 {@code mod 4}
   * residues once more: exact shard coverage at both {@code shards_per_partition = 4} and the
   * uncapped default, no probabilistic spread.
   */
  private static List<Integer> idsCoveringAllShardResidues(Schema pkSchema, Instant ts, int firstId)
      throws CoderException {
    Map<Integer, Integer> idByShardResidue = new TreeMap<>();
    Map<Integer, Integer> extraIdByOffsetResidue = new TreeMap<>();
    for (int candidate = firstId;
        idByShardResidue.size() < 12 || extraIdByOffsetResidue.size() < 4;
        candidate++) {
      if (candidate - firstId >= 10_000) {
        throw new AssertionError("residue search did not converge; the PK encoding drifted");
      }
      int hash = TableSetup.pkHash(encodePk(pkSchema, candidate, ts));
      if (idByShardResidue.putIfAbsent(Math.floorMod(hash, 12), candidate) != null) {
        extraIdByOffsetResidue.putIfAbsent(Math.floorMod(hash, 4), candidate);
      }
    }
    List<Integer> ids = new ArrayList<>(idByShardResidue.values());
    ids.addAll(extraIdByOffsetResidue.values());
    return ids;
  }

  /**
   * The dial between the endpoints, measured: three day partitions at {@code num_shards = 12},
   * written at {@code shards_per_partition = 4} and uncapped. {@link #idsCoveringAllShardResidues}
   * makes both counts exact (3 x 4 = 12 files dialed, 3 x 12 = 36 uncapped) and both tables must
   * hold identical contents: the dial moves files, never rows.
   */
  @Test
  public void shardsPerPartitionDialsFileCountBetweenTheEndpoints() throws IOException {
    int numShards = 12;
    int shardsPerPartition = 4;
    int numDays = 3;

    TableIdentifier dialId = createDayPartitionedTable("spp_dial");
    TableIdentifier uncappedId = createDayPartitionedTable("spp_dial_uncapped");
    Table dial = catalog.loadTable(dialId);
    Table uncapped = catalog.loadTable(uncappedId);
    Schema inputSchema = inputSchemaFor(dial);
    Schema pkSchema = dayPartitionedPkSchema(dial);

    List<KV<ValueKind, Row>> rows = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (int day = 0; day < numDays; day++) {
      Instant ts = Instant.parse("2024-06-01T12:00:00Z").plus(day, ChronoUnit.DAYS);
      for (int id : idsCoveringAllShardResidues(pkSchema, ts, 1000 * (day + 1))) {
        rows.add(KV.of(INSERT, row(inputSchema, id, ts, "n" + id, 1L)));
        expected.add(id + ":" + micros(ts) + ":n" + id);
      }
    }

    PCollection<Row> in = boundedInput(inputSchema, rows);
    in.apply(
        "WriteDial",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(dialId)
            .withSequenceNumberColumn("seq")
            .withNumShards(numShards)
            .withShardsPerPartition(shardsPerPartition));
    in.apply(
        "WriteUncapped",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(uncappedId)
            .withSequenceNumberColumn("seq")
            .withNumShards(numShards));
    p.run().waitUntilFinish();

    // Identical, complete contents in identical partitions under both settings.
    List<String> expectedSorted =
        expected.stream().sorted().collect(ImmutableList.toImmutableList());
    assertThat(readColumns(dial, "id", "ts", "name"), equalTo(expectedSorted));
    assertThat(readColumns(uncapped, "id", "ts", "name"), equalTo(expectedSorted));
    assertThat(scannedPartitions(dial, 1), hasSize(numDays));
    assertThat(scannedPartitions(dial, 1), equalTo(scannedPartitions(uncapped, 1)));

    // The measured dial: exactly spp shards per partition, against numShards shards uncapped.
    assertThat(dataFileCount(dial), equalTo(numDays * shardsPerPartition));
    assertThat(dataFileCount(uncapped), equalTo(numDays * numShards));
  }

  /**
   * End-to-end correctness at the tightest cap ({@code shards_per_partition = 1}) over cross-window
   * updates and deletes: the final contents must be identical to the uncapped write of the same
   * stream.
   */
  @Test
  public void shardsPerPartitionOneMatchesPrimaryKeyShardingEndToEnd() throws IOException {
    TableIdentifier affineId = createDayPartitionedTable("pas_e2e_affine");
    TableIdentifier pkId = createDayPartitionedTable("pas_e2e_pk");
    Table affine = catalog.loadTable(affineId);
    Table pk = catalog.loadTable(pkId);
    Schema inputSchema = inputSchemaFor(affine);

    Instant march = Instant.parse("2024-03-15T10:30:00Z");
    Instant april = Instant.parse("2024-04-02T08:00:00Z");
    Instant may = Instant.parse("2024-05-20T23:00:00Z");

    PCollection<Row> in =
        streamingInput(
            inputSchema,
            testStream(inputSchema)
                .advanceWatermarkTo(BASE)
                // Window 1: four inserts across three day partitions.
                .addElements(
                    at(INSERT, row(inputSchema, 1, march, "a", 1L), 1),
                    at(INSERT, row(inputSchema, 2, march, "b", 1L), 1),
                    at(INSERT, row(inputSchema, 3, april, "c", 1L), 2),
                    at(INSERT, row(inputSchema, 4, may, "d", 2L), 2))
                .advanceWatermarkTo(BASE.plus(WINDOW))
                // Window 2: update one March row, delete the other; delete the May row.
                .addElements(
                    at(UPDATE_BEFORE, row(inputSchema, 1, march, "a", 3L), 61),
                    at(UPDATE_AFTER, row(inputSchema, 1, march, "a2", 3L), 61),
                    at(DELETE, row(inputSchema, 2, march, "b", 3L), 62),
                    at(DELETE, row(inputSchema, 4, may, "d", 4L), 62))
                .advanceWatermarkTo(BASE.plus(WINDOW).plus(WINDOW))
                // Window 3: update the April row and re-insert into the emptied May partition.
                .addElements(
                    at(UPDATE_BEFORE, row(inputSchema, 3, april, "c", 5L), 121),
                    at(UPDATE_AFTER, row(inputSchema, 3, april, "c2", 5L), 121),
                    at(INSERT, row(inputSchema, 5, may, "e", 5L), 122))
                .advanceWatermarkToInfinity());

    in.apply(
        "WriteAffine",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(affineId)
            .withSequenceNumberColumn("seq")
            .withTriggeringFrequency(WINDOW)
            .withShardsPerPartition(1));
    in.apply(
        "WritePk",
        IcebergIO.writeCdcRows(catalogConfig())
            .to(pkId)
            .withSequenceNumberColumn("seq")
            .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    List<String> expected =
        ImmutableList.of(
            "1:" + micros(march) + ":a2", "3:" + micros(april) + ":c2", "5:" + micros(may) + ":e");
    assertThat(readColumns(affine, "id", "ts", "name"), equalTo(expected));
    assertThat(readColumns(pk, "id", "ts", "name"), equalTo(expected));

    // Rows physically landed in their day partitions under both modes.
    assertThat(scannedPartitions(affine, 1), equalTo(scannedPartitions(pk, 1)));
    assertThat(
        scannedPartitions(affine, 1),
        containsInAnyOrder(
            String.valueOf((int) LocalDate.of(2024, 3, 15).toEpochDay()),
            String.valueOf((int) LocalDate.of(2024, 4, 2).toEpochDay()),
            String.valueOf((int) LocalDate.of(2024, 5, 20).toEpochDay())));
  }

  // -----------------------------------------------------------------------------------------------
  // 12. DECIMAL and BINARY partition values, through the full sink
  // -----------------------------------------------------------------------------------------------

  /**
   * {@code identity(DECIMAL)} + {@code identity(BINARY)} partitions apply changes end to end: the
   * types the path-rendered predecessor banned outright (base64 out, raw UTF-8 back in). The binary
   * values are deliberately invalid UTF-8 with {@code 0x00} and {@code '/'}, so a byte-lossy
   * round-trip anywhere changes the partition tuple and shows in {@link #scannedPartitions}.
   */
  @Test
  public void decimalAndBinaryPartitionValuesApplyChangesEndToEnd() throws IOException {
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "amount", Types.DecimalType.of(9, 3)),
            Types.NestedField.required(3, "bin", Types.BinaryType.get()),
            Types.NestedField.optional(4, "name", Types.StringType.get()));
    TableIdentifier id =
        createTable(
            "decbinpart",
            icebergSchema,
            ImmutableSet.of(1, 2, 3),
            2,
            PartitionSpec.builderFor(icebergSchema).identity("amount").identity("bin").build());
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    BigDecimal negative = new BigDecimal("-12345.678");
    BigDecimal positive = new BigDecimal("0.001");
    byte[] gnarly = new byte[] {0x00, (byte) 0xFF, 0x2F, (byte) 0x80};
    byte[] plain = new byte[] {0x01, 0x02};

    boundedInput(
            inputSchema,
            KV.of(INSERT, row(inputSchema, 1, negative, gnarly, "a", 1L)),
            KV.of(INSERT, row(inputSchema, 2, positive, plain, "b", 1L)),
            KV.of(INSERT, row(inputSchema, 3, negative, gnarly, "c", 1L)),
            // Update and delete inside the gnarly-binary partition.
            KV.of(UPDATE_BEFORE, row(inputSchema, 1, negative, gnarly, "a", 2L)),
            KV.of(UPDATE_AFTER, row(inputSchema, 1, negative, gnarly, "a2", 2L)),
            KV.of(DELETE, row(inputSchema, 3, negative, gnarly, "c", 2L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(
        readColumns(t, "id", "amount", "name"),
        containsInAnyOrder("1:" + negative + ":a2", "2:" + positive + ":b"));
    // Both partition tuples survived byte-for-byte: the decimal keeps its scale and sign, and the
    // binary keeps its 0x00 / 0xFF / '/' bytes.
    assertThat(
        scannedPartitions(t, 2),
        containsInAnyOrder(negative + ":" + hex(gnarly), positive + ":" + hex(plain)));
  }

  /** Renders a partition value that may be a {@link ByteBuffer} or {@code byte[]} as hex. */
  private static String hex(Object value) {
    byte[] bytes;
    if (value instanceof ByteBuffer) {
      ByteBuffer view = ((ByteBuffer) value).duplicate();
      bytes = new byte[view.remaining()];
      view.get(bytes);
    } else {
      bytes = (byte[]) value;
    }
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  // -----------------------------------------------------------------------------------------------
  // 13. Table CREATED with a sort order
  // -----------------------------------------------------------------------------------------------

  /**
   * A cross-window DELETE commits against a table CREATED with a sort order. The sink's equality
   * deletes carry sort-order id 0 (unsorted), and a table born sorted stores no id 0 at all;
   * resolving the id straight out of {@code table.sortOrders()} threw here, and the committer's
   * rethrow-before-state-write re-fired the identical window forever. Structurally case 3 on a
   * sorted-on-create table; the order is on a NON-key column on purpose.
   */
  @Test
  public void crossWindowDeleteCommitsAgainstTableCreatedWithASortOrder() {
    TableIdentifier tableId = TableIdentifier.of("db", "sortedcreate_" + System.nanoTime());
    CdcSinkTestUtils.createSortedTable(
        catalog,
        tableId,
        ICEBERG_SCHEMA,
        ImmutableSet.of(1),
        2,
        PartitionSpec.unpartitioned(),
        SortOrder.builderFor(ICEBERG_SCHEMA).asc("name").desc("data").build());
    Table t = catalog.loadTable(tableId);

    // The premise: this table has a sort order, and it does NOT have id 0.
    assertThat(t.sortOrder().isSorted(), is(true));
    assertThat(t.sortOrders().keySet(), not(hasItem(SortOrder.unsorted().orderId())));

    TestStream<KV<ValueKind, Row>> stream =
        testStream(INPUT_SCHEMA)
            .advanceWatermarkTo(BASE)
            .addElements(
                at(INSERT, row(1, "a", "x", 1L), 1),
                at(INSERT, row(2, "b", "y", 1L), 2),
                at(INSERT, row(3, "c", "z", 1L), 3))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70))) // window 0 commits
            .addElements(at(DELETE, row(1, "a", "x", 2L), 61))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130))) // window 1 commits
            .advanceWatermarkToInfinity();

    streamingInput(INPUT_SCHEMA, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(tableId)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    // The delete window really committed; a stalled destination leaves one snapshot, three rows.
    assertThat(snapshotCount(t), equalTo(2));
    assertThat(readRows(t), containsInAnyOrder("2:b:y", "3:c:z"));

    // The equality delete landed under the unsorted order, not silently relabelled as the table's.
    List<DeleteFile> deletes = addedDeleteFiles(t);
    assertThat(deletes, not(empty()));
    for (DeleteFile delete : deletes) {
      assertThat(delete.content(), equalTo(FileContent.EQUALITY_DELETES));
      assertThat(delete.sortOrderId(), equalTo(SortOrder.unsorted().orderId()));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // 14. Mid-run partition-spec evolution: the pinned worker keeps writing, correctly.
  // -----------------------------------------------------------------------------------------------

  /** The {@code id}-only primary-key schema of the {@link #DATA_SCHEMA} fixture tables. */
  private static final Schema ID_PK_SCHEMA =
      Schema.builder().addField(DATA_SCHEMA.getField("id")).build();

  /**
   * A sorted-group element for driving {@link WriteDeltas.WriteDeltasFn} directly. The collapse
   * writer blocks on the sort key's pk prefix, so it carries the row's encoded {@code id}, the
   * fixture tables' primary key.
   */
  private static KV<byte[], CdcRecord> cdc(Row data, long seq, ValueKind kind)
      throws CoderException {
    Row pk = Row.withSchema(ID_PK_SCHEMA).addValues(data.getInt32("id")).build();
    byte[] pkBytes = CoderUtils.encodeToByteArray(RowCoder.of(ID_PK_SCHEMA), pk);
    return KV.of(CdcSortKey.encode(pkBytes, seq, kind), CdcRecord.of(data, kind, seq));
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

  /**
   * A worker that resolved before a mid-run spec evolution keeps writing under the pinned spec,
   * equality deletes included, so they reach the earlier window's data. The write stage runs at the
   * {@link WriteDeltas.WriteDeltasFn} level so both windows provably share one worker instance (the
   * pin is per worker); the commit stage and read-back are real.
   */
  @Test
  public void evolvedSpecKeepsWritingUnderPinnedSpec() throws IOException {
    TableIdentifier id =
        createTable(
            "pinnedspec",
            ICEBERG_SCHEMA,
            ImmutableSet.of(1),
            2,
            PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("id", 4).build());
    Table t = catalog.loadTable(id);
    int pinnedSpecId = t.spec().specId();

    CdcWriteConfig config = CdcWriteConfig.builder().setSinkId("pin-sink").build();
    TableSetup setup =
        new TableSetup(
            catalogConfig(), config, DynamicDestinations.singleTable(id, DATA_SCHEMA), "px");
    WriteDeltas.WriteDeltasFn fn = new WriteDeltas.WriteDeltasFn(setup, config, "px", DATA_SCHEMA);

    // Window 1: three inserts, resolved and written under the original spec.
    List<ShardDeltaFiles> w1 = new ArrayList<>();
    fn.process(
        KV.of(
            DestinationShard.of(id.toString(), 0),
            ImmutableList.of(
                cdc(row(DATA_SCHEMA, 1, "a", "x"), 1L, INSERT),
                cdc(row(DATA_SCHEMA, 2, "b", "y"), 1L, INSERT),
                cdc(row(DATA_SCHEMA, 3, "c", "z"), 1L, INSERT))),
        GlobalWindow.INSTANCE,
        CdcSinkTestUtils.collectInto(w1));

    // The operator evolves the partition spec; the worker's shared Table instance sees it.
    TableSetup.Dest dest = setup.get(id.toString(), DATA_SCHEMA);
    dest.table().updateSpec().addField(Expressions.bucket("id", 8)).commit();
    dest.table().refresh();
    assertThat(dest.table().spec().specId(), not(equalTo(pinnedSpecId)));

    // Window 2 through the SAME worker instance: update id=1, delete id=2.
    List<ShardDeltaFiles> w2 = new ArrayList<>();
    fn.process(
        KV.of(
            DestinationShard.of(id.toString(), 0),
            ImmutableList.of(
                cdc(row(DATA_SCHEMA, 1, "a", "x"), 2L, UPDATE_BEFORE),
                cdc(row(DATA_SCHEMA, 1, "a2", "x2"), 2L, UPDATE_AFTER),
                cdc(row(DATA_SCHEMA, 2, "b", "y"), 3L, DELETE))),
        GlobalWindow.INSTANCE,
        CdcSinkTestUtils.collectInto(w2));
    assertThat(w1, hasSize(1));
    assertThat(w2, hasSize(1));

    // Commit both windows, in order, through the real committer.
    TestStream.Builder<ShardDeltaFiles> stream =
        TestStream.create(ShardDeltaFiles.coder()).advanceWatermarkTo(BASE);
    stream =
        stream.addElements(TimestampedValue.of(w1.get(0), BASE.plus(Duration.standardSeconds(1))));
    stream =
        stream.addElements(TimestampedValue.of(w2.get(0), BASE.plus(Duration.standardSeconds(61))));
    PipelineResult result =
        p.apply(stream.advanceWatermarkToInfinity())
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), "pin-sink"))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    // Two windows committed; every committed file carries the pinned original spec id.
    assertThat(snapshotCount(t), equalTo(2));
    for (Snapshot snap : t.snapshots()) {
      for (DataFile file : snap.addedDataFiles(t.io())) {
        assertThat(file.specId(), equalTo(pinnedSpecId));
      }
      for (DeleteFile file : snap.addedDeleteFiles(t.io())) {
        assertThat(file.specId(), equalTo(pinnedSpecId));
      }
    }
    // The pinned-spec equality deletes applied to the pinned-spec data: exact final contents.
    assertThat(readRows(t), containsInAnyOrder("1:a2:x2", "3:c:z"));
    assertThat(committerCounter(result, "specMismatchedWindows"), equalTo(0L));
  }

  // -----------------------------------------------------------------------------------------------
  // 15. A worker joining mid-run after a spec evolution adopts the run's stamped spec.
  // -----------------------------------------------------------------------------------------------

  /**
   * A worker that first resolves AFTER a mid-run spec evolution (a fresh {@link TableSetup} under
   * the same runId) adopts the spec the first commit stamped, so its files still carry the original
   * spec id and its deletes reach the first pass's data. Two pipelines because the stamp must be in
   * the table before the joining worker resolves.
   */
  @Test
  public void joiningWorkerScenarioStaysConsistent() throws IOException {
    TableIdentifier id =
        createTable(
            "joiningworker",
            ICEBERG_SCHEMA,
            ImmutableSet.of(1),
            2,
            PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("id", 4).build());
    Table t = catalog.loadTable(id);
    int stampedSpecId = t.spec().specId();
    String runId = "join-runId";
    CdcWriteConfig config = CdcWriteConfig.builder().setSinkId("join-sink").build();

    // Pass 1: an original worker writes window 1 under the initial spec ...
    TableSetup setup1 =
        new TableSetup(
            catalogConfig(), config, DynamicDestinations.singleTable(id, DATA_SCHEMA), runId);
    WriteDeltas.WriteDeltasFn fn1 =
        new WriteDeltas.WriteDeltasFn(setup1, config, runId, DATA_SCHEMA);
    List<ShardDeltaFiles> w1 = new ArrayList<>();
    fn1.process(
        KV.of(
            DestinationShard.of(id.toString(), 0),
            ImmutableList.of(
                cdc(row(DATA_SCHEMA, 1, "a", "x"), 1L, INSERT),
                cdc(row(DATA_SCHEMA, 2, "b", "y"), 1L, INSERT),
                cdc(row(DATA_SCHEMA, 3, "c", "z"), 1L, INSERT))),
        GlobalWindow.INSTANCE,
        CdcSinkTestUtils.collectInto(w1));

    // ... and the committer commits it, stamping the run's pinned spec into the summary.
    PipelineResult first =
        p.apply(
                TestStream.create(ShardDeltaFiles.coder())
                    .advanceWatermarkTo(BASE)
                    .addElements(
                        TimestampedValue.of(w1.get(0), BASE.plus(Duration.standardSeconds(1))))
                    .advanceWatermarkToInfinity())
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), "join-sink", null, null, runId))
            .getPipeline()
            .run();
    first.waitUntilFinish();

    // The spec evolves after the stamp landed; the process-shared Table sees it.
    Table shared = setup1.get(id.toString(), DATA_SCHEMA).table();
    shared.updateSpec().addField(Expressions.bucket("id", 8)).commit();
    shared.refresh();
    assertThat(shared.spec().specId(), not(equalTo(stampedSpecId)));

    // Pass 2: the joining worker resolves the STAMPED spec and writes window 2 under it:
    // an update and a delete against window 1's rows.
    TableSetup setup2 =
        new TableSetup(
            catalogConfig(), config, DynamicDestinations.singleTable(id, DATA_SCHEMA), runId);
    WriteDeltas.WriteDeltasFn fn2 =
        new WriteDeltas.WriteDeltasFn(setup2, config, runId, DATA_SCHEMA);
    List<ShardDeltaFiles> w2 = new ArrayList<>();
    fn2.process(
        KV.of(
            DestinationShard.of(id.toString(), 0),
            ImmutableList.of(
                cdc(row(DATA_SCHEMA, 1, "a", "x"), 2L, UPDATE_BEFORE),
                cdc(row(DATA_SCHEMA, 1, "a2", "x2"), 2L, UPDATE_AFTER),
                cdc(row(DATA_SCHEMA, 2, "b", "y"), 3L, DELETE))),
        GlobalWindow.INSTANCE,
        CdcSinkTestUtils.collectInto(w2));
    assertThat(setup2.get(id.toString(), DATA_SCHEMA).spec().specId(), equalTo(stampedSpecId));

    TestPipeline second = TestPipeline.create();
    second.enableAbandonedNodeEnforcement(false);
    PipelineResult secondResult =
        second
            .apply(
                TestStream.create(ShardDeltaFiles.coder())
                    .advanceWatermarkTo(BASE)
                    .addElements(
                        TimestampedValue.of(w2.get(0), BASE.plus(Duration.standardSeconds(61))))
                    .advanceWatermarkToInfinity())
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), "join-sink", null, null, runId))
            .getPipeline()
            .run();
    secondResult.waitUntilFinish();

    // Every committed file of both passes carries the stamped spec; no mismatch was flagged.
    t.refresh();
    assertThat(snapshotCount(t), equalTo(2));
    for (Snapshot snap : t.snapshots()) {
      for (DataFile file : snap.addedDataFiles(t.io())) {
        assertThat(file.specId(), equalTo(stampedSpecId));
      }
      for (DeleteFile file : snap.addedDeleteFiles(t.io())) {
        assertThat(file.specId(), equalTo(stampedSpecId));
      }
    }
    assertThat(readRows(t), containsInAnyOrder("1:a2:x2", "3:c:z"));
    assertThat(
        committerCounter(first, "specMismatchedWindows")
            + committerCounter(secondResult, "specMismatchedWindows"),
        equalTo(0L));
  }

  // -----------------------------------------------------------------------------------------------
  // 16. Non-key partitioning: day(ts) with only id as the key
  // -----------------------------------------------------------------------------------------------

  /** {@link #DAY_PARTITIONED_SCHEMA} with {@code day(ts)} partitioning but ONLY {@code id} key. */
  private TableIdentifier createNonKeyDayPartitionedTable(String prefix) {
    return createTable(
        prefix,
        DAY_PARTITIONED_SCHEMA,
        ImmutableSet.of(1),
        2,
        PartitionSpec.builderFor(DAY_PARTITIONED_SCHEMA).day("ts").build());
  }

  private static final Instant MARCH = Instant.parse("2024-03-15T10:30:00Z");
  private static final Instant JUNE = Instant.parse("2024-06-01T09:00:00Z");
  private static final int MARCH_DAY = (int) LocalDate.of(2024, 3, 15).toEpochDay();
  private static final int JUNE_DAY = (int) LocalDate.of(2024, 6, 1).toEpochDay();

  /**
   * On a {@code day(ts)}-partitioned table whose key is {@code id} alone, an update that moves a
   * row across partitions within one window deletes it from the partition it actually occupied: a
   * delete routed by the after-image would land in the new partition and leave the old row alive
   * beside the new one.
   */
  @Test
  public void nonKeyPartitionedMoveDeletesTheRowFromItsOldPartition() throws IOException {
    TableIdentifier id = createNonKeyDayPartitionedTable("nonkeymove");
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    TestStream<KV<ValueKind, Row>> stream =
        testStream(inputSchema)
            .advanceWatermarkTo(BASE)
            .addElements(
                at(INSERT, row(inputSchema, 1, MARCH, "a", 1L), 1),
                at(INSERT, row(inputSchema, 2, MARCH, "b", 1L), 2))
            .advanceWatermarkTo(BASE.plus(WINDOW)) // window 1 commits both March rows
            .addElements(
                at(UPDATE_BEFORE, row(inputSchema, 1, MARCH, "a", 2L), 61),
                at(UPDATE_AFTER, row(inputSchema, 1, JUNE, "a2", 2L), 61))
            .advanceWatermarkToInfinity();

    streamingInput(inputSchema, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    assertThat(snapshotCount(t), equalTo(2));
    assertThat(
        readColumns(t, "id", "ts", "name"),
        containsInAnyOrder("1:" + micros(JUNE) + ":a2", "2:" + micros(MARCH) + ":b"));

    // Physical corroboration: March still holds id=2's live file, June holds the moved row, and
    // window 2's only delete file sits in MARCH, where the old row lives.
    assertThat(
        scannedPartitions(t, 1),
        containsInAnyOrder(String.valueOf(MARCH_DAY), String.valueOf(JUNE_DAY)));
    List<DeleteFile> deletes = addedDeleteFiles(t);
    assertThat(deletes, hasSize(1));
    assertThat(deletes.get(0).content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(deletes.get(0).partition().get(0, Integer.class), equalTo(MARCH_DAY));
  }

  /**
   * A move whose {@code UPDATE_BEFORE} and {@code UPDATE_AFTER} land in DIFFERENT commit windows
   * still converges: the bare-UB window commits the equality delete in the old partition, the
   * bare-UA window commits the row in the new one. This is why a bare-UA block must write without
   * being rejected: its delete can already sit in an earlier commit.
   */
  @Test
  public void nonKeyPartitionedMoveSplitAcrossWindowsConverges() {
    TableIdentifier id = createNonKeyDayPartitionedTable("nonkeysplit");
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    TestStream<KV<ValueKind, Row>> stream =
        testStream(inputSchema)
            .advanceWatermarkTo(BASE)
            .addElements(at(INSERT, row(inputSchema, 1, MARCH, "a", 1L), 1))
            .advanceWatermarkTo(BASE.plus(WINDOW)) // window 1 commits the March row
            .addElements(at(UPDATE_BEFORE, row(inputSchema, 1, MARCH, "a", 2L), 61))
            .advanceWatermarkTo(BASE.plus(WINDOW).plus(WINDOW)) // window 2: the bare UB
            .addElements(at(UPDATE_AFTER, row(inputSchema, 1, JUNE, "a2", 2L), 121))
            .advanceWatermarkToInfinity(); // window 3: the bare UA

    streamingInput(inputSchema, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    t.refresh();
    assertThat(readColumns(t, "id", "ts", "name"), contains("1:" + micros(JUNE) + ":a2"));

    // The per-commit file trail: data at March; the bare UB's delete at March; data at June.
    List<Snapshot> snapshots =
        Lists.newArrayList(t.snapshots()).stream()
            .sorted(Comparator.comparingLong(Snapshot::sequenceNumber))
            .collect(Collectors.toList());
    assertThat(snapshots, hasSize(3));

    List<DataFile> w1Data = Lists.newArrayList(snapshots.get(0).addedDataFiles(t.io()));
    assertThat(w1Data, hasSize(1));
    assertThat(w1Data.get(0).partition().get(0, Integer.class), equalTo(MARCH_DAY));

    assertThat(Lists.newArrayList(snapshots.get(1).addedDataFiles(t.io())), empty());
    List<DeleteFile> w2Deletes = Lists.newArrayList(snapshots.get(1).addedDeleteFiles(t.io()));
    assertThat(w2Deletes, hasSize(1));
    assertThat(w2Deletes.get(0).content(), equalTo(FileContent.EQUALITY_DELETES));
    assertThat(w2Deletes.get(0).partition().get(0, Integer.class), equalTo(MARCH_DAY));

    assertThat(Lists.newArrayList(snapshots.get(2).addedDeleteFiles(t.io())), empty());
    List<DataFile> w3Data = Lists.newArrayList(snapshots.get(2).addedDataFiles(t.io()));
    assertThat(w3Data, hasSize(1));
    assertThat(w3Data.get(0).partition().get(0, Integer.class), equalTo(JUNE_DAY));
  }

  /**
   * The bare-UA outcome on a non-key-partitioned table, pinned as intended behavior: without its
   * {@code UPDATE_BEFORE} (the input contract for such tables, see the package-info partitioning
   * section), a moved row cannot be deleted from the partition it occupied, so the old image stays
   * live beside the new one and no delete file is written.
   */
  @Test
  public void nonKeyPartitionedBareUpdateAfterLeavesTheOldRowLive() throws IOException {
    TableIdentifier id = createNonKeyDayPartitionedTable("nonkeybareua");
    Table t = catalog.loadTable(id);
    Schema inputSchema = inputSchemaFor(t);

    TestStream<KV<ValueKind, Row>> stream =
        testStream(inputSchema)
            .advanceWatermarkTo(BASE)
            .addElements(at(INSERT, row(inputSchema, 1, MARCH, "a", 1L), 1))
            .advanceWatermarkTo(BASE.plus(WINDOW)) // window 1 commits the March row
            .addElements(at(UPDATE_AFTER, row(inputSchema, 1, JUNE, "a2", 2L), 61))
            .advanceWatermarkToInfinity(); // window 2: the bare UA

    streamingInput(inputSchema, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    assertThat(
        readColumns(t, "id", "ts", "name"),
        containsInAnyOrder("1:" + micros(MARCH) + ":a", "1:" + micros(JUNE) + ":a2"));
    assertThat(
        scannedPartitions(t, 1),
        containsInAnyOrder(String.valueOf(MARCH_DAY), String.valueOf(JUNE_DAY)));
    assertThat(addedDeleteFiles(t), empty());
  }

  // -----------------------------------------------------------------------------------------------
  // 17. One runId end to end
  // -----------------------------------------------------------------------------------------------

  /**
   * The committed run-spec stamp, the writer's file names, and spec adoption agree on one runId:
   * the stamp's runId prefix is the runId embedded in the committed data file names, and a {@link
   * TableSetup} built with it adopts the stamped spec over a live evolution. Fails if {@code
   * WriteCdcRows.expand()} hands its stages different runIds.
   */
  @Test
  public void runIdThreadsIdenticallyThroughTheAssembledTransform() {
    TableIdentifier id =
        createTable(
            "runid",
            ICEBERG_SCHEMA,
            ImmutableSet.of(1),
            2,
            PartitionSpec.builderFor(ICEBERG_SCHEMA).bucket("id", 4).build());
    Table t = catalog.loadTable(id);

    boundedInput(INPUT_SCHEMA, KV.of(INSERT, row(1, "a", "x", 1L)))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId("runid-sink"));
    p.run().waitUntilFinish();

    t.refresh();
    Snapshot committed = checkStateNotNull(t.currentSnapshot());
    String stamp = checkStateNotNull(committed.summary()).get("beam.cdc.run-spec.runid-sink");
    assertThat(stamp, notNullValue());
    int cut = stamp.lastIndexOf(':');
    String runId = stamp.substring(0, cut);
    int stampedSpecId = Integer.parseInt(stamp.substring(cut + 1));

    // The commit stage's stamped runId is the writer stage's runId, part of every file name.
    List<DataFile> dataFiles = Lists.newArrayList(committed.addedDataFiles(t.io()));
    assertThat(dataFiles, not(empty()));
    for (DataFile file : dataFiles) {
      assertThat(file.location(), containsString(runId));
    }

    // A TableSetup built with the same runId (the assign/write stages' construction) adopts the
    // stamped spec over the live evolved one.
    t.updateSpec().addField(Expressions.bucket("id", 8)).commit();
    t.refresh();
    assertThat(t.spec().specId(), not(equalTo(stampedSpecId)));
    TableSetup joining =
        new TableSetup(
            catalogConfig(),
            CdcWriteConfig.builder().setSinkId("runid-sink").setSequenceNumberColumn("seq").build(),
            DynamicDestinations.singleTable(id, DATA_SCHEMA),
            runId);
    assertThat(joining.get(id.toString(), DATA_SCHEMA).spec().specId(), equalTo(stampedSpecId));
  }
}
