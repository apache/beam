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
package org.apache.beam.sdk.io.iceberg.catalog;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.ValueKind.DELETE;
import static org.apache.beam.sdk.values.ValueKind.INSERT;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_AFTER;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_BEFORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeTrue;

import com.google.api.services.storage.model.StorageObject;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.io.iceberg.cdc.sink.CdcSinkTestUtils;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end acceptance tests for the assembled CDC sink against a real catalog and warehouse, read
 * back via {@link IcebergGenerics#read} as ground truth. Subclasses supply the catalog, the same
 * way {@link IcebergCatalogBaseIT} does, so every test runs against each supported catalog. Named
 * {@code *IT}: runs under {@code integrationTest}, not the fast {@code test} suite.
 *
 * <p>Genuinely covered: real catalogs and warehouses (every referenced file must exist), cross-run
 * token recovery (two sequential pipelines sharing one {@code sink_id}, the second rebuilding
 * progress purely from snapshot ancestry), ordering across many commits with a foreign snapshot
 * interleaved, and the round trip through the CDC source.
 *
 * <p>NOT covered here (needs a runner honoring {@code @RequiresStableInput}, Dataflow or Flink in
 * exactly-once mode): bundle retry mid-commit (the double-commit window the token closes; the
 * DirectRunner never retries bundles, so treat this class as an assembly gate, not proof of
 * exactly-once); backlog draining on a portable runner; pipeline update/drain with in-flight file
 * metadata in committer state; Iceberg's optimistic-concurrency retry (the foreign snapshot lands
 * BETWEEN the sink's commits, so no genuine {@code CommitFailedException} refresh-and-retry runs).
 *
 * <p>The {@link TestStream} cases run on the DirectRunner only. The source round trips also run on
 * Dataflow through {@code dataflowIntegrationTest}: the native-ValueKind variant on the legacy
 * worker, since Runner v2 does not carry element ValueKinds yet, and the change-type-column variant
 * on Runner v2.
 */
public abstract class IcebergCdcWriteBaseIT implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCdcWriteBaseIT.class);
  private static final long SETUP_TEARDOWN_SLEEP_MS = 5000;
  private static final String RANDOM = UUID.randomUUID().toString();

  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);

  /** The catalog under test; the sink reaches it through {@link #managedIcebergConfig}. */
  public abstract Catalog createCatalog();

  /** The Managed-style config for {@code tableId}: catalog name, properties, and Hadoop config. */
  public abstract Map<String, Object> managedIcebergConfig(String tableId);

  public abstract String type();

  public static String warehouse(Class<? extends IcebergCdcWriteBaseIT> testClass) {
    return String.format(
        "%s/%s/%s",
        TestPipeline.testingPipelineOptions().getTempLocation(), testClass.getSimpleName(), RANDOM);
  }

  /** Whether {@code warehouse} lives on GCS; local warehouses skip the consistency sleeps. */
  static boolean isGcs(String warehouse) {
    return warehouse.startsWith("gs://");
  }

  /** The Iceberg FileIO for {@code warehouse}: GCS-native on GCS, Hadoop's otherwise. */
  static String ioImplFor(String warehouse) {
    return isGcs(warehouse)
        ? "org.apache.iceberg.gcp.gcs.GCSFileIO"
        : "org.apache.iceberg.hadoop.HadoopFileIO";
  }

  protected static String warehouse;
  public Catalog catalog;
  public String catalogName = type() + "_cdc_test_catalog_" + System.currentTimeMillis();
  private final List<String> namespacesToCleanup = new ArrayList<>();

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TestName testName = new TestName();

  @Rule
  public transient Timeout globalTimeout =
      Timeout.seconds(OPTIONS.getRunner().equals(DirectRunner.class) ? 300 : 20 * 60);

  /** Canonical test table schema, shared with the {@code cdc/sink} unit suites. */
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

  /** Event-time origin for the streaming cases. */
  private static final Instant BASE = new Instant(0);

  private static final Duration WINDOW = Duration.standardSeconds(60);

  @Before
  public void setUp() throws Exception {
    OPTIONS.as(DirectOptions.class).setTargetParallelism(1);
    warehouse = warehouse(getClass());
    catalog = createCatalog();
    namespacesToCleanup.add(namespace());
    if (catalog instanceof SupportsNamespaces) {
      ((SupportsNamespaces) catalog).createNamespace(Namespace.of(namespace()));
    }
    if (isGcs(warehouse)) {
      Thread.sleep(SETUP_TEARDOWN_SLEEP_MS);
    }
  }

  @After
  public void cleanUp() throws Exception {
    for (String namespaceName : namespacesToCleanup) {
      Namespace namespace = Namespace.of(namespaceName);
      for (TableIdentifier identifier : catalog.listTables(namespace)) {
        catalog.dropTable(identifier);
      }
      if (catalog instanceof SupportsNamespaces) {
        ((SupportsNamespaces) catalog).dropNamespace(namespace);
      }
    }
    LOG.info("Cleaned up namespaces: {}", namespacesToCleanup);
    if (!isGcs(warehouse)) {
      return;
    }
    Thread.sleep(SETUP_TEARDOWN_SLEEP_MS);
    try {
      GcsUtil gcsUtil = OPTIONS.as(GcsOptions.class).getGcsUtil();
      GcsPath path = GcsPath.fromUri(warehouse);
      @Nullable List<StorageObject> objects =
          gcsUtil
              .listObjects(
                  path.getBucket(),
                  getClass().getSimpleName() + "/" + path.getFileName().toString(),
                  null)
              .getItems();
      // A catalog's cleanup sometimes removes every file; delete whatever is left.
      if (objects != null) {
        gcsUtil.remove(
            objects.stream()
                .map(obj -> "gs://" + path.getBucket() + "/" + obj.getName())
                .collect(Collectors.toList()));
      }
    } catch (Exception e) {
      LOG.warn("Failed to clean up GCS files.", e);
    }
  }

  public String namespace() {
    return catalogName + "_" + testName.getMethodName();
  }

  /** The sink's catalog config, from the same map the Managed tests use. */
  @SuppressWarnings("unchecked")
  protected IcebergCatalogConfig catalogConfig() {
    Map<String, Object> config = managedIcebergConfig("unused.table");
    return IcebergCatalogConfig.builder()
        .setCatalogName((String) config.get("catalog_name"))
        .setCatalogProperties((Map<String, String>) config.get("catalog_properties"))
        .setConfigProperties((Map<String, String>) config.get("config_properties"))
        .build();
  }

  // -----------------------------------------------------------------------------------------------
  // Fixtures
  // -----------------------------------------------------------------------------------------------

  /** Creates a uniquely named table in this test's namespace and returns its identifier. */
  private TableIdentifier createTable(
      String prefix,
      org.apache.iceberg.Schema schema,
      Set<Integer> identifierFieldIds,
      int formatVersion,
      PartitionSpec spec) {
    TableIdentifier id = TableIdentifier.of(namespace(), prefix + "_" + System.nanoTime());
    org.apache.iceberg.Schema schemaWithIds =
        new org.apache.iceberg.Schema(schema.columns(), identifierFieldIds);
    catalog.createTable(
        id, schemaWithIds, spec, ImmutableMap.of("format-version", String.valueOf(formatVersion)));
    return id;
  }

  /** A uniquely named unpartitioned table over {@link #ICEBERG_SCHEMA} (PK = {@code id}). */
  private TableIdentifier createCanonicalTable(String prefix, int formatVersion) {
    return createTable(
        prefix, ICEBERG_SCHEMA, ImmutableSet.of(1), formatVersion, PartitionSpec.unpartitioned());
  }

  /** Builds an input row over the canonical {@link #INPUT_SCHEMA}. */
  private static Row row(int id, String name, String data, long seq) {
    return Row.withSchema(INPUT_SCHEMA).addValues(id, name, data, seq).build();
  }

  private static KvCoder<ValueKind, Row> taggedRowCoder(Schema schema) {
    return KvCoder.of(SerializableCoder.of(ValueKind.class), RowCoder.of(schema));
  }

  private static TestStream.Builder<KV<ValueKind, Row>> testStream() {
    return TestStream.create(taggedRowCoder(INPUT_SCHEMA));
  }

  private static TimestampedValue<KV<ValueKind, Row>> at(ValueKind kind, Row row, int atSeconds) {
    return TimestampedValue.of(KV.of(kind, row), BASE.plus(Duration.standardSeconds(atSeconds)));
  }

  /** A bounded CDC input of the given kind-tagged rows, read by {@code pipeline}. */
  @SafeVarargs
  private static PCollection<Row> boundedInput(TestPipeline pipeline, KV<ValueKind, Row>... rows) {
    return CdcSinkTestUtils.withKinds(
            pipeline.apply(
                Create.of(ImmutableList.copyOf(rows)).withCoder(taggedRowCoder(INPUT_SCHEMA))))
        .setRowSchema(INPUT_SCHEMA);
  }

  /** An unbounded CDC input driven by {@code stream}, read by {@code pipeline}. */
  private static PCollection<Row> streamingInput(
      TestPipeline pipeline, TestStream<KV<ValueKind, Row>> stream) {
    return CdcSinkTestUtils.withKinds(pipeline.apply(stream)).setRowSchema(INPUT_SCHEMA);
  }

  /**
   * A second, independently constructed pipeline for the restart cases; enforcement is switched off
   * because it is built and run inline, outside the {@code @Rule} machinery.
   */
  private static TestPipeline restartPipeline() {
    return restartPipeline(TestPipeline.testingPipelineOptions());
  }

  private static TestPipeline restartPipeline(PipelineOptions options) {
    TestPipeline pipeline = TestPipeline.fromOptions(options);
    pipeline.enableAbandonedNodeEnforcement(false);
    return pipeline;
  }

  // -----------------------------------------------------------------------------------------------
  // Ground truth: read the committed table back
  // -----------------------------------------------------------------------------------------------

  /** Every live row as sorted {@code "id:name:data"} strings: the whole table, comparable. */
  private static List<String> readRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(
            record ->
                record.getField("id")
                    + ":"
                    + record.getField("name")
                    + ":"
                    + record.getField("data"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  private static List<Snapshot> snapshotsOf(Table table) {
    table.refresh();
    return Lists.newArrayList(table.snapshots());
  }

  /** This sink's tokens in snapshot order; foreign snapshots carry none and are skipped. */
  private static List<Long> committedThroughTokens(Table table, String sinkId) {
    List<Long> tokens = new ArrayList<>();
    for (Snapshot snapshot : snapshotsOf(table)) {
      String value = snapshot.summary().get("beam.cdc.committed-through-ms." + sinkId);
      if (value != null) {
        tokens.add(Long.parseLong(value));
      }
    }
    return tokens;
  }

  /** Non-empty and strictly ascending: commits landed in window order. */
  private static void assertStrictlyAscending(List<Long> tokens) {
    assertThat(tokens, not(empty()));
    for (int i = 1; i < tokens.size(); i++) {
      assertThat(tokens.get(i - 1), lessThan(tokens.get(i)));
    }
  }

  /** The data files added by every snapshot of {@code table}, in snapshot order. */
  private static List<DataFile> allAddedDataFiles(Table table) {
    List<DataFile> files = new ArrayList<>();
    for (Snapshot snapshot : snapshotsOf(table)) {
      Lists.newArrayList(snapshot.addedDataFiles(table.io())).forEach(files::add);
    }
    return files;
  }

  private static List<DeleteFile> addedDeleteFiles(Table table) {
    table.refresh();
    return Lists.newArrayList(
        checkStateNotNull(table.currentSnapshot()).addedDeleteFiles(table.io()));
  }

  /** Total committed value of the committer counter named {@code name}. */
  private static long committerCounter(PipelineResult result, String name) {
    Iterable<MetricResult<Long>> counters =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(CdcSinkTestUtils.COMMITTER_METRICS_NAMESPACE, name))
                    .build())
            .getCounters();
    long total = 0;
    for (MetricResult<Long> counter : counters) {
      total += counter.getCommitted();
    }
    return total;
  }

  // -----------------------------------------------------------------------------------------------
  // The warehouse
  // -----------------------------------------------------------------------------------------------

  /** Every file the committed snapshots reference really exists in the warehouse. */
  private static void assertWarehouseIsClean(Table table) {
    // Guards against passing vacuously: the current metadata exists and files were added.
    String metadataLocation =
        ((HasTableOperations) table).operations().current().metadataFileLocation();
    assertThat(table.io().newInputFile(metadataLocation).exists(), is(true));
    List<DataFile> added = allAddedDataFiles(table);
    assertThat(added, not(empty()));

    for (DataFile file : added) {
      assertThat(
          "committed data file missing from the warehouse: " + file.location(),
          table.io().newInputFile(file.location()).exists(),
          is(true));
    }
  }

  // -----------------------------------------------------------------------------------------------
  // 1. Format-version-3 end to end, against a real warehouse
  // -----------------------------------------------------------------------------------------------

  /**
   * The whole sink on a V3 table in a real warehouse: insert/update/delete resolve correctly, the
   * same-window churn collapses in the writer so the commit adds no delete files at all, every
   * committed file exists in the warehouse.
   */
  @Test
  public void v3EndToEndAppliesInsertUpdateDelete() {
    TableIdentifier id = createCanonicalTable("v3e2e", 3);
    Table t = catalog.loadTable(id);

    boundedInput(
            p,
            KV.of(INSERT, row(1, "a", "x", 1L)),
            KV.of(UPDATE_BEFORE, row(1, "a", "x", 2L)),
            KV.of(UPDATE_AFTER, row(1, "a2", "z", 2L)),
            KV.of(INSERT, row(2, "b", "y", 1L)),
            KV.of(DELETE, row(2, "b", "y", 2L)),
            KV.of(INSERT, row(3, "c", "w", 1L)))
        .apply(IcebergIO.writeCdcRows(catalogConfig()).to(id).withSequenceNumberColumn("seq"));
    p.run().waitUntilFinish();

    assertThat(readRows(t), containsInAnyOrder("1:a2:z", "3:c:w"));

    // Every change is same-window churn: the collapse resolves it before any file is written,
    // and the sink writes no delete file beyond a cross-window equality delete (none here).
    assertThat(addedDeleteFiles(t), empty());

    assertWarehouseIsClean(t);
  }

  // -----------------------------------------------------------------------------------------------
  // 2. Streaming across several commit windows
  // -----------------------------------------------------------------------------------------------

  /**
   * Three event-time windows produce three snapshots with strictly ascending tokens, and the final
   * contents are the three windows applied in sequence.
   */
  @Test
  public void streamingMultiWindowCommitsWindowsInOrder() {
    assumeDirectRunner();
    TableIdentifier id = createCanonicalTable("streame2e", 2);
    Table t = catalog.loadTable(id);
    String sinkId = "sink-" + System.nanoTime();

    TestStream<KV<ValueKind, Row>> stream =
        testStream()
            .advanceWatermarkTo(BASE)
            // Window 0 = [0s, 60s): insert ids 1 and 2.
            .addElements(at(INSERT, row(1, "a", "x", 1L), 1), at(INSERT, row(2, "b", "y", 1L), 2))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70)))
            // Window 1 = [60s, 120s): update id 1, delete id 2.
            .addElements(
                at(UPDATE_BEFORE, row(1, "a", "x", 2L), 61),
                at(UPDATE_AFTER, row(1, "a2", "z2", 2L), 61),
                at(DELETE, row(2, "b", "y", 2L), 62))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130)))
            // Window 2 = [120s, 180s): update id 1 again, insert id 3.
            .addElements(
                at(UPDATE_BEFORE, row(1, "a2", "z2", 3L), 121),
                at(UPDATE_AFTER, row(1, "a3", "z3", 3L), 121),
                at(INSERT, row(3, "c", "w", 3L), 122))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(190)))
            .advanceWatermarkToInfinity();

    streamingInput(p, stream)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId)
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    // One snapshot per window, each carrying a token strictly newer than the previous one.
    assertThat(snapshotsOf(t), hasSize(3));
    List<Long> tokens = committedThroughTokens(t, sinkId);
    assertThat(tokens, hasSize(3));
    assertStrictlyAscending(tokens);

    assertThat(readRows(t), containsInAnyOrder("1:a3:z3", "3:c:w"));
  }

  // -----------------------------------------------------------------------------------------------
  // 3. Restart: a second pipeline resumes from the token in the table
  // -----------------------------------------------------------------------------------------------

  /**
   * The stream both restart runs replay; {@code throughSeconds} decides how far it goes. The
   * windows are shaped so re-applying window 0 or 1 is plainly visible in the final contents (id 2
   * comes back, or id 1 reverts).
   */
  private static TestStream<KV<ValueKind, Row>> restartStream(int throughSeconds) {
    TestStream.Builder<KV<ValueKind, Row>> stream =
        testStream()
            .advanceWatermarkTo(BASE)
            .addElements(at(INSERT, row(1, "a", "x", 1L), 1), at(INSERT, row(2, "b", "y", 1L), 2))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70)))
            .addElements(
                at(UPDATE_BEFORE, row(1, "a", "x", 2L), 61),
                at(UPDATE_AFTER, row(1, "a2", "z2", 2L), 61),
                at(DELETE, row(2, "b", "y", 2L), 62))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130)));
    if (throughSeconds >= 180) {
      stream =
          stream
              .addElements(
                  at(UPDATE_BEFORE, row(1, "a2", "z2", 3L), 121),
                  at(UPDATE_AFTER, row(1, "a3", "z3", 3L), 121),
                  at(INSERT, row(3, "c", "w", 3L), 122))
              .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(190)));
    }
    return stream.advanceWatermarkToInfinity();
  }

  /**
   * Two sequential pipelines sharing one {@code sink_id} behave like a restart: the second starts
   * with empty runner state and rebuilds progress purely from snapshot ancestry. Both replayed
   * windows are skipped (no double apply) while the new window still commits (no gap): together,
   * the whole restart contract.
   */
  @Test
  public void restartWithStableSinkIdResumesWithoutDoubleApply() {
    assumeDirectRunner();
    TableIdentifier id = createCanonicalTable("restart", 2);
    Table t = catalog.loadTable(id);
    String sinkId = "stable-sink-" + System.nanoTime();

    // Run 1: windows 0 and 1.
    streamingInput(p, restartStream(130))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId)
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(2));
    assertThat(readRows(t), containsInAnyOrder("1:a2:z2"));
    List<Long> tokensAfterRun1 = committedThroughTokens(t, sinkId);
    assertThat(tokensAfterRun1, hasSize(2));

    // Run 2: the restart. Same sink id, the same two windows replayed, plus a new window 2.
    TestPipeline restarted = restartPipeline();
    streamingInput(restarted, restartStream(190))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId)
                .withTriggeringFrequency(WINDOW));
    PipelineResult restartResult = restarted.run();
    restartResult.waitUntilFinish();

    // No double apply: both replayed windows skipped, exactly one new snapshot.
    assertThat(committerCounter(restartResult, "alreadyCommittedWindowsSkipped"), equalTo(2L));
    assertThat(snapshotsOf(t), hasSize(3));

    // No gap: window 2 committed strictly after the first run's tokens.
    List<Long> tokens = committedThroughTokens(t, sinkId);
    assertThat(tokens, hasSize(3));
    assertStrictlyAscending(tokens);
    assertThat(tokens.subList(0, 2), equalTo(tokensAfterRun1));

    // Window 2's result, not a re-application: id 2 stayed deleted, id 1 carries window 2's image.
    assertThat(readRows(t), containsInAnyOrder("1:a3:z3", "3:c:w"));
  }

  // -----------------------------------------------------------------------------------------------
  // 4. A foreign writer's snapshot between the sink's commits
  // -----------------------------------------------------------------------------------------------

  /**
   * Commits one row as an unrelated writer would: an independent append with none of the sink's
   * tokens. Stands in for a compaction or a second ingestion job.
   */
  private static void foreignAppend(Table table, int id, String name, String data)
      throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("name", name);
    record.setField("data", data);

    DataWriter<Record> writer =
        new GenericAppenderFactory(table.schema(), table.spec())
            .newDataWriter(
                EncryptedFiles.plainAsEncryptedOutput(
                    table
                        .io()
                        .newOutputFile(
                            table.location() + "/data/foreign-" + System.nanoTime() + ".parquet")),
                FileFormat.PARQUET,
                null);
    try {
      writer.write(record);
    } finally {
      writer.close();
    }
    table.newAppend().appendFile(writer.toDataFile()).commit();
  }

  /**
   * A foreign writer commits between the sink's own commits: recovery walks the ancestry past the
   * foreign snapshot, the replayed windows skip, the new window commits, and both writers' rows
   * survive. The assembled-pipeline counterpart of {@code
   * CommitDeltasTest#recoversTokenBehindForeignCommitAndCommitsNextWindow}.
   */
  @Test
  public void foreignCommitBetweenSinkCommitsPreservesTokenRecovery() throws IOException {
    assumeDirectRunner();
    TableIdentifier id = createCanonicalTable("foreign", 2);
    Table t = catalog.loadTable(id);
    String sinkId = "stable-sink-" + System.nanoTime();

    // The sink's first commit: window 0 only.
    streamingInput(p, restartStream(130))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId)
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();
    assertThat(snapshotsOf(t), hasSize(2));

    // A foreign writer commits on top: the current snapshot now has no token of ours.
    foreignAppend(t, 100, "other-writer", "kept");
    assertThat(snapshotsOf(t), hasSize(3));
    Snapshot foreign = snapshotsOf(t).get(2);
    assertThat(foreign.summary().get("beam.cdc.committed-through-ms." + sinkId), nullValue());

    // The sink restarts behind the foreign snapshot: windows 0 and 1 replay, window 2 is new.
    TestPipeline restarted = restartPipeline();
    streamingInput(restarted, restartStream(190))
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withSinkId(sinkId)
                .withTriggeringFrequency(WINDOW));
    PipelineResult restartResult = restarted.run();
    restartResult.waitUntilFinish();

    // The ancestry scan found the token behind the foreign snapshot: the replayed windows were
    // skipped and only window 2 committed.
    assertThat(committerCounter(restartResult, "alreadyCommittedWindowsSkipped"), equalTo(2L));
    assertThat(snapshotsOf(t), hasSize(4));

    // Ordering held across the foreign commit.
    assertStrictlyAscending(committedThroughTokens(t, sinkId));

    // Neither writer lost anything: the sink's CDC result plus the foreign writer's row.
    assertThat(readRows(t), containsInAnyOrder("1:a3:z3", "3:c:w", "100:other-writer:kept"));
  }

  // -----------------------------------------------------------------------------------------------
  // 5. Partitioned table, end to end
  // -----------------------------------------------------------------------------------------------

  /**
   * A {@code bucket(8, id)}-partitioned table through the whole pipeline: contents correct AND
   * every added file carries the partition value its rows imply, the cross-window equality deletes
   * included, which must sit in the same partitions as the data they remove.
   */
  @Test
  public void partitionedEndToEndRoutesRowsAndDeletesIntoTheirBuckets() {
    assumeDirectRunner();
    org.apache.iceberg.Schema schemaWithIds =
        new org.apache.iceberg.Schema(ICEBERG_SCHEMA.columns(), ImmutableSet.of(1));
    PartitionSpec spec = PartitionSpec.builderFor(schemaWithIds).bucket("id", 8).build();
    TableIdentifier id = createTable("partitioned", ICEBERG_SCHEMA, ImmutableSet.of(1), 2, spec);
    Table t = catalog.loadTable(id);

    List<KV<ValueKind, Row>> window0 = new ArrayList<>();
    for (int key = 1; key <= 6; key++) {
      window0.add(KV.of(INSERT, row(key, "n" + key, "d" + key, 1L)));
    }

    TestStream.Builder<KV<ValueKind, Row>> stream = testStream().advanceWatermarkTo(BASE);
    for (KV<ValueKind, Row> element : window0) {
      stream = stream.addElements(at(element.getKey(), element.getValue(), 1));
    }
    TestStream<KV<ValueKind, Row>> withSecondWindow =
        stream
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(70)))
            // Window 1: delete id 3, update id 5, both reaching back into window 0's partitions.
            .addElements(
                at(DELETE, row(3, "n3", "d3", 2L), 61),
                at(UPDATE_BEFORE, row(5, "n5", "d5", 2L), 62),
                at(UPDATE_AFTER, row(5, "n5b", "d5b", 2L), 62))
            .advanceWatermarkTo(BASE.plus(Duration.standardSeconds(130)))
            .advanceWatermarkToInfinity();

    streamingInput(p, withSecondWindow)
        .apply(
            IcebergIO.writeCdcRows(catalogConfig())
                .to(id)
                .withSequenceNumberColumn("seq")
                .withTriggeringFrequency(WINDOW));
    p.run().waitUntilFinish();

    assertThat(
        readRows(t), containsInAnyOrder("1:n1:d1", "2:n2:d2", "4:n4:d4", "5:n5b:d5b", "6:n6:d6"));

    // Every id that was ever written mapped to some bucket; the files the sink added must carry
    // exactly those bucket values and no others.
    SerializableFunction<Integer, Integer> bucketOf =
        Transforms.<Integer>bucket(8).bind(Types.IntegerType.get());
    Set<Integer> expectedBuckets = new TreeSet<>();
    for (int key = 1; key <= 6; key++) {
      expectedBuckets.add(bucketOf.apply(key));
    }

    List<DataFile> dataFiles = allAddedDataFiles(t);
    assertThat(dataFiles, not(empty()));
    Set<Integer> actualBuckets = new TreeSet<>();
    for (DataFile file : dataFiles) {
      assertThat(file.specId(), equalTo(spec.specId()));
      assertThat(file.partition().size(), equalTo(1));
      Integer bucket = file.partition().get(0, Integer.class);
      assertThat("data file has no partition value: " + file.location(), bucket, notNullValue());
      actualBuckets.add(bucket);
    }
    assertThat(actualBuckets, equalTo(expectedBuckets));

    // Window 1's deletes reached back into window 0's data: the equality deletes (the only
    // delete files the sink writes) are partitioned too, and sit in exactly the buckets of the
    // keys they touch (ids 3 and 5).
    Set<Integer> deleteBuckets = new TreeSet<>();
    for (DeleteFile file : addedDeleteFiles(t)) {
      assertThat(file.content(), equalTo(FileContent.EQUALITY_DELETES));
      assertThat(file.partition().size(), equalTo(1));
      Integer bucket = file.partition().get(0, Integer.class);
      assertThat("delete file has no partition value: " + file.location(), bucket, notNullValue());
      deleteBuckets.add(bucket);
    }
    assertThat(deleteBuckets, equalTo(ImmutableSet.of(bucketOf.apply(3), bucketOf.apply(5))));
  }

  // -----------------------------------------------------------------------------------------------
  // 6. Round trip through the CDC source: sink -> source -> sink
  // -----------------------------------------------------------------------------------------------

  /**
   * Writes changes of every kind to table A over three commits, then reads back table A's changelog
   * with the Managed CDC source and applies those changes to table B with a second Managed sink in
   * merge-on-read mode. The source applies native element metadata ValueKinds so no need to set a
   * change_type_column. For sequence column, we use the default {@code
   * _commit_snapshot_sequence_number} coming from the source.
   */
  @Test
  public void changelogOfSinkWrittenTableRoundTripsThroughTheSource() throws Exception {
    roundTripThroughSource(/* upsert= */ false, /* changeTypeColumn= */ false);
  }

  /** The same round trip with the second sink in upsert mode, which drops the before-images. */
  @Test
  public void changelogOfSinkWrittenTableRoundTripsThroughTheSourceWithUpsert() throws Exception {
    roundTripThroughSource(/* upsert= */ true, /* changeTypeColumn= */ false);
  }

  @Test
  public void changelogOfSinkWrittenTableRoundTripsThroughTheSourceWithChangeTypeColumn()
      throws Exception {
    roundTripThroughSource(/* upsert= */ false, /* changeTypeColumn= */ true);
  }

  private void roundTripThroughSource(boolean upsert, boolean changeTypeColumn) throws Exception {
    TableIdentifier sourceId = createCanonicalTable("rt_source", 2);
    TableIdentifier targetId = createCanonicalTable("rt_target", 2);

    // Three commits to A, each its own batch run, so the changelog carries three sequence numbers
    // and every kind of change, including a delete after an update and a re-insert after a delete.
    writeBatch(
        sourceId,
        KV.of(ValueKind.INSERT, row(1, "a", "x", 1L)),
        KV.of(ValueKind.INSERT, row(2, "b", "y", 1L)),
        KV.of(ValueKind.INSERT, row(3, "c", "z", 1L)),
        KV.of(ValueKind.INSERT, row(4, "d", "w", 1L)),
        KV.of(ValueKind.INSERT, row(5, "e", "v", 1L)),
        KV.of(ValueKind.INSERT, row(6, "f", "u", 1L)));
    writeBatch(
        sourceId,
        KV.of(ValueKind.UPDATE_BEFORE, row(1, "a", "x", 2L)),
        KV.of(ValueKind.UPDATE_AFTER, row(1, "a2", "x2", 2L)),
        KV.of(ValueKind.DELETE, row(2, "b", "y", 2L)),
        KV.of(ValueKind.UPDATE_BEFORE, row(3, "c", "z", 2L)),
        KV.of(ValueKind.UPDATE_AFTER, row(3, "c2", "z", 2L)),
        KV.of(ValueKind.INSERT, row(7, "g", "t", 2L)));
    writeBatch(
        sourceId,
        KV.of(ValueKind.DELETE, row(1, "a2", "x2", 3L)),
        KV.of(ValueKind.INSERT, row(2, "b2", "y2", 3L)),
        KV.of(ValueKind.DELETE, row(6, "f", "u", 3L)),
        KV.of(ValueKind.UPDATE_BEFORE, row(7, "g", "t", 3L)),
        KV.of(ValueKind.UPDATE_AFTER, row(7, "g2", "t2", 3L)));

    Table source = catalog.loadTable(sourceId);
    List<String> expected = ImmutableList.of("2:b2:y2", "3:c2:z", "4:d:w", "5:e:v", "7:g2:t2");
    assertThat(readRows(source), equalTo(expected));

    // A's changelog into B. The requested sequence-number column is the sink's default ordering
    // column. The kind comes from the source's native ValueKind, which Dataflow Runner v2 does not
    // carry yet (so that variant runs on the legacy worker there), or from the _change_type
    // column, which any runner carries.
    List<String> metadataColumns = new ArrayList<>();
    if (changeTypeColumn) {
      metadataColumns.add(IcebergCdcMetadataColumns.CHANGE_TYPE);
    }
    metadataColumns.add(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER);
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    if (!changeTypeColumn) {
      CdcSinkTestUtils.useLegacyDataflowWorker(options);
    }
    Map<String, Object> readConfig = new HashMap<>(managedIcebergConfig(sourceId.toString()));
    readConfig.put("include_metadata_columns", metadataColumns);
    Map<String, Object> writeConfig = new HashMap<>(managedIcebergConfig(targetId.toString()));
    writeConfig.put("mode", "merge-on-read");
    writeConfig.put("upsert", upsert);
    if (changeTypeColumn) {
      writeConfig.put("change_type_column", IcebergCdcMetadataColumns.CHANGE_TYPE);
    }
    TestPipeline chain = restartPipeline(options);
    PCollection<Row> changes =
        chain
            .apply("read changelog", Managed.read(Managed.ICEBERG_CDC).withConfig(readConfig))
            .getSinglePCollection();
    changes.apply("apply changelog", Managed.write(Managed.ICEBERG).withConfig(writeConfig));
    chain.run().waitUntilFinish();

    Table target = catalog.loadTable(targetId);
    assertThat(readRows(target), equalTo(readRows(source)));
    assertThat(readRows(target), equalTo(expected));
  }

  /**
   * The {@link TestStream} cases drive event time by hand, which only the DirectRunner supports.
   */
  private static void assumeDirectRunner() {
    assumeTrue(OPTIONS.getRunner().equals(DirectRunner.class));
  }

  /**
   * One batch run of the sink against {@code tableId}: one commit. Fixture commits run on the
   * DirectRunner whatever the configured runner, so a Dataflow run spends its one job on the
   * pipeline under test.
   */
  @SafeVarargs
  private final void writeBatch(TableIdentifier tableId, KV<ValueKind, Row>... rows) {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setRunner(DirectRunner.class);
    TestPipeline pipeline = restartPipeline(options);
    Map<String, Object> config = new HashMap<>(managedIcebergConfig(tableId.toString()));
    config.put("mode", "merge-on-read");
    config.put("sequence_number_column", "seq");
    boundedInput(pipeline, rows).apply("write", Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();
  }
}
