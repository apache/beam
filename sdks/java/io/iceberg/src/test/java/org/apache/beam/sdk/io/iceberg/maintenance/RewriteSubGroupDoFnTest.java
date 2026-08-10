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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RewriteSubGroupDoFn}. */
@RunWith(JUnit4.class)
public class RewriteSubGroupDoFnTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  private Table buildTable(int numFiles) throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "rg_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    for (int i = 0; i < numFiles; i++) {
      DataFile df =
          warehouse.writeRecords(
              "f" + i + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              TestFixtures.FILE1SNAPSHOT1);
      append.appendFile(df);
    }
    append.commit();
    return table;
  }

  @Test
  public void binPackReducesFiles() throws Exception {
    // 4 small files (3 records each = 12 total). With target = Long.MAX_VALUE they all pack into 1.
    Table table = buildTable(4);
    long snapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    assertEquals(4, tasks.size());

    RewriteSubGroup group =
        RewriteSubGroup.builder()
            .setGlobalIndex(0)
            .setFileScanTasks(tasks, table.specs())
            .setOutputSpecId(table.spec().specId())
            .setWriteMaxFileSize(Long.MAX_VALUE)
            .setStartingSnapshotId(snapshotId)
            .setStartingSequenceNumber(table.snapshot(snapshotId).sequenceNumber())
            .setOperationId("op-test")
            .setParentGroupIndex(0)
            .setParentSubgroupCount(1)
            .build();

    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    SchemaCoder<RewriteSubGroup> groupCoder =
        SchemaRegistry.createDefault().getSchemaCoder(RewriteSubGroup.class);
    SchemaCoder<ExecutedGroup> executedGroupCoder =
        SchemaRegistry.createDefault().getSchemaCoder(ExecutedGroup.class);

    PCollection<KV<Integer, ExecutedGroup>> out =
        p.apply(Create.of(KV.of(0, group)).withCoder(KvCoder.of(VarIntCoder.of(), groupCoder)))
            .apply(
                ParDo.of(new RewriteSubGroupDoFn(st))
                    .withOutputTags(
                        RewriteSubGroupDoFn.REWRITTEN,
                        TupleTagList.of(RewriteSubGroupDoFn.FAILED_PARENTS)))
            .get(RewriteSubGroupDoFn.REWRITTEN)
            .setCoder(KvCoder.of(VarIntCoder.of(), executedGroupCoder));

    PAssert.that(out)
        .satisfies(
            kvs -> {
              List<KV<Integer, ExecutedGroup>> list = new ArrayList<>();
              kvs.forEach(list::add);
              assertEquals("Expected exactly one output element", 1, list.size());
              ExecutedGroup eg = list.get(0).getValue();
              assertEquals(
                  "Starting snapshot id must match", snapshotId, eg.getStartingSnapshotId());
              assertEquals(
                  "All 4 small files should compact into 1 output file",
                  1,
                  eg.getNewFiles().size());
              return null;
            });

    p.run().waitUntilFinish();
  }

  /**
   * Regression test: two groups processed by the SAME worker must produce data files with distinct
   * paths. Each group's {@code OutputFileFactory} uses the group's global index as its partitionId,
   * so even though the per-factory file counter restarts at 0, the names differ.
   */
  @Test
  public void distinctOutputPathsForGroupsOnOneWorker() throws Exception {
    Table table = buildTable(4);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    assertEquals(4, tasks.size());

    RewriteSubGroup g1 = groupOf(tasks.subList(0, 2), 1, snapshotId, table);
    RewriteSubGroup g2 = groupOf(tasks.subList(2, 4), 2, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    // One DoFnTester bundle => one DoFn instance processes BOTH groups. (Each rewrite also mints
    // its
    // own attemptId; the per-group globalIndex alone already keeps the names distinct.)
    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, g1), KV.of(0, g2));
    List<KV<Integer, ExecutedGroup>> out = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN);

    Set<String> paths = new HashSet<>();
    int totalFiles = 0;
    for (KV<Integer, ExecutedGroup> kv : out) {
      for (SerializableDataFile sdf : kv.getValue().getNewFiles()) {
        paths.add(sdf.createDataFile(table.specs()).location().toString());
        totalFiles++;
      }
    }
    assertEquals("two groups should each produce one compacted file", 2, totalFiles);
    assertEquals(
        "output paths across groups on one worker must be distinct", totalFiles, paths.size());
  }

  /**
   * A group whose rewrite fails must be routed to the REWRITE_FAILURES side output (so it can be
   * counted and reported in the result) while every other group is still rewritten normally — one
   * bad file group must not sink the whole job. This routing happens in BOTH modes — the DoFn never
   * fails fast; atomic all-or-nothing is enforced downstream by the commit gate (not by throwing
   * here), so a failed group's successful siblings can be cleaned up rather than leaked.
   */
  @Test
  public void partialProgressRoutesFailedGroupAndKeepsOthers() throws Exception {
    Table table = buildTable(4);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup good = groupOf(tasks.subList(0, 2), 1, snapshotId, table);
    RewriteSubGroup poisoned = groupOf(tasks.subList(2, 4), 2, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    // Corrupt the poisoned group: delete one of its input files so the read fails.
    table.io().deleteFile(tasks.get(2).file().location().toString());

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, good), KV.of(0, poisoned));

    assertEquals(
        "the healthy group must still be rewritten",
        1,
        tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).size());
    assertEquals(
        "the failed group must be routed to the failures side output",
        1,
        tester.peekOutputElements(RewriteSubGroupDoFn.FAILED_PARENTS).size());
  }

  /**
   * A {@link RewriteSubGroupDoFn} whose first {@code failFirst} rewrite attempts throw (simulating
   * a transient storage blip); later attempts delegate to the real rewrite.
   */
  private static class FlakyRewrite extends RewriteSubGroupDoFn {
    private final int failFirst;
    private int calls = 0;

    FlakyRewrite(SerializableTable table, int failFirst) {
      super(table);
      this.failFirst = failFirst;
    }

    @Override
    ExecutedGroup rewriteOnce(RewriteSubGroup group, List<FileScanTask> tasks) throws Exception {
      if (calls++ < failFirst) {
        throw new RuntimeException("simulated transient failure #" + calls);
      }
      return super.rewriteOnce(group, tasks);
    }
  }

  @Test
  public void transientRewriteFailureRetriedThenSucceeds() throws Exception {
    // A transient read/write failure must NOT immediately skip the group: a bounded retry recovers
    // it (fail the first 2 attempts, succeed on the 3rd).
    Table table = buildTable(2);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new FlakyRewrite(st, 2));
    tester.processBundle(KV.of(0, group));

    assertEquals(
        "the group must be rewritten once transient retries recover",
        1,
        tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).size());
    assertTrue(
        "no group should be routed aside when retries recover",
        tester.peekOutputElements(RewriteSubGroupDoFn.FAILED_PARENTS).isEmpty());
  }

  @Test
  public void permanentRewriteFailureRoutedAsideAfterRetries() throws Exception {
    // A permanently failing group must be routed aside after a BOUNDED number of retries, not
    // retried forever.
    Table table = buildTable(2);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new FlakyRewrite(st, Integer.MAX_VALUE));
    tester.processBundle(KV.of(0, group));

    assertTrue(
        "a permanently failing group must not be rewritten",
        tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).isEmpty());
    assertEquals(
        "a permanently failing group must be routed to REWRITE_FAILURES after retries",
        1,
        tester.peekOutputElements(RewriteSubGroupDoFn.FAILED_PARENTS).size());
  }

  /**
   * A {@link RewriteSubGroupDoFn} whose rewrite always fails with an interruption-caused exception.
   */
  private static class InterruptingRewrite extends RewriteSubGroupDoFn {
    InterruptingRewrite(SerializableTable table) {
      super(table);
    }

    @Override
    ExecutedGroup rewriteOnce(RewriteSubGroup group, List<FileScanTask> tasks) throws Exception {
      // Simulate a worker drain/preemption surfacing as an IO error wrapping InterruptedException.
      throw new java.io.IOException(
          "simulated worker drain", new InterruptedException("worker draining"));
    }
  }

  @Test
  public void interruptedRewriteFailsBundleNotRoutedAside() throws Exception {
    // F9: an interruption (worker drain / preemption / autoscale downscale) must FAIL the bundle so
    // the runner can retry, NOT be converted into a permanent REWRITE_FAILURES element (which would
    // be reported as a rewrite failure, or abort atomic mode and delete sibling outputs, when a
    // plain retry would have succeeded).
    Table table = buildTable(2);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new InterruptingRewrite(st));

    assertThrows(Exception.class, () -> tester.processBundle(KV.of(0, group)));
    assertTrue(
        "an interruption must not be routed aside as a tolerated rewrite failure",
        tester.peekOutputElements(RewriteSubGroupDoFn.FAILED_PARENTS).isEmpty());
  }

  @Test
  public void retryOfSameGroupProducesDistinctOutputPaths() throws Exception {
    // F11: a bundle retry re-runs rewriteOnce for the SAME group on the SAME (reused) DoFn
    // instance.
    // Each attempt must write to distinct paths; otherwise the retry regenerates the first
    // attempt's
    // already-persisted file names (AlreadyExistsException on HDFS-backed FileIO, silent overwrite
    // on object stores while a prior attempt may already have handed that path to commit).
    Table table = buildTable(4);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    RewriteSubGroupDoFn doFn = new RewriteSubGroupDoFn(st);

    ExecutedGroup first = doFn.rewriteOnce(group, tasks);
    ExecutedGroup second = doFn.rewriteOnce(group, tasks);

    Set<String> firstPaths = new HashSet<>();
    for (SerializableDataFile sdf : first.getNewFiles()) {
      firstPaths.add(sdf.getPath());
    }
    assertFalse("sanity: the first attempt wrote output files", firstPaths.isEmpty());
    for (SerializableDataFile sdf : second.getNewFiles()) {
      assertFalse(
          "a retry must not reuse the first attempt's output path: " + sdf.getPath(),
          firstPaths.contains(sdf.getPath()));
    }
  }

  @Test
  public void repartitioningFanoutBeyondOpenWriterCapThrowsWithGuidance() throws Exception {
    // C5: with output-spec-id repartitioning (input spec != output spec) a subgroup's rows can fan
    // out to many output partitions — one open writer each — an OOM risk. A hard cap must throw
    // with actionable guidance. Lower the cap so a two-partition fan-out breaches it.
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    TableIdentifier id = TableIdentifier.of("default", "fanoutcap_" + System.nanoTime());
    Table table = warehouse.createTable(id, schema); // unpartitioned, spec 0
    List<Record> records = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      Record r = GenericRecord.create(schema);
      r.setField("id", (long) i);
      r.setField("shard", i % 2); // two partitions once the spec evolves
      records.add(r);
    }
    table
        .newAppend()
        .appendFile(warehouse.writeRecords("fc_" + System.nanoTime() + ".parquet", schema, records))
        .commit();
    table.updateSpec().addField("shard").commit(); // spec 1: partition by shard
    table.refresh();
    int partitionedSpecId = table.spec().specId();
    long snapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group =
        RewriteSubGroup.builder()
            .setGlobalIndex(1)
            .setParentGroupIndex(0)
            .setParentSubgroupCount(1)
            .setFileScanTasks(tasks, table.specs())
            .setOutputSpecId(partitionedSpecId) // repartition to the partitioned spec
            .setWriteMaxFileSize(Long.MAX_VALUE)
            .setStartingSnapshotId(snapshotId)
            .setStartingSequenceNumber(table.snapshot(snapshotId).sequenceNumber())
            .setOperationId("op-test")
            .build();
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    RewriteSubGroupDoFn doFn = new RewriteSubGroupDoFn(st);
    List<FileScanTask> reconstructed = RewriteGroupTestHelpers.tasks(group, table);

    int originalCap = WriterFactory.maxOpenFanoutWriters;
    WriterFactory.maxOpenFanoutWriters = 1; // a two-partition fan-out now breaches
    try {
      IllegalStateException ex =
          assertThrows(IllegalStateException.class, () -> doFn.rewriteOnce(group, reconstructed));
      assertTrue(
          "cap breach must give actionable guidance: " + ex.getMessage(),
          ex.getMessage().contains("open writers"));
    } finally {
      WriterFactory.maxOpenFanoutWriters = originalCap;
    }
  }

  private static RewriteSubGroup groupOf(
      List<FileScanTask> tasks, int globalIndex, long snapshotId, Table table) {
    return RewriteSubGroup.builder()
        .setGlobalIndex(globalIndex)
        .setFileScanTasks(tasks, table.specs())
        .setOutputSpecId(table.spec().specId())
        .setWriteMaxFileSize(Long.MAX_VALUE)
        .setStartingSnapshotId(snapshotId)
        .setStartingSequenceNumber(table.snapshot(snapshotId).sequenceNumber())
        .setOperationId("op-test")
        .setParentGroupIndex(globalIndex)
        .setParentSubgroupCount(1)
        .build();
  }

  @Test
  public void nonParquetTableRejected() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "avro_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("write.format.default", "avro"));
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    assertThrows(UnsupportedOperationException.class, () -> new RewriteSubGroupDoFn(st));
  }

  @Test
  public void executedGroupCarriesCompactDeleteDescriptors() throws Exception {
    // The commit payload must be compact: the to-DELETE descriptors carry NO column metrics
    // (Iceberg
    // matches deletes by path/identity), while the to-ADD files keep their metrics for the
    // manifest.
    Table table = buildTable(4);
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    assertEquals("all 4 inputs are scheduled for deletion", 4, eg.getRewrittenDataFiles().size());
    for (SerializableDataFile sdf : eg.getRewrittenDataFiles()) {
      DataFile df = sdf.createDataFile(table.specs());
      assertNull("to-delete descriptor must drop column sizes", df.columnSizes());
      assertNull("to-delete descriptor must drop lower bounds", df.lowerBounds());
      assertNull("to-delete descriptor must drop upper bounds", df.upperBounds());
    }
    boolean anyAddMetrics = false;
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      DataFile df = sdf.createDataFile(table.specs());
      if (df.columnSizes() != null && !df.columnSizes().isEmpty()) {
        anyAddMetrics = true;
      }
    }
    assertTrue("to-add files must keep their column metrics for the manifest", anyAddMetrics);
  }

  @Test
  public void writePropertiesOverrideTableWritePropertiesInOutput() throws Exception {
    // A user-supplied write property must override the table's own write property for the rewrite
    // output. The table collects FULL column metrics; the rewrite overrides that to "none", which
    // is directly observable on the newly written files (empty column sizes / bounds).
    TableIdentifier id = TableIdentifier.of("default", "wp_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id,
            TestFixtures.SCHEMA,
            null,
            ImmutableMap.of("write.metadata.metrics.default", "full"));
    AppendFiles append = table.newAppend();
    for (int i = 0; i < 4; i++) {
      append.appendFile(
          warehouse.writeRecords(
              "f" + i + "_" + System.nanoTime() + ".parquet",
              table.schema(),
              TestFixtures.FILE1SNAPSHOT1));
    }
    append.commit();
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(
            new RewriteSubGroupDoFn(st, ImmutableMap.of("write.metadata.metrics.default", "none")));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    assertEquals("4 small files pack into one output", 1, eg.getNewFiles().size());
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      DataFile df = sdf.createDataFile(table.specs());
      assertTrue("override must suppress output column sizes", df.columnSizes().isEmpty());
      assertTrue("override must suppress output lower bounds", df.lowerBounds().isEmpty());
      assertTrue("override must suppress output upper bounds", df.upperBounds().isEmpty());
    }
  }

  @Test
  public void smallTargetRollsToMultipleOutputFiles() throws Exception {
    // Iceberg's rolling writer only checks the target size every ROWS_DIVISOR (1000) rows, so a
    // real target-sizing test needs > 1000 rows AND a tiny target to force a roll. With 2500 rows
    // and target=1 the writer rolls at rows 1000 and 2000, producing multiple output files.
    TableIdentifier id = TableIdentifier.of("default", "roll_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "big_" + System.nanoTime() + ".parquet", table.schema(), manyRows(2500)))
        .commit();
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group =
        RewriteSubGroup.builder()
            .setGlobalIndex(1)
            .setFileScanTasks(tasks, table.specs())
            .setOutputSpecId(table.spec().specId())
            .setWriteMaxFileSize(1L) // tiny target -> roll at each size check
            .setStartingSnapshotId(snapshotId)
            .setStartingSequenceNumber(table.snapshot(snapshotId).sequenceNumber())
            .setOperationId("op-test")
            .setParentGroupIndex(0)
            .setParentSubgroupCount(1)
            .build();
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    assertTrue(
        "a small target must roll into multiple output files, got " + eg.getNewFiles().size(),
        eg.getNewFiles().size() > 1);
  }

  @Test
  public void rowLineagePreservedOnRewriteV3() throws Exception {
    // F2/F3 regression: a data-preserving compaction of a v3 row-lineage table must KEEP each row's
    // _row_id (F2 — else the rewritten file inherits a fresh first_row_id range and every _row_id
    // changes) AND its _last_updated_sequence_number (F3 — restored from the input file's DATA
    // sequence number; the ContentFileParser JSON that ships a TaskDescriptor's data file does not
    // serialize sequence numbers, so without the descriptor's explicit dataSequenceNumber scalar a
    // naive rewrite writes null for every row and the per-row update sequence is lost).
    TableIdentifier id = TableIdentifier.of("default", "v3_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    // Two files in two snapshots so _row_id spans two first_row_id ranges AND the rows have
    // DISTINCT
    // data sequence numbers.
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "a_" + System.nanoTime() + ".parquet", table.schema(), rows(1, 2, 3)))
        .commit();
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "b_" + System.nanoTime() + ".parquet", table.schema(), rows(4, 5, 6)))
        .commit();
    table.refresh();

    Map<Long, Long> rowIdsBefore = rowIdById(table);
    Map<Long, Long> lastUpdatedBefore = lastUpdatedSeqById(table);
    assertEquals("sanity: 6 rows", 6, rowIdsBefore.size());
    assertEquals("sanity: distinct _row_ids", 6, new HashSet<>(rowIdsBefore.values()).size());
    assertTrue(
        "sanity: the two files must have distinct sequence numbers",
        new HashSet<>(lastUpdatedBefore.values()).size() >= 2);

    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    // Commit the rewrite so the new files are readable with lineage.
    RewriteFiles rf = table.newRewrite().validateFromSnapshot(snapshotId);
    for (FileScanTask t : tasks) {
      rf.deleteFile(t.file());
    }
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      rf.addFile(sdf.createDataFile(table.specs()));
    }
    rf.commit();
    table.refresh();

    assertEquals(
        "_row_id must be preserved per row across the rewrite", rowIdsBefore, rowIdById(table));
    assertEquals(
        "_last_updated_sequence_number must be preserved per row across the rewrite",
        lastUpdatedBefore,
        lastUpdatedSeqById(table));
  }

  private static List<Record> rows(long... ids) {
    List<Record> recs = new ArrayList<>();
    for (long i : ids) {
      GenericRecord r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", i);
      r.setField("data", "d" + i);
      recs.add(r);
    }
    return recs;
  }

  private static List<Record> manyRows(int n) {
    List<Record> recs = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      GenericRecord r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "d" + i);
      recs.add(r);
    }
    return recs;
  }

  /** Maps each row's {@code id} to its materialized {@code _row_id} by reading with lineage. */
  private static Map<Long, Long> rowIdById(Table table) throws Exception {
    org.apache.iceberg.Schema lineage = MetadataColumns.schemaWithRowLineage(table.schema());
    Map<Long, Long> out = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        try (CloseableIterable<Record> records = ReadUtils.createReader(t, table, lineage)) {
          for (Record r : records) {
            out.put((Long) r.getField("id"), (Long) r.getField("_row_id"));
          }
        }
      }
    }
    return out;
  }

  /** Maps each row's {@code id} to its materialized {@code _last_updated_sequence_number}. */
  private static Map<Long, Long> lastUpdatedSeqById(Table table) throws Exception {
    org.apache.iceberg.Schema lineage = MetadataColumns.schemaWithRowLineage(table.schema());
    Map<Long, Long> out = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        try (CloseableIterable<Record> records = ReadUtils.createReader(t, table, lineage)) {
          for (Record r : records) {
            out.put((Long) r.getField("id"), (Long) r.getField("_last_updated_sequence_number"));
          }
        }
      }
    }
    return out;
  }

  /** Writes a v3 deletion vector deleting {@code position} in {@code dataFile} and commits it. */
  private void addDeletionVector(Table table, DataFile dataFile, long position) throws Exception {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 3, 3L).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try {
      writer.delete(dataFile.location().toString(), position, table.spec(), null);
    } finally {
      writer.close();
    }
    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
    table.refresh();
  }

  @Test
  public void rowLineagePreservedWithDeletionVectorV3() throws Exception {
    // F2 regression: a v3 file that carries a deletion vector is read with _pos appended to its
    // projection. The generic Parquet writer copies record fields BY POSITION, so if records are
    // laid out [id, data, _pos, _row_id, _lus] while the writer expects [id, data, _row_id, _lus],
    // the _row_id column silently receives the file POSITION instead of the real _row_id. Each
    // surviving row must keep its original _row_id.
    TableIdentifier id = TableIdentifier.of("default", "v3dv_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    // Two files in two snapshots so file B's first_row_id > 0 (its positions != its _row_ids).
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "a_" + System.nanoTime() + ".parquet", table.schema(), rows(1, 2, 3)))
        .commit();
    DataFile fileB =
        warehouse.writeRecords(
            "b_" + System.nanoTime() + ".parquet", table.schema(), rows(4, 5, 6));
    table.newAppend().appendFile(fileB).commit();
    table.refresh();

    // A deletion vector on file B (delete position 0 => id 4) forces _pos into file B's projection.
    addDeletionVector(table, fileB, 0L);

    Map<Long, Long> originalRowIds = rowIdById(table);

    long snapshotId = table.currentSnapshot().snapshotId();
    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    // Commit the rewrite: delete the input files and their DV, add the new compacted file.
    RewriteFiles rf = table.newRewrite().validateFromSnapshot(snapshotId);
    for (FileScanTask t : tasks) {
      rf.deleteFile(t.file());
      for (DeleteFile dv : t.deletes()) {
        rf.deleteFile(dv);
      }
    }
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      rf.addFile(sdf.createDataFile(table.specs()));
    }
    rf.commit();
    table.refresh();

    Map<Long, Long> afterRowIds = rowIdById(table);
    assertNull("the DV-deleted row must be gone after the rewrite", afterRowIds.get(4L));
    for (long survivingId : new long[] {1L, 2L, 3L, 5L, 6L}) {
      assertEquals(
          "row " + survivingId + " must keep its original _row_id after a DV rewrite",
          originalRowIds.get(survivingId),
          afterRowIds.get(survivingId));
    }
  }

  @Test
  public void rowLineageSurvivesBeamRowRoundTrip() throws Exception {
    // Prerequisite for F4-C (record-shuffle rewrite): _row_id / _last_updated_sequence_number must
    // survive Record -> Beam Row -> Record via the lineage-augmented schema, since the record
    // shuffle moves Beam Rows (there is no Iceberg Record coder) and must not drop lineage.
    TableIdentifier id = TableIdentifier.of("default", "v3rt_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "a_" + System.nanoTime() + ".parquet", table.schema(), rows(4, 5, 6)))
        .commit();
    table.refresh();

    org.apache.iceberg.Schema lineage = MetadataColumns.schemaWithRowLineage(table.schema());
    org.apache.beam.sdk.schemas.Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(lineage);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      FileScanTask t = tasks.iterator().next();
      try (CloseableIterable<Record> records = ReadUtils.createReader(t, table, lineage)) {
        Record record = records.iterator().next();
        Long rowId = (Long) record.getField("_row_id");
        assertNotNull("sanity: the read record materializes _row_id", rowId);

        org.apache.beam.sdk.values.Row row =
            IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
        Record back = IcebergUtils.beamRowToIcebergRecord(lineage, row);

        assertEquals("_row_id must survive the Row round-trip", rowId, back.getField("_row_id"));
        assertEquals(
            "_last_updated_sequence_number must survive the Row round-trip",
            record.getField("_last_updated_sequence_number"),
            back.getField("_last_updated_sequence_number"));
        assertEquals(
            "the data column must survive the Row round-trip",
            record.getField("data"),
            back.getField("data"));
      }
    }
  }

  @Test
  public void nonParquetInputFileFailsFastNotRoutedAside() throws Exception {
    // A Parquet-default table whose format evolved over time can still hold a non-Parquet input
    // file. The Parquet-only contract must FAIL the rewrite (in both modes), not tolerate the file
    // as an ordinary per-group failure routed to REWRITE_FAILURES.
    Table table = buildTable(1); // write.format.default = parquet, one real Parquet file
    // A metadata-only Avro data file: the format guard rejects it before any read, so its bytes
    // never need to exist.
    DataFile avro =
        DataFiles.builder(table.spec())
            .withPath(table.location() + "/data/legacy_" + System.nanoTime() + ".avro")
            .withFormat(FileFormat.AVRO)
            .withFileSizeInBytes(128L)
            .withRecordCount(3L)
            .build();
    table.newAppend().appendFile(avro).commit();
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();

    List<FileScanTask> tasks;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      tasks = Lists.newArrayList(it);
    }
    RewriteSubGroup group = groupOf(tasks, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    // Must THROW (fail-fast) rather than complete with the Avro file routed to REWRITE_FAILURES.
    Exception ex = assertThrows(Exception.class, () -> tester.processBundle(KV.of(0, group)));
    boolean parquetError = false;
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains("only Parquet")) {
        parquetError = true;
        break;
      }
    }
    assertTrue("must fail fast with the Parquet-only error, not route aside: " + ex, parquetError);
  }

  /**
   * R2 (extends F2 to a start&gt;0 RANGE task — the riskiest newly-exercised path). A
   * multi-row-group v3 file's later row groups become range tasks with a non-zero {@code start}.
   * Reading such a range must still materialize {@code _row_id = first_row_id + _pos} (Parquet
   * {@code _pos} is absolute, not range-relative) and apply a deletion vector whose deleted
   * position falls inside that range. Every surviving row keeps its original {@code _row_id}; the
   * DV-deleted row is gone.
   */
  @Test
  public void rowLineagePreservedOnRangedTaskWithDeletionVectorV3() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "v3range_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    // File A in snapshot 1 so file B's first_row_id > 0 (its positions differ from its _row_ids).
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "a_" + System.nanoTime() + ".parquet", table.schema(), rows(1, 2, 3)))
        .commit();
    // File B: multi-row-group, distinct ids 100..(100+n-1).
    int n = 400;
    DataFile fileB =
        warehouse.writeRecords(
            "b_" + System.nanoTime() + ".parquet",
            table.schema(),
            distinctRows(100, n),
            MULTI_ROW_GROUP_PROPS);
    table.newAppend().appendFile(fileB).commit();
    table.refresh();

    // A deletion vector on the LAST row (position n-1, id 100+n-1), which lives in the last row
    // group — hence in a range task with start > 0.
    long deletePos = n - 1L;
    long deletedId = 100 + deletePos;
    addDeletionVector(table, fileB, deletePos);

    Map<Long, Long> originalRowIds = rowIdById(table);
    Map<Long, Long> originalLus = lastUpdatedSeqById(table);
    long snapshotId = table.currentSnapshot().snapshotId();

    // Split file B into row-group ranges (one per row group, DV delegated to each range).
    List<FileScanTask> ranges = Lists.newArrayList(scanTaskFor(table, fileB.location()).split(1L));
    assertTrue("file B must span multiple row groups", ranges.size() >= 2);
    assertTrue(
        "the row containing the deleted position must be in a start>0 range",
        ranges.get(ranges.size() - 1).start() > 0);

    RewriteSubGroup group = groupOf(ranges, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    // Commit: delete file B and its DV, add the rewritten file(s). File A is untouched.
    RewriteFiles rf = table.newRewrite().validateFromSnapshot(snapshotId);
    rf.deleteFile(fileB);
    for (FileScanTask t : ranges) {
      for (DeleteFile dv : t.deletes()) {
        rf.deleteFile(dv);
      }
    }
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      rf.addFile(sdf.createDataFile(table.specs()));
    }
    rf.commit();
    table.refresh();

    Map<Long, Long> afterRowIds = rowIdById(table);
    Map<Long, Long> afterLus = lastUpdatedSeqById(table);
    assertNull(
        "the DV-deleted row must be gone after the ranged rewrite", afterRowIds.get(deletedId));
    for (Map.Entry<Long, Long> e : originalRowIds.entrySet()) {
      if (e.getKey() == deletedId) {
        continue;
      }
      assertEquals(
          "row " + e.getKey() + " must keep its original _row_id after a ranged DV rewrite",
          e.getValue(),
          afterRowIds.get(e.getKey()));
      // F4-C acceptance #3: _last_updated_sequence_number must survive a start>0 ranged rewrite too
      // (restored from the planning-time data sequence number, not the rewrite's own).
      assertEquals(
          "row "
              + e.getKey()
              + " must keep its _last_updated_sequence_number after a ranged "
              + "DV rewrite",
          originalLus.get(e.getKey()),
          afterLus.get(e.getKey()));
    }
  }

  /**
   * R2: a subgroup bin can hold row-group ranges from SEVERAL files. Reading such a mixed bin
   * through the unchanged {@link RewriteSubGroupDoFn} must reproduce exactly the union of those
   * ranges' rows — no drop, duplicate, or swap across range and file boundaries.
   */
  @Test
  public void rowMultisetPreservedAcrossBinMixingRangesOfMultipleFiles() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "mix_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id,
            TestFixtures.SCHEMA,
            null,
            ImmutableMap.of("write.parquet.compression-codec", "uncompressed"));
    DataFile f1 =
        warehouse.writeRecords(
            "m1_" + System.nanoTime() + ".parquet",
            table.schema(),
            distinctRows(0, 300),
            MULTI_ROW_GROUP_PROPS);
    DataFile f2 =
        warehouse.writeRecords(
            "m2_" + System.nanoTime() + ".parquet",
            table.schema(),
            distinctRows(1000, 300),
            MULTI_ROW_GROUP_PROPS);
    table.newAppend().appendFile(f1).appendFile(f2).commit();
    table.refresh();
    List<String> expected = rowMultiset(table);
    long snapshotId = table.currentSnapshot().snapshotId();

    // Split both files into ranges and INTERLEAVE them into one group so the bin mixes files.
    List<FileScanTask> r1 = Lists.newArrayList(scanTaskFor(table, f1.location()).split(1L));
    List<FileScanTask> r2 = Lists.newArrayList(scanTaskFor(table, f2.location()).split(1L));
    assertTrue("both files must be multi-row-group", r1.size() >= 2 && r2.size() >= 2);
    List<FileScanTask> mixed = new ArrayList<>();
    for (int i = 0; i < Math.max(r1.size(), r2.size()); i++) {
      if (i < r1.size()) {
        mixed.add(r1.get(i));
      }
      if (i < r2.size()) {
        mixed.add(r2.get(i));
      }
    }
    RewriteSubGroup group = groupOf(mixed, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    RewriteFiles rf = table.newRewrite().validateFromSnapshot(snapshotId);
    rf.deleteFile(f1);
    rf.deleteFile(f2);
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      rf.addFile(sdf.createDataFile(table.specs()));
    }
    rf.commit();
    table.refresh();

    assertEquals(
        "row multiset must be preserved across a bin mixing ranges of multiple files",
        expected,
        rowMultiset(table));
  }

  /**
   * B9/R2: the bin-mixing path on a v3 row-lineage table with the two files in SEPARATE snapshots
   * (distinct data sequence numbers). Reading the interleaved bin must preserve BOTH {@code
   * _row_id} and {@code _last_updated_sequence_number} per row — the only case that would catch a
   * future misalignment of the index-aligned {@code getTaskDescriptors().get(i)
   * .getDataSequenceNumber()} lookup for interleaved multi-file range tasks.
   */
  @Test
  public void rowLineagePreservedAcrossBinMixingRangesOfMultipleFilesV3() throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "mixv3_" + System.nanoTime());
    Table table =
        warehouse.createTable(
            id, TestFixtures.SCHEMA, null, ImmutableMap.of("format-version", "3"));
    // Two files committed in SEPARATE snapshots -> distinct data sequence numbers, so their rows
    // carry different _last_updated_sequence_number values.
    DataFile f1 =
        warehouse.writeRecords(
            "mv1_" + System.nanoTime() + ".parquet",
            table.schema(),
            distinctRows(0, 300),
            MULTI_ROW_GROUP_PROPS);
    table.newAppend().appendFile(f1).commit();
    DataFile f2 =
        warehouse.writeRecords(
            "mv2_" + System.nanoTime() + ".parquet",
            table.schema(),
            distinctRows(1000, 300),
            MULTI_ROW_GROUP_PROPS);
    table.newAppend().appendFile(f2).commit();
    table.refresh();

    Map<Long, Long> beforeRowIds = rowIdById(table);
    Map<Long, Long> beforeLus = lastUpdatedSeqById(table);
    assertNotEquals(
        "the two files must sit at distinct sequence numbers",
        beforeLus.get(0L),
        beforeLus.get(1000L));
    long snapshotId = table.currentSnapshot().snapshotId();

    // Split both files into ranges and INTERLEAVE them so one bin mixes files from both snapshots.
    List<FileScanTask> r1 = Lists.newArrayList(scanTaskFor(table, f1.location()).split(1L));
    List<FileScanTask> r2 = Lists.newArrayList(scanTaskFor(table, f2.location()).split(1L));
    assertTrue("both files must be multi-row-group", r1.size() >= 2 && r2.size() >= 2);
    List<FileScanTask> mixed = new ArrayList<>();
    for (int i = 0; i < Math.max(r1.size(), r2.size()); i++) {
      if (i < r1.size()) {
        mixed.add(r1.get(i));
      }
      if (i < r2.size()) {
        mixed.add(r2.get(i));
      }
    }
    RewriteSubGroup group = groupOf(mixed, 1, snapshotId, table);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);

    DoFnTester<KV<Integer, RewriteSubGroup>, KV<Integer, ExecutedGroup>> tester =
        DoFnTester.of(new RewriteSubGroupDoFn(st));
    tester.processBundle(KV.of(0, group));
    ExecutedGroup eg = tester.peekOutputElements(RewriteSubGroupDoFn.REWRITTEN).get(0).getValue();

    RewriteFiles rf = table.newRewrite().validateFromSnapshot(snapshotId);
    rf.deleteFile(f1);
    rf.deleteFile(f2);
    for (SerializableDataFile sdf : eg.getNewFiles()) {
      rf.addFile(sdf.createDataFile(table.specs()));
    }
    rf.commit();
    table.refresh();

    assertEquals(
        "every row's _row_id must survive the mixed-file bin rewrite",
        beforeRowIds,
        rowIdById(table));
    assertEquals(
        "every row's _last_updated_sequence_number must survive the mixed-file bin rewrite",
        beforeLus,
        lastUpdatedSeqById(table));
  }

  /** Writer properties forcing many small row groups (see RewriteDataFilesCorrectnessTest note). */
  private static final Map<String, String> MULTI_ROW_GROUP_PROPS =
      ImmutableMap.<String, String>builder()
          .put("write.parquet.row-group-size-bytes", "8192")
          .put("parquet.enable.dictionary", "false")
          .put("write.parquet.page-size-bytes", "1024")
          .put("write.parquet.row-group-check-max-record-count", "100")
          .put("write.parquet.compression-codec", "uncompressed")
          .build();

  private static List<Record> distinctRows(long startId, int count) {
    List<Record> recs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      GenericRecord r = GenericRecord.create(TestFixtures.SCHEMA);
      long v = startId + i;
      r.setField("id", v);
      // Fixed-width per-row-distinct payload so files reliably span several row groups regardless
      // of
      // id magnitude (small ids would otherwise make tiny rows that fit in a single row group).
      r.setField("data", "row-" + v + "-padding-0123456789abcdef0123456789abcdef0123456789");
      recs.add(r);
    }
    return recs;
  }

  private static FileScanTask scanTaskFor(Table table, String location) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        if (t.file().location().equals(location)) {
          return t;
        }
      }
    }
    throw new AssertionError("no scan task for " + location);
  }

  /** Live rows as a sorted (id|data) multiset (post-rewrite tables carry no deletes). */
  private static List<String> rowMultiset(Table table) throws Exception {
    List<String> keys = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask t : tasks) {
        try (CloseableIterable<Record> reader = ReadUtils.createReader(t, table, table.schema())) {
          for (Record r : reader) {
            keys.add(r.getField("id") + "|" + r.getField("data"));
          }
        }
      }
    }
    java.util.Collections.sort(keys);
    return keys;
  }
}
