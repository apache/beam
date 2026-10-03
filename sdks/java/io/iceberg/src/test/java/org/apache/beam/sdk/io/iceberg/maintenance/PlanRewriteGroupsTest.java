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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TestDataWarehouse;
import org.apache.beam.sdk.io.iceberg.TestFixtures;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link PlanRewriteGroups}. */
public class PlanRewriteGroupsTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  /** Creates an unpartitioned table with {@code numFiles} small data files (3 records each). */
  private Table buildTable(int numFiles) throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "plan_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    AppendFiles append = table.newAppend();
    for (int i = 0; i < numFiles; i++) {
      DataFile df =
          warehouse.writeRecords("f" + i + ".parquet", table.schema(), TestFixtures.FILE1SNAPSHOT1);
      append.appendFile(df);
    }
    append.commit();
    return table;
  }

  /**
   * A {@code shard}-partitioned (identity) table with {@code filesPerShard[s]} small files in shard
   * {@code s}. Planning is per-partition, so each shard becomes its own parent group.
   */
  private Table buildShardedTable(int... filesPerShard) throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "shard", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("shard").build();
    TableIdentifier id = TableIdentifier.of("default", "shard_" + System.nanoTime());
    Table table = warehouse.createTable(id, schema, spec);
    AppendFiles append = table.newAppend();
    for (int shard = 0; shard < filesPerShard.length; shard++) {
      for (int f = 0; f < filesPerShard[shard]; f++) {
        Record partition = GenericRecord.create(spec.partitionType());
        partition.setField("shard", shard);
        List<Record> recs = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
          Record r = GenericRecord.create(schema);
          r.setField("id", (long) (shard * 1000L + f * 10L + i));
          r.setField("shard", shard);
          recs.add(r);
        }
        append.appendFile(
            warehouse.writeRecords(
                "shard" + shard + "_" + f + "_" + System.nanoTime() + ".parquet",
                schema,
                spec,
                partition,
                recs));
      }
    }
    append.commit();
    table.refresh();
    return table;
  }

  private static long partitionBytes(Table table, int shard) throws Exception {
    long total = 0;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        if (((Integer) t.file().partition().get(0, Integer.class)) == shard) {
          total += t.file().fileSizeInBytes();
        }
      }
    }
    return total;
  }

  private PCollection<KV<Integer, RewriteSubGroup>> runPlan(
      Table table, RewriteDataFiles.Configuration config) {
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    return p.apply(Create.of(SnapshotInfo.fromSnapshot(table.currentSnapshot())))
        .apply(new PlanRewriteGroups(st, config))
        .get(PlanRewriteGroups.GROUPS);
  }

  @Test
  public void planSummaryFragmentMatchesPlannedReality() throws Exception {
    // The planning DoFn emits exactly one RewriteResult fragment whose fields equal what it
    // planned — ids set, and the planned parent/file/byte counts matching the emitted groups.
    Table table = buildTable(8);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("rewrite-all", "true", "min-input-files", "1"))
            .build();
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> tester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    tester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    List<KV<Integer, RewriteSubGroup>> groups = tester.peekOutputElements(PlanRewriteGroups.GROUPS);
    List<RewriteResult> summaries = tester.peekOutputElements(PlanRewriteGroups.PLAN_SUMMARY);

    assertEquals("exactly one planning fragment", 1, summaries.size());
    RewriteResult s = summaries.get(0);
    assertNotNull("operation id set at planning time", s.getOperationId());
    assertEquals(Long.valueOf(table.currentSnapshot().snapshotId()), s.getStartingSnapshotId());

    Set<Integer> parents = new HashSet<>();
    Set<String> files = new HashSet<>();
    for (KV<Integer, RewriteSubGroup> kv : groups) {
      parents.add(kv.getValue().getParentGroupIndex());
      RewriteGroupTestHelpers.rewrittenDataFiles(kv.getValue(), table)
          .forEach(f -> files.add(f.location()));
      assertEquals(
          "every group shares the summary's operation id",
          s.getOperationId(),
          kv.getValue().getOperationId());
    }
    assertEquals(
        "plannedParentGroups == distinct planned parents",
        (long) parents.size(),
        s.getPlannedParentGroups());
    assertEquals("plannedFiles == distinct input files", (long) files.size(), s.getPlannedFiles());
    assertTrue("plannedBytes > 0 when groups were planned", s.getPlannedBytes() > 0);
  }

  @Test
  public void planSummaryEmittedEvenWhenNothingPlanned() throws Exception {
    // The fragment is emitted ALWAYS — including when the planner keeps zero groups (ids set,
    // counts 0), so an otherwise-no-op run still reports a result row.
    Table table = buildTable(2);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    // min-input-files far above the file count => no group qualifies => zero groups planned.
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(ImmutableMap.of("min-input-files", "100"))
            .build();
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> tester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    tester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));

    assertTrue("no groups planned", tester.peekOutputElements(PlanRewriteGroups.GROUPS).isEmpty());
    List<RewriteResult> summaries = tester.peekOutputElements(PlanRewriteGroups.PLAN_SUMMARY);
    assertEquals("still exactly one planning fragment", 1, summaries.size());
    RewriteResult s = summaries.get(0);
    assertNotNull("operation id set even for a no-op plan", s.getOperationId());
    assertEquals(Long.valueOf(table.currentSnapshot().snapshotId()), s.getStartingSnapshotId());
    assertEquals(0L, s.getPlannedParentGroups());
    assertEquals(0L, s.getPlannedFiles());
    assertEquals(0L, s.getPlannedBytes());
  }

  @Test
  public void operationIdMintedPerPlanningExecution() throws Exception {
    // The operation id used for idempotency stamping must be minted at planning (execution) time.
    // a re-executed serialized graph (e.g. a Dataflow template) would otherwise collide with a
    // prior run's stamps and silently skip every commit.
    Table table = buildTable(10);
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    RewriteDataFiles.Configuration config = RewriteDataFiles.Configuration.builder().build();
    SnapshotInfo impulse = SnapshotInfo.fromSnapshot(table.currentSnapshot());

    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> tester1 =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    tester1.processBundle(impulse);
    List<KV<Integer, RewriteSubGroup>> run1 = tester1.peekOutputElements(PlanRewriteGroups.GROUPS);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> tester2 =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    tester2.processBundle(impulse);
    List<KV<Integer, RewriteSubGroup>> run2 = tester2.peekOutputElements(PlanRewriteGroups.GROUPS);

    assertFalse("planning must emit groups", run1.isEmpty());
    assertFalse("planning must emit groups", run2.isEmpty());
    String op1 = run1.get(0).getValue().getOperationId();
    String op2 = run2.get(0).getValue().getOperationId();
    assertNotNull("operation id must be set at planning time", op1);
    for (KV<Integer, RewriteSubGroup> kv : run1) {
      assertEquals(
          "all groups in one execution share the operation id",
          op1,
          kv.getValue().getOperationId());
    }
    assertNotEquals("each planning execution must mint a fresh operation id", op1, op2);
  }

  @Test
  public void nothingToRewrite_emitsNothing() throws Exception {
    // A single small file does not meet the default min-input-files (5) -> no group.
    Table table = buildTable(1);
    PCollection<KV<Integer, RewriteSubGroup>> out =
        runPlan(table, RewriteDataFiles.Configuration.builder().build());
    PAssert.that(out).empty();
    p.run().waitUntilFinish();
  }

  @Test
  public void binPacksManySmallFiles_singleCommitKey() throws Exception {
    // 10 small files pack into ONE group (default target size is huge) and meet min-input-files.
    Table table = buildTable(10);
    PCollection<KV<Integer, RewriteSubGroup>> out =
        runPlan(table, RewriteDataFiles.Configuration.builder().build());

    PAssert.that(out)
        .satisfies(
            kvs -> {
              int totalFiles = 0;
              for (KV<Integer, RewriteSubGroup> kv : kvs) {
                assertEquals("All groups must share commit key 0", Integer.valueOf(0), kv.getKey());
                totalFiles += kv.getValue().getTaskDescriptors().size();
              }
              assertEquals(10, totalFiles);
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void filesToRewriteByteSizeReportsEachPlannedFileOnce() throws Exception {
    // The per-file input-size distribution must fire once per DISTINCT planned file. An earlier
    // `t.start() == 0` guard never matched Parquet's first range (which starts at splitOffsets[0]
    // == 4, never 0), so the distribution stayed permanently empty for every real file.
    Table table = buildTable(10); // 10 distinct single-row-group files; default plan -> one group
    runPlan(table, RewriteDataFiles.Configuration.builder().build());
    PipelineResult result = p.run();
    result.waitUntilFinish();

    long count = 0;
    for (MetricResult<DistributionResult> d :
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(PlanRewriteGroups.class, "fileByteSizeToRewrite"))
                    .build())
            .getDistributions()) {
      count += d.getAttempted().getCount();
    }
    assertEquals(
        "distribution must report exactly one sample per distinct planned file", 10L, count);
  }

  @Test
  public void maxFilesToRewrite_noDataLoss() throws Exception {
    // With max-files-to-rewrite=3 exactly 3 files are scheduled (never 5), proving files that are
    // not rewritten are never scheduled for deletion.
    Table table = buildTable(5);
    PCollection<KV<Integer, RewriteSubGroup>> out =
        runPlan(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.of("min-input-files", "1", "max-files-to-rewrite", "3"))
                .build());

    PAssert.that(out)
        .satisfies(
            kvs -> {
              int totalFiles = 0;
              for (KV<Integer, RewriteSubGroup> kv : kvs) {
                totalFiles += kv.getValue().getTaskDescriptors().size();
              }
              assertEquals("Exactly max-files-to-rewrite files must be scheduled", 3, totalFiles);
              return null;
            });
    p.run().waitUntilFinish();
  }

  @Test
  public void filterRestrictsPlanningToMatchingPartition() throws Exception {
    // setFilter must restrict which files the planner scans. A planner that ignored
    // config.getFilter() would (wrongly) plan files from every shard.
    Table table = buildShardedTable(2, 2); // shard 0 and shard 1, 2 files each
    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setFilter("shard = 0")
                .setRewriteOptions(ImmutableMap.of("min-input-files", "1", "rewrite-all", "true"))
                .build());
    assertFalse("filter shard=0 must still plan groups", planned.isEmpty());
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
        assertEquals(
            "only shard-0 files may be planned under filter shard=0",
            Integer.valueOf(0),
            (Integer) t.file().partition().get(0, Integer.class));
      }
    }
  }

  @Test
  public void caseSensitivityOnFilterColumnNameIsCanonicalizedBeforePlanning() throws Exception {
    // FilterUtils.convert resolves the filter column CASE-INSENSITIVELY and rewrites it to the
    // schema's canonical name before the planner binds it, so a mis-cased column ("SHARD") plans
    // the same either way. Pins that behavior — caseSensitive is inert for converted filters.
    Table table = buildShardedTable(2, 2);
    for (boolean caseSensitive : new boolean[] {false, true}) {
      List<KV<Integer, RewriteSubGroup>> planned =
          planOnly(
              table,
              RewriteDataFiles.Configuration.builder()
                  .setFilter("SHARD = 0")
                  .setCaseSensitive(caseSensitive)
                  .setRewriteOptions(ImmutableMap.of("min-input-files", "1", "rewrite-all", "true"))
                  .build());
      assertFalse(
          "mis-cased filter must still plan shard-0 with caseSensitive=" + caseSensitive,
          planned.isEmpty());
      for (KV<Integer, RewriteSubGroup> kv : planned) {
        for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
          assertEquals(
              "only shard-0 files may be planned",
              Integer.valueOf(0),
              (Integer) t.file().partition().get(0, Integer.class));
        }
      }
    }
  }

  @Test
  public void maxRewriteBytesSkipsOverBudgetGroupButAdmitsLaterSmallerOnes() throws Exception {
    // The maxRewriteBytes running budget must SKIP an over-budget group yet still ADMIT a later
    // smaller one that fits (continue, not break). Shard 1 is huge and exceeds the budget alone;
    // shards 0 and 2 are small and fit. A `break` would drop shard 2, which follows shard 1.
    Table table = buildShardedTable(1, 12, 1);
    long small0 = partitionBytes(table, 0);
    long small2 = partitionBytes(table, 2);
    long huge = partitionBytes(table, 1);
    long budget = small0 + small2 + 1; // fits both small partitions; far below the huge one
    assertTrue("fixture: the huge partition must exceed the budget on its own", huge > budget);

    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setMaxRewriteBytes(budget)
                .setRewriteOptions(ImmutableMap.of("min-input-files", "1", "rewrite-all", "true"))
                .build());

    Set<Integer> plannedShards = new HashSet<>();
    Set<String> countedFiles = new HashSet<>();
    long plannedBytes = 0;
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
        plannedShards.add((Integer) t.file().partition().get(0, Integer.class));
        if (countedFiles.add(t.file().location())) {
          plannedBytes += t.file().fileSizeInBytes();
        }
      }
    }
    assertEquals(
        "only the two small partitions (skip huge, admit later smaller) must be planned",
        ImmutableSet.of(0, 2),
        plannedShards);
    assertTrue(
        "planned bytes (" + plannedBytes + ") must stay within maxRewriteBytes (" + budget + ")",
        plannedBytes <= budget);
  }

  @Test
  public void roundRobinCommitKeysCoverExpectedRangeAndKeepParentsTogether() throws Exception {
    // Commit keys are round-robin over parents (keptIndex % maxCommits). Every emitted key must
    // fall in [0, min(maxCommits, parentCount)), and all subgroups of one parent must share a key.
    Table table = buildTable(8); // 8 single-file parents (each shatters into several subgroups)
    int maxCommits = 3;
    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setPartialProgressEnabled(true)
                .setMaxCommits(maxCommits)
                .setRewriteOptions(
                    ImmutableMap.<String, String>builder()
                        .put("min-input-files", "1")
                        .put("max-file-group-size-bytes", "1")
                        .put("min-file-size-bytes", "0")
                        .put("target-file-size-bytes", "2")
                        .put("max-file-size-bytes", "3")
                        .build())
                .build());
    assertFalse("planning must emit groups", planned.isEmpty());

    Set<Integer> keys = new HashSet<>();
    Map<Integer, Set<Integer>> keysByParent = new HashMap<>();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      keys.add(kv.getKey());
      keysByParent
          .computeIfAbsent(kv.getValue().getParentGroupIndex(), k -> new HashSet<>())
          .add(kv.getKey());
    }
    int parentCount = keysByParent.size();
    assertEquals("fixture must yield 8 parents", 8, parentCount);
    for (Integer key : keys) {
      assertTrue(
          "commit key " + key + " must fall in the round-robin range",
          key >= 0 && key < Math.min(maxCommits, parentCount));
    }
    for (Map.Entry<Integer, Set<Integer>> e : keysByParent.entrySet()) {
      assertEquals(
          "all subgroups of parent " + e.getKey() + " must share one commit key",
          1,
          e.getValue().size());
    }
  }

  @Test
  public void partialProgress_assignsMultipleKeys() throws Exception {
    // A tiny max-file-group-size-bytes (1) puts each file in its own group (8 groups), and a tiny
    // max-file-size-bytes makes every file a rewrite candidate so each single-file group is kept.
    // maxCommits=4 -> round-robin keys 0,1,2,3,0,1,2,3 -> 4 distinct keys.
    Table table = buildTable(8);
    // Reconstructing input files from the compact descriptors needs table.specs(), so plan
    // in-process (planOnly) rather than through a serialized PAssert lambda.
    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setPartialProgressEnabled(true)
                .setMaxCommits(4)
                .setRewriteOptions(
                    ImmutableMap.<String, String>builder()
                        .put("min-input-files", "1")
                        .put("max-file-group-size-bytes", "1")
                        .put("min-file-size-bytes", "0")
                        .put("target-file-size-bytes", "2")
                        .put("max-file-size-bytes", "3")
                        .build())
                .build());

    Set<Integer> keys = new HashSet<>();
    // Count DISTINCT input data files: these fixture files carry no split offsets, so a tiny target
    // shatters each into several ranges spread across sub-groups (the fixed-size fallback). Only
    // the distinct count is comparable to the original file count.
    Set<String> distinctInputFiles = new HashSet<>();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      keys.add(kv.getKey());
      RewriteGroupTestHelpers.rewrittenDataFiles(kv.getValue(), table)
          .forEach(f -> distinctInputFiles.add(f.location()));
    }
    assertEquals("Expected 4 distinct commit keys", 4, keys.size());
    assertEquals("No file loss across keys", 8, distinctInputFiles.size());
  }

  @Test
  public void splitsOneGroupIntoParallelSubGroups() throws Exception {
    // 4 files form a SINGLE parent group (default 100GB group size). They carry NO split offsets
    // (3-arg writeRecords), so a tiny target drives the fixed-size split fallback, shattering them
    // into small ranges bin-packed across several parallel sub-groups. All sub-groups of the one
    // parent share a single commit key, and no input file is lost.
    Table table = buildTable(4);
    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.<String, String>builder()
                        .put("min-input-files", "1")
                        .put("min-file-size-bytes", "0")
                        .put("target-file-size-bytes", "2")
                        .put("max-file-size-bytes", "3")
                        .build())
                .build());

    Set<Integer> keys = new HashSet<>();
    Set<String> distinctInputFiles = new HashSet<>();
    int subGroups = 0;
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      keys.add(kv.getKey());
      subGroups++;
      RewriteGroupTestHelpers.rewrittenDataFiles(kv.getValue(), table)
          .forEach(f -> distinctInputFiles.add(f.location()));
    }
    assertEquals("one parent group -> a single commit key", 1, keys.size());
    assertTrue("the group must split into multiple parallel sub-groups", subGroups > 1);
    assertEquals("no input file lost across sub-groups", 4, distinctInputFiles.size());
  }

  /**
   * A multi-row-group file splits into row-group RANGE tasks ({@code SplitScanTask}, non-zero
   * {@code start}) packed across bins, so a target below the file size lands its ranges in several
   * subgroups. That is safe only because parent-group atomicity commits all of a parent's subgroups
   * together: every subgroup holding a range of the file carries the same {@code parentGroupIndex},
   * and the parent's subgroup count equals the number of bins emitted.
   */
  @Test
  public void spanningFileRangesShareOneParentAcrossSubGroups() throws Exception {
    Table table = buildMultiRowGroupTable(1500);
    assertRowGroupsAtLeast(table, 3);
    String theFile = onlyDataFile(table);
    long target = totalDataFileBytes(table) / 3; // a few bins; the single file spans them

    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.of(
                        "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
                .build());

    assertTrue("the file must split into multiple sub-groups", planned.size() > 1);

    int subGroupsWithFile = 0;
    boolean sawRangeStart = false;
    Set<Integer> parentIndexes = new HashSet<>();
    Set<Integer> parentCounts = new HashSet<>();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      RewriteSubGroup g = kv.getValue();
      boolean has = false;
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(g, table)) {
        if (t.file().location().equals(theFile)) {
          has = true;
          if (t.start() > 0) {
            sawRangeStart = true;
          }
        }
      }
      if (has) {
        subGroupsWithFile++;
        parentIndexes.add(g.getParentGroupIndex());
        parentCounts.add(g.getParentSubgroupCount());
      }
    }
    assertTrue(
        "the file's row-group ranges must span >= 2 sub-groups, got " + subGroupsWithFile,
        subGroupsWithFile >= 2);
    assertTrue("at least one range task must start past the file beginning", sawRangeStart);
    assertEquals(
        "all sub-groups holding the file share one parent group index", 1, parentIndexes.size());
    assertEquals(
        "parentSubgroupCount must be uniform across the parent's sub-groups",
        1,
        parentCounts.size());
    assertEquals(
        "parentSubgroupCount must equal the number of emitted bins",
        Integer.valueOf(planned.size()),
        parentCounts.iterator().next());
  }

  /**
   * Row-group range splitting must lose no bytes. Collected across all sub-groups and sorted by
   * start, the file's range tasks must tile a contiguous span (each range ends exactly where the
   * next begins — Iceberg merges adjacent ranges within a bin, but across bins they still abut) and
   * reach the end of the whole-file task. A gap would drop rows; an overlap would double-read them.
   */
  @Test
  public void rangeSplitTilesTheFileContiguously() throws Exception {
    Table table = buildMultiRowGroupTable(1500);
    String theFile = onlyDataFile(table);
    long wholeLength = wholeFileTaskLength(table);
    long target = totalDataFileBytes(table) / 3;
    long firstSplitOffset;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      firstSplitOffset = it.iterator().next().file().splitOffsets().get(0);
    }

    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.of(
                        "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
                .build());

    List<long[]> ranges = Lists.newArrayList(); // (start, endExclusive)
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
        if (t.file().location().equals(theFile)) {
          ranges.add(new long[] {t.start(), t.start() + t.length()});
        }
      }
    }
    assertTrue("expected the file to be split into multiple ranges", ranges.size() >= 2);
    ranges.sort((a, b) -> Long.compare(a[0], b[0]));
    // Anchor the FIRST range to the file's first split offset (4 for standard Parquet, not 0). A
    // planner that dropped the first row group would still tile and reach EOF, but start too late.
    assertEquals(
        "the first range must start at the file's first split offset",
        firstSplitOffset,
        ranges.get(0)[0]);
    for (int i = 1; i < ranges.size(); i++) {
      assertEquals(
          "row-group ranges must abut with no gap or overlap between bins",
          ranges.get(i - 1)[1],
          ranges.get(i)[0]);
    }
    assertEquals(
        "the ranges must extend to the end of the file's readable bytes",
        wholeLength,
        ranges.get(ranges.size() - 1)[1]);
  }

  /**
   * A file WITHOUT usable split offsets (as non-Iceberg Parquet writers may produce) must still
   * split — the fixed-size fallback yields >=1 range that tiles {@code [0, fileLength)}. This is
   * the production-reachable path the offsets-based range tests don't cover; the fallback DOES
   * start at 0 (unlike Parquet's split offset[0] == 4).
   */
  @Test
  public void noOffsetsFileFallsBackToFixedSizeRangesStartingAtZero() throws Exception {
    Table table = buildNoOffsetsTable(1500);
    String theFile = onlyDataFile(table);
    long wholeLength = wholeFileTaskLength(table);
    long target = totalDataFileBytes(table) / 3;

    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.of(
                        "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
                .build());

    List<long[]> ranges = Lists.newArrayList();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
        if (t.file().location().equals(theFile)) {
          ranges.add(new long[] {t.start(), t.start() + t.length()});
        }
      }
    }
    assertFalse("a no-offsets file must still yield at least one range task", ranges.isEmpty());
    ranges.sort((a, b) -> Long.compare(a[0], b[0]));
    assertEquals("the fixed-size fallback's first range starts at 0", 0L, ranges.get(0)[0]);
    for (int i = 1; i < ranges.size(); i++) {
      assertEquals(
          "fallback ranges must tile contiguously", ranges.get(i - 1)[1], ranges.get(i)[0]);
    }
    assertEquals(
        "fallback ranges must reach the end of the file",
        wholeLength,
        ranges.get(ranges.size() - 1)[1]);
  }

  /**
   * When a multi-row-group file's ranges all fit ONE bin (target &gt;= file size), Iceberg merges
   * the adjacent ranges back into a SINGLE task spanning the whole file — not N per-row-group
   * tasks.
   */
  @Test
  public void adjacentRowGroupRangesMergeBackWhenTheyFitOneBin() throws Exception {
    Table table = buildMultiRowGroupTable(1500);
    assertRowGroupsAtLeast(table, 3);
    String theFile = onlyDataFile(table);
    long wholeLength = wholeFileTaskLength(table);
    long firstSplitOffset;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      firstSplitOffset = it.iterator().next().file().splitOffsets().get(0);
    }

    // rewrite-all + a huge target -> the whole file is one bin -> ranges merge to a single task.
    List<KV<Integer, RewriteSubGroup>> planned =
        planOnly(
            table,
            RewriteDataFiles.Configuration.builder()
                .setRewriteOptions(
                    ImmutableMap.of(
                        "min-input-files",
                        "1",
                        "rewrite-all",
                        "true",
                        "target-file-size-bytes",
                        String.valueOf(totalDataFileBytes(table) * 16)))
                .build());

    List<FileScanTask> fileTasks = new ArrayList<>();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (FileScanTask t : RewriteGroupTestHelpers.tasks(kv.getValue(), table)) {
        if (t.file().location().equals(theFile)) {
          fileTasks.add(t);
        }
      }
    }
    assertEquals(
        "all row groups fitting one bin must merge into exactly one task", 1, fileTasks.size());
    assertEquals(
        "the merged task must start at the first split offset",
        firstSplitOffset,
        fileTasks.get(0).start());
    assertEquals(
        "the merged task must span to the end of the file",
        wholeLength,
        fileTasks.get(0).start() + fileTasks.get(0).length());
  }

  /**
   * Planning the same table twice must produce the same multi-bin structure — deterministic
   * packing, so retry/idempotency reasoning holds. The size check also pins that the file really
   * does split into more than one sub-group.
   */
  @Test
  public void planningIsStableAcrossRuns() throws Exception {
    Table table = buildMultiRowGroupTable(1500);
    long target = totalDataFileBytes(table) / 3;
    RewriteDataFiles.Configuration config =
        RewriteDataFiles.Configuration.builder()
            .setRewriteOptions(
                ImmutableMap.of(
                    "min-input-files", "1", "target-file-size-bytes", String.valueOf(target)))
            .build();

    List<String> sig1 = rangeSignature(planOnly(table, config));
    List<String> sig2 = rangeSignature(planOnly(table, config));
    assertTrue("planning must split into multiple sub-groups", sig1.size() > 1);
    assertEquals("planned range boundaries must be identical across runs", sig1, sig2);
  }

  /** Sorted (start:length) signatures of every range task, for stable-plan comparison. */
  private static List<String> rangeSignature(List<KV<Integer, RewriteSubGroup>> planned) {
    List<String> sig = Lists.newArrayList();
    for (KV<Integer, RewriteSubGroup> kv : planned) {
      for (TaskDescriptor d : kv.getValue().getTaskDescriptors()) {
        sig.add(d.getStart() + ":" + d.getLength());
      }
    }
    java.util.Collections.sort(sig);
    return sig;
  }

  /**
   * Parquet writer properties that force many small row groups from little data: tiny row-group
   * size, dictionary off, tiny pages, capped check interval, uncompressed. Without them nothing
   * splits and the range tests below are vacuous.
   */
  private static final Map<String, String> MULTI_ROW_GROUP_PROPS =
      ImmutableMap.<String, String>builder()
          .put("write.parquet.row-group-size-bytes", "8192")
          .put("parquet.enable.dictionary", "false")
          .put("write.parquet.page-size-bytes", "1024")
          .put("write.parquet.row-group-check-max-record-count", "100")
          .put("write.parquet.compression-codec", "uncompressed")
          .build();

  /**
   * A table with a single file that spans many row groups (so its scan task splits per row group).
   */
  private Table buildMultiRowGroupTable(int records) throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "mrg_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    List<Record> rows = Lists.newArrayList();
    for (int i = 0; i < records; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i + "-" + (i * 2654435761L));
      rows.add(r);
    }
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords(
                "mrg_" + System.nanoTime() + ".parquet",
                table.schema(),
                rows,
                MULTI_ROW_GROUP_PROPS))
        .commit();
    table.refresh();
    return table;
  }

  /**
   * A single-file table whose {@link DataFile} records NO split offsets (3-arg {@code writeRecords}
   * omits them), so row-group range splitting must use the fixed-size fallback.
   */
  private Table buildNoOffsetsTable(int records) throws Exception {
    TableIdentifier id = TableIdentifier.of("default", "nooff_" + System.nanoTime());
    Table table = warehouse.createTable(id, TestFixtures.SCHEMA);
    List<Record> rows = Lists.newArrayList();
    for (int i = 0; i < records; i++) {
      Record r = GenericRecord.create(TestFixtures.SCHEMA);
      r.setField("id", (long) i);
      r.setField("data", "row-" + i + "-padding-0123456789abcdef0123456789abcdef");
      rows.add(r);
    }
    table
        .newAppend()
        .appendFile(
            warehouse.writeRecords("nooff_" + System.nanoTime() + ".parquet", table.schema(), rows))
        .commit();
    table.refresh();
    return table;
  }

  private List<KV<Integer, RewriteSubGroup>> planOnly(
      Table table, RewriteDataFiles.Configuration config) throws Exception {
    SerializableTable st = (SerializableTable) SerializableTable.copyOf(table);
    DoFnTester<SnapshotInfo, KV<Integer, RewriteSubGroup>> planTester =
        DoFnTester.of(new PlanRewriteGroups.ScanAndPlan(st, config));
    planTester.processBundle(SnapshotInfo.fromSnapshot(table.currentSnapshot()));
    return planTester.peekOutputElements(PlanRewriteGroups.GROUPS);
  }

  private static String onlyDataFile(Table table) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      List<FileScanTask> tasks = Lists.newArrayList(it);
      assertEquals("fixture must have exactly one file", 1, tasks.size());
      return tasks.get(0).file().location();
    }
  }

  private static long wholeFileTaskLength(Table table) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      return Lists.newArrayList(it).get(0).length();
    }
  }

  private static long totalDataFileBytes(Table table) throws Exception {
    long total = 0;
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        total += t.file().fileSizeInBytes();
      }
    }
    return total;
  }

  private static void assertRowGroupsAtLeast(Table table, int min) throws Exception {
    try (CloseableIterable<FileScanTask> it = table.newScan().planFiles()) {
      for (FileScanTask t : it) {
        List<Long> offsets = t.file().splitOffsets();
        int rowGroups = offsets == null ? 1 : offsets.size();
        assertTrue("fixture must have multi-row-group files; got " + rowGroups, rowGroups >= min);
      }
    }
  }
}
