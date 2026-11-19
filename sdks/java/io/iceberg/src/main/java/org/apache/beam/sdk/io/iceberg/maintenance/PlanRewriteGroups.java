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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.iceberg.FilterUtils;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.TableScanUtil;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the table once with Iceberg's native {@link BinPackRewriteFilePlanner} and produces
 * bin-pack rewrite groups, keyed by a "commit key" so downstream commits can be batched.
 *
 * <p><b>Intra-group parallelism.</b> Each planned group is split into <i>subgroups</i> by packing
 * its files' row-group <b>ranges</b> into bins of ~one target-sized output file (via {@code
 * inputSplitSize}), so a large group is rewritten by many workers in parallel while undersized
 * files combine into target-sized outputs. All subgroups of a parent share its commit key and
 * commit together as one batch: a range-split file can span subgroups, so committing the parent
 * atomically ensures a shared file is never partially replaced.
 *
 * <p><b>Convergence caveats.</b> A group may leave one remainder bin below {@code
 * min-file-size-bytes} — it converges on a later run or stays as one stable small file. A
 * single-row-group file whose size sits in the rewrite band cannot be split further and is
 * rewritten ~1:1. Delete-heavy groups shrink once their deletes are applied, converging on the
 * following run. Spec-changing rewrites ({@code output-spec-id} / post-evolution) fan a file out
 * across output partitions, bounded by {@link WriterFactory}.
 */
class PlanRewriteGroups extends PTransform<PCollection<SnapshotInfo>, PCollectionTuple> {
  /** Main output: the planned rewrite subgroups, keyed by commit key. */
  static final TupleTag<KV<Integer, RewriteSubGroup>> GROUPS = new TupleTag<>() {};

  /** One {@link RewriteResult} planning fragment per processed impulse element (always emitted). */
  static final TupleTag<RewriteResult> PLAN_SUMMARY = new TupleTag<>() {};

  private final SerializableTable table;
  private final RewriteDataFiles.Configuration config;

  PlanRewriteGroups(SerializableTable table, RewriteDataFiles.Configuration config) {
    this.table = table;
    this.config = config;
  }

  @Override
  public PCollectionTuple expand(PCollection<SnapshotInfo> input) {
    PCollectionTuple planned =
        input.apply(
            "Scan and Plan Rewrite Groups",
            ParDo.of(new ScanAndPlan(table, config))
                .withOutputTags(GROUPS, TupleTagList.of(PLAN_SUMMARY)));
    planned.get(GROUPS).setCoder(KvCoder.of(VarIntCoder.of(), RewriteDataFiles.SUB_GROUP_CODER));
    planned.get(PLAN_SUMMARY).setCoder(RewriteDataFiles.RESULT_CODER);
    return planned;
  }

  static class ScanAndPlan extends DoFn<SnapshotInfo, KV<Integer, RewriteSubGroup>> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanAndPlan.class);
    private final SerializableTable table;
    private final RewriteDataFiles.Configuration config;

    private static final Counter plannedGroups =
        Metrics.counter(PlanRewriteGroups.class, "plannedGroups");
    private static final Counter plannedPartitionsToRewrite =
        Metrics.counter(PlanRewriteGroups.class, "plannedPartitionsToRewrite");
    private static final Counter plannedFilesToRewrite =
        Metrics.counter(PlanRewriteGroups.class, "plannedFilesToRewrite");
    private static final Counter plannedBytesToRewrite =
        Metrics.counter(PlanRewriteGroups.class, "plannedBytesToRewrite");
    // Size of each DISTINCT planned input file
    private static final Distribution fileByteSizeToRewrite =
        Metrics.distribution(PlanRewriteGroups.class, "fileByteSizeToRewrite");

    ScanAndPlan(SerializableTable table, RewriteDataFiles.Configuration config) {
      this.table = table;
      this.config = config;
    }

    @ProcessElement
    public void process(@Element SnapshotInfo element, MultiOutputReceiver out) {
      long startSnap;
      @Nullable String branch = config.getBranch();
      if (config.getSnapshotId() != null) {
        // An explicit snapshot id always wins.
        startSnap = config.getSnapshotId();
      } else if (branch != null) {
        // Write-audit-publish: plan the BRANCH head, not main's current snapshot.
        SnapshotRef ref = table.refs().get(branch);
        if (ref == null) {
          throw new IllegalArgumentException(
              "Cannot rewrite branch '" + branch + "': the branch does not exist.");
        }
        startSnap = ref.snapshotId();
      } else {
        startSnap = element.getSnapshotId();
      }
      // The starting snapshot's sequence number floors the commit's idempotency-stamp scan, which
      // keeps working even if that snapshot is later expired.
      long startSequenceNumber =
          checkStateNotNull(
                  table.snapshot(startSnap), "Starting snapshot %s not found in table", startSnap)
              .sequenceNumber();
      // Carried in each group: names and tags the output files, and stamps commits for idempotency.
      String operationId = UUID.randomUUID().toString();
      Expression filter =
          config.getFilter() != null
              ? FilterUtils.convert(config.getFilter(), table.schema())
              : Expressions.alwaysTrue();

      BinPackRewriteFilePlanner planner =
          new BinPackRewriteFilePlanner(table, filter, startSnap, config.caseSensitive());
      planner.init(
          config.getRewriteOptions() != null ? config.getRewriteOptions() : Collections.emptyMap());

      int maxCommits = config.partialProgressEnabled() ? config.maxCommits() : 1;
      long totalRunningBytes = 0L;
      long maxRewriteBytes = config.maxRewriteBytes();

      Set<String> plannedFiles = new HashSet<>();
      Set<String> partitionPaths = new HashSet<>();
      int plannedGroupIndex =
          0; // running index among KEPT parents; drives the round-robin commit key
      int globalIndex = 0;
      // Emit each kept parent's subgroups as the planner produces them. commitKey is round-robin
      // over maxCommits; atomic mode has maxCommits=1, so everything lands on key 0.
      try (CloseableIterator<org.apache.iceberg.actions.RewriteFileGroup> it =
          planner.plan().groups().iterator()) {
        while (it.hasNext()) {
          org.apache.iceberg.actions.RewriteFileGroup group = it.next();
          long groupBytes = group.inputFilesSizeInBytes();
          // Skip groups that would push us over the byte budget; a smaller later group may fit.
          if (totalRunningBytes + groupBytes > maxRewriteBytes) {
            continue;
          }
          totalRunningBytes += groupBytes;

          String partitionPath = table.spec().partitionToPath(group.info().partition());
          partitionPaths.add(partitionPath);
          int commitKey = plannedGroupIndex % maxCommits;

          // A planned parent group covers several target output files, so split it into subgroups
          // by packing its files' row-group RANGES into bins of ~one target-sized output.
          // parentSubgroupCount is the number of bins emitted, which the commit stage uses to
          // verify completeness before replacing the parent's input files.
          List<ScanTaskGroup<FileScanTask>> bins =
              planSubGroupBins(group.fileScanTasks(), group.inputSplitSize());
          int parentSubgroupCount = bins.size();
          for (ScanTaskGroup<FileScanTask> bin : bins) {
            globalIndex++;
            List<FileScanTask> subTasks = new ArrayList<>(bin.tasks());
            RewriteSubGroup beamGroup =
                RewriteSubGroup.builder()
                    .setGlobalIndex(globalIndex)
                    .setParentGroupIndex(plannedGroupIndex)
                    .setParentSubgroupCount(parentSubgroupCount)
                    .setFileScanTasks(subTasks, table.specs())
                    .setOutputSpecId(group.outputSpecId())
                    .setWriteMaxFileSize(group.maxOutputFileSize())
                    .setStartingSnapshotId(startSnap)
                    .setStartingSequenceNumber(startSequenceNumber)
                    .setOperationId(operationId)
                    .build();
            for (FileScanTask t : subTasks) {
              if (plannedFiles.add(t.file().location())) {
                fileByteSizeToRewrite.update(t.file().fileSizeInBytes());
              }
            }
            out.get(GROUPS).output(KV.of(commitKey, beamGroup));
          }
          plannedGroupIndex++;
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to plan rewrite groups", e);
      }

      // Exactly ONE planning fragment per impulse element
      out.get(PLAN_SUMMARY)
          .output(
              RewriteResult.builder()
                  .setOperationId(operationId)
                  .setStartingSnapshotId(startSnap)
                  .setPlannedParentGroups(plannedGroupIndex)
                  .setPlannedFiles(plannedFiles.size())
                  .setPlannedBytes(totalRunningBytes)
                  .build());

      if (plannedGroupIndex == 0) {
        LOG.info("No rewrite groups planned for snapshot {}.", startSnap);
        return;
      }

      LOG.info(
          "Planned {} rewrite group(s) -> {} parallel subgroup(s) across {} partition(s) for "
              + "snapshot {}.",
          plannedGroupIndex,
          globalIndex,
          partitionPaths.size(),
          startSnap);

      plannedGroups.inc(plannedGroupIndex);
      plannedFilesToRewrite.inc(plannedFiles.size());
      plannedBytesToRewrite.inc(totalRunningBytes);
      plannedPartitionsToRewrite.inc(partitionPaths.size());
    }

    /**
     * Packs a planned group's files into subgroup bins. Each file is split by its row-group
     * <b>ranges</b>, the ranges are bin-packed to {@code splitSize} capacity, and adjacent ranges
     * of the same file within a bin are merged back into one contiguous range. Undersized files
     * therefore combine into target-sized outputs while a large file is spread across several bins.
     *
     * <p>A file's ranges landing in several bins is safe: parent-group atomicity (see {@link
     * CommitRewriteGroups}) commits all of a parent's subgroups together, so a shared input file is
     * deleted only once every bin that read part of it has committed. A dropped or failed bin drops
     * the whole parent, orphaning its output files.
     */
    private static List<ScanTaskGroup<FileScanTask>> planSubGroupBins(
        List<FileScanTask> tasks, long splitSize) {
      long effectiveSplitSize = Math.max(1L, splitSize);
      return TableScanUtil.planTaskGroups(
          tasks, effectiveSplitSize, /* lookback= */ 10, /* openFileCost= */ 0L);
    }
  }
}
