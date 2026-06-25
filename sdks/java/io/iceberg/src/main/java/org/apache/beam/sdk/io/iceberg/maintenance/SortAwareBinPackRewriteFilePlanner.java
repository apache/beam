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

import static org.apache.beam.sdk.io.iceberg.maintenance.RewriteDataFiles.REWRITE_PREFIX;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.iceberg.util.PropertyUtil.propertyAsLong;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.maintenance.RewriteDataFiles.Configuration;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteJobOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.BinPackRewriteFilePlanner;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Groups specified data files in the {@link Table} into {@link RewriteFileGroup}s. The files are
 * grouped by partitions based on their size using fix sized bins. Extends {@link
 * SizeBasedFileRewritePlanner} with delete file number and delete ratio thresholds and job {@link
 * RewriteDataFiles#REWRITE_JOB_ORDER} handling.
 */
public class SortAwareBinPackRewriteFilePlanner extends BinPackRewriteFilePlanner {
  /**
   * The max number of files to be rewritten (Not providing this value would rewrite all the files)
   */
  public static final String MAX_FILES_TO_REWRITE = "max-files-to-rewrite";

  private static final Logger LOG =
      LoggerFactory.getLogger(SortAwareBinPackRewriteFilePlanner.class);

  private final Expression filter;
  private final @Nullable Long snapshotId;
  private final boolean caseSensitive;
  private final List<DataFile> oldFiles = Lists.newArrayList();
  private final Integer maxFilesToRewrite;
  private final RewriteJobOrder rewriteJobOrder;
  private final boolean rewriteAll;
  private final long targetFileSize;

  /**
   * Creates the planner for the given table.
   *
   * @param table to plan for
   * @param filter used to remove files from the plan
   * @param rewriteConfig used to configure the rewrite planner
   */
  public SortAwareBinPackRewriteFilePlanner(
      Table table, @Nullable Expression filter, Configuration rewriteConfig) {
    super(table);
    this.filter = filter != null ? filter : Expressions.alwaysTrue();
    this.snapshotId = rewriteConfig.getSnapshotId();
    this.caseSensitive = Boolean.TRUE.equals(rewriteConfig.getCaseSensitive());

    Map<String, String> options =
        firstNonNull(rewriteConfig.getRewriteOptions(), Collections.emptyMap());
    this.rewriteJobOrder =
        RewriteJobOrder.fromName(
            PropertyUtil.propertyAsString(
                options,
                RewriteDataFiles.REWRITE_JOB_ORDER,
                RewriteDataFiles.REWRITE_JOB_ORDER_DEFAULT));
    this.maxFilesToRewrite = PropertyUtil.propertyAsNullableInt(options, MAX_FILES_TO_REWRITE);
    Preconditions.checkArgument(
        maxFilesToRewrite == null || maxFilesToRewrite > 0,
        "Cannot set %s to %s, the value must be positive integer.",
        MAX_FILES_TO_REWRITE,
        maxFilesToRewrite);
    this.rewriteAll = PropertyUtil.propertyAsBoolean(options, REWRITE_ALL, REWRITE_ALL_DEFAULT);
    this.targetFileSize =
        propertyAsLong(
            options,
            TARGET_FILE_SIZE_BYTES,
            propertyAsLong(
                table.properties(),
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT));
  }

  public List<RewriteFileGroup> beamPlan() {
    StructLikeMap<List<List<FileScanTask>>> plan = planFileGroups();
    RewriteExecutionContext ctx = new RewriteExecutionContext();
    List<RewriteFileGroup> selectedFileGroups = Lists.newArrayList();
    AtomicInteger fileCountRunner = new AtomicInteger();

    plan.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .forEach(
            entry -> {
              StructLike partition = entry.getKey();
              entry
                  .getValue()
                  .forEach(
                      fileScanTasks -> {
                        long inputSize = inputSize(fileScanTasks);
                        if (maxFilesToRewrite == null) {
                          selectedFileGroups.add(
                              newRewriteGroup(
                                  ctx,
                                  partition,
                                  fileScanTasks,
                                  inputSplitSize(inputSize),
                                  expectedOutputFiles(inputSize)));
                        } else if (fileCountRunner.get() < maxFilesToRewrite) {
                          int remainingSize = maxFilesToRewrite - fileCountRunner.get();
                          int scanTasksToRewrite = Math.min(fileScanTasks.size(), remainingSize);
                          selectedFileGroups.add(
                              newRewriteGroup(
                                  ctx,
                                  partition,
                                  fileScanTasks.subList(0, scanTasksToRewrite),
                                  inputSplitSize(inputSize),
                                  expectedOutputFiles(inputSize)));
                          fileCountRunner.getAndAdd(scanTasksToRewrite);
                        }
                        fileScanTasks.forEach(task -> oldFiles.add(task.file()));
                      });
            });
    return selectedFileGroups.stream()
        .sorted(RewriteFileGroup.comparator(rewriteJobOrder))
        .collect(Collectors.toList());
  }

  public List<DataFile> getOldFiles() {
    return oldFiles;
  }

  private StructLikeMap<List<List<FileScanTask>>> planFileGroups() {
    TableScan scan =
        table().newScan().filter(filter).caseSensitive(caseSensitive).ignoreResiduals();

    if (snapshotId != null) {
      scan = scan.useSnapshot(snapshotId);
    }

    Types.StructType partitionType = table().spec().partitionType();
    CloseableIterable<FileScanTask> fileScanTasks = scan.planFiles();

    try {
      List<FileScanTask> sortedTasks =
          FileScanTaskSorter.sortByTableSortOrder(fileScanTasks, table().sortOrder());
      StructLikeMap<List<FileScanTask>> filesByPartition =
          groupByPartition(table(), partitionType, sortedTasks);
      System.out.println("xxx partitions: " + filesByPartition.keySet());
      return filesByPartition.transformValues(tasks -> ImmutableList.copyOf(planFileGroups(tasks)));
    } finally {
      try {
        fileScanTasks.close();
      } catch (IOException io) {
        LOG.error("Cannot properly close file iterable while planning for rewrite", io);
      }
    }
  }

  private StructLikeMap<List<FileScanTask>> groupByPartition(
      Table table, Types.StructType partitionType, List<FileScanTask> tasks) {
    StructLikeMap<List<FileScanTask>> filesByPartition = StructLikeMap.create(partitionType);
    StructLike emptyStruct = GenericRecord.create(partitionType);

    for (FileScanTask task : tasks) {
      // If a task uses an incompatible partition spec the data inside could contain values
      // which belong to multiple partitions in the current spec. Treating all such files as
      // un-partitioned and grouping them together helps to minimize new files made.
      StructLike taskPartition =
          task.file().specId() == table.spec().specId() ? task.file().partition() : emptyStruct;

      filesByPartition.computeIfAbsent(taskPartition, unused -> Lists.newArrayList()).add(task);
    }

    LOG.info(
        REWRITE_PREFIX + "Planning compaction across {} partition(s).", filesByPartition.size());
    return filesByPartition;
  }

  @Override
  public Iterable<List<FileScanTask>> planFileGroups(Iterable<FileScanTask> tasks) {
    Iterable<FileScanTask> filteredTasks = rewriteAll ? tasks : filterFiles(tasks);
    BinPacking.ListPacker<FileScanTask> packer =
        new BinPacking.ListPacker<>(targetFileSize, 1, false);
    List<List<FileScanTask>> groups = packer.pack(filteredTasks, ContentScanTask::length);
    return rewriteAll ? groups : filterFileGroups(groups);
  }

  private RewriteFileGroup newRewriteGroup(
      RewriteExecutionContext ctx,
      StructLike partition,
      List<FileScanTask> tasks,
      long inputSplitSize,
      int expectedOutputFiles) {
    return RewriteFileGroup.builder()
        .setGlobalIndex(ctx.currentGlobalIndex())
        .setPartitionIndex(ctx.currentPartitionIndex(partition))
        .setPartitionPath(table().spec().partitionToPath(partition))
        .setFileScanTasks(tasks)
        .setOutputSpecId(outputSpecId())
        .setWriteMaxFileSize(writeMaxFileSize())
        .setInputSplitSize(inputSplitSize)
        .setExpectedOutputFiles(expectedOutputFiles)
        .build();
  }

  protected static class RewriteExecutionContext {
    private final Map<StructLike, Integer> partitionIndexMap;
    private final AtomicInteger groupIndex;

    protected RewriteExecutionContext() {
      this.partitionIndexMap = Maps.newConcurrentMap();
      this.groupIndex = new AtomicInteger(1);
    }

    protected int currentGlobalIndex() {
      return groupIndex.getAndIncrement();
    }

    protected int currentPartitionIndex(StructLike partition) {
      return partitionIndexMap.merge(partition, 1, Integer::sum);
    }
  }
}
