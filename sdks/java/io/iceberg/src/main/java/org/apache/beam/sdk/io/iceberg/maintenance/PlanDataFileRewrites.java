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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.FilterUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.expressions.Expression;
import org.checkerframework.checker.nullness.qual.Nullable;

class PlanDataFileRewrites extends PTransform<PCollection<SnapshotInfo>, PCollectionTuple> {
  private final SerializableTable table;
  private final RewriteDataFiles.Configuration rewriteConfig;
  static final TupleTag<RewriteFileGroup> REWRITE_GROUPS = new TupleTag<>() {};
  static final TupleTag<SerializableDataFile> OLD_FILES = new TupleTag<>() {};

  PlanDataFileRewrites(SerializableTable table, RewriteDataFiles.Configuration rewriteConfig) {
    this.table = table;
    this.rewriteConfig = rewriteConfig;
  }

  @Override
  public PCollectionTuple expand(PCollection<SnapshotInfo> input) {

    return input.apply(
        ParDo.of(new ScanAndCreatePlan(table, rewriteConfig))
            .withOutputTags(REWRITE_GROUPS, TupleTagList.of(OLD_FILES)));
  }

  static class ScanAndCreatePlan extends DoFn<SnapshotInfo, RewriteFileGroup> {
    private final SerializableTable table;
    private final RewriteDataFiles.Configuration rewriteConfig;
    private final @Nullable Expression filter;
    private static final Counter plannedFilesToRewrite =
        Metrics.counter(ScanAndCreatePlan.class, "plannedFilesToRewrite");
    private static final Counter plannedBytesToRewrite =
        Metrics.counter(ScanAndCreatePlan.class, "plannedBytesToRewrite");
    private static final Counter plannedPartitionsToRewrite =
        Metrics.counter(ScanAndCreatePlan.class, "plannedPartitionsToRewrite");
    private static final Counter plannedOutputFiles =
        Metrics.counter(ScanAndCreatePlan.class, "plannedOutputFiles");

    ScanAndCreatePlan(SerializableTable table, RewriteDataFiles.Configuration rewriteConfig) {
      this.table = table;
      this.rewriteConfig = rewriteConfig;
      this.filter =
          rewriteConfig.getFilter() != null
              ? FilterUtils.convert(rewriteConfig.getFilter(), table.schema())
              : null;
    }

    @ProcessElement
    public void process(MultiOutputReceiver output) {
      SortAwareBinPackRewriteFilePlanner planner =
          new SortAwareBinPackRewriteFilePlanner(table, filter, rewriteConfig);
      planner.init(firstNonNull(rewriteConfig.getRewriteOptions(), Collections.emptyMap()));
      int totalFilesToRewrite = 0;
      long totalBytesToRewrite = 0L;
      int totalPlannedOutputFiles = 0;
      Set<String> partitionPaths = new HashSet<>();

      for (RewriteFileGroup group : planner.beamPlan()) {
        output.get(REWRITE_GROUPS).output(group);

        partitionPaths.add(group.getPartitionPath());
        totalBytesToRewrite += group.getTotalInputFileByteSize();
        totalFilesToRewrite += group.numInputFiles();
        totalPlannedOutputFiles++;
      }
      for (DataFile file : planner.getOldFiles()) {
        SerializableDataFile serializableFile = SerializableDataFile.from(file, table.specs());
        output.get(OLD_FILES).output(serializableFile);
      }

      plannedFilesToRewrite.inc(totalFilesToRewrite);
      plannedBytesToRewrite.inc(totalBytesToRewrite);
      plannedPartitionsToRewrite.inc(partitionPaths.size());
      plannedOutputFiles.inc(totalPlannedOutputFiles);
    }
  }
}
