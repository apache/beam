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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.action;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.coverSameKeySpace;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessNewPartitionsAction {
  private static final Logger LOG = LoggerFactory.getLogger(ProcessNewPartitionsAction.class);

  private final ChangeStreamMetrics metrics;
  private final MetadataTableDao metadataTableDao;
  @Nullable private final Instant endTime;

  public ProcessNewPartitionsAction(
      ChangeStreamMetrics metrics, MetadataTableDao metadataTableDao, @Nullable Instant endTime) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.endTime = endTime;
  }

  /**
   * Process a single new partition. New partition resulting from split and merges need to be
   * outputted to be streamed. Regardless if it's a split or a merge, we have the same verification
   * process in order to ensure the new partition can actually be streamed.
   *
   * <p>When a parent partition splits, it receives two or more new partitions. It will write a new
   * row, with the new row ranges as row key, for each new partition. These new partitions can be
   * immediately streamed.
   *
   * <p>The complicated scenario is merges. Two or more parent partitions will merge into one new
   * partition. Each parent partition receives the same new partition (row range) but each parent
   * partition will have a different continuation token. The parent partitions will all write to the
   * same row key form by the new row range. Each parent will record its continuation token, and
   * watermark. Parent partitions may not receive the message to stop at the same time. So when we
   * try to process the new partition, we need to ensure that all the parent partitions have stopped
   * and recorded their metadata table. We do so by verifying that the row ranges of the parents
   * covers a contiguous block of row range that is same as the new row range.
   *
   * <p>For example, partition1, A-B, and partition2, B-C, merges into partition3, A-C.
   *
   * <ol>
   *   <li>p1 writes to row A-C in metadata table
   *   <li>processNewPartition process A-C seeing that only A-B has been recorded and A-B does not
   *       cover A-C. Do Nothing
   *   <li>p2 writes to row A-C in metadata table
   *   <li>processNewPartition process A-C again, seeing that A-B and B-C has been recorded and
   *       outputs new partition A-C to be streamed.
   * </ol>
   *
   * <p>Note that, the algorithm to verify if a merge is valid, also correctly verifies if a split
   * is valid. A split is immediately valid as long as the row exists because there's only one
   * parent that needs to write to that row.
   *
   * @param newPartition new partition to be processed
   * @param receiver to output new partitions
   */
  public boolean processNewPartition(
      NewPartition newPartition, OutputReceiver<PartitionRecord> receiver) {
    List<ByteStringRange> parentPartitions = newPartition.getParentPartitions();
    if (!coverSameKeySpace(parentPartitions, newPartition.getPartition())) {
      LOG.warn("DNP: New partition does not have all the parents: {}", newPartition);
      return false;
    }

    String uuid = UniqueIdGenerator.getNextId();
    PartitionRecord partitionRecord =
        new PartitionRecord(
            newPartition.getPartition(),
            newPartition.getChangeStreamContinuationTokens(),
            uuid,
            newPartition.getLowWatermark(),
            Collections.singletonList(newPartition),
            endTime);
    if (parentPartitions.size() > 1) {
      metrics.incPartitionMergeCount();
    } else {
      metrics.incPartitionSplitCount();
    }
    LOG.info("DNP: Outputting new partition: {}", partitionRecord);
    metadataTableDao.markNewPartitionForDeletion(newPartition);
    // We are outputting elements with timestamp of 0 to prevent reliance on event time. This limits
    // the ability to window on commit time of any data changes. It is still possible to window on
    // processing time.
    receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
    return true;
  }
}
