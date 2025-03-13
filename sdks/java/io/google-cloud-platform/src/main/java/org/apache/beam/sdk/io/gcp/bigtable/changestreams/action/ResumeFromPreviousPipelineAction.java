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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
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

public class ResumeFromPreviousPipelineAction {
  private static final Logger LOG = LoggerFactory.getLogger(ResumeFromPreviousPipelineAction.class);

  private final ChangeStreamMetrics metrics;
  private final MetadataTableDao metadataTableDao;
  @Nullable private final Instant endTime;
  private final ProcessNewPartitionsAction processNewPartitionsAction;

  public ResumeFromPreviousPipelineAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      @Nullable Instant endTime,
      ProcessNewPartitionsAction processNewPartitionsAction) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.endTime = endTime;
    this.processNewPartitionsAction = processNewPartitionsAction;
  }

  /**
   * Resume from previously drained pipeline. Restart StreamPartitions by reading the last metadata
   * write and output PartitionRecord based on that. Process NewPartitions normally.
   *
   * <ol>
   *   <li>Clear out missing partitions metadata in the metadata table
   *   <li>Read all StreamPartitions and output them
   *   <li>Read all NewPartitions and output them normally
   * </ol>
   *
   * @param receiver output receiver to output
   * @throws InvalidProtocolBufferException when failing to read from metadata table.
   */
  public void run(OutputReceiver<PartitionRecord> receiver) throws InvalidProtocolBufferException {
    // Erase any existing missing partitions.
    metadataTableDao.writeDetectNewPartitionMissingPartitions(new HashMap<>());

    List<PartitionRecord> partitions = metadataTableDao.readAllStreamPartitions();

    for (PartitionRecord partitionRecord : partitions) {
      if (partitionRecord.getUuid().isEmpty()) {
        partitionRecord.setUuid(UniqueIdGenerator.getNextId());
      }
      if (endTime != null) {
        partitionRecord.setEndTime(endTime);
      }
      LOG.info("DNP: Outputting existing partition: {}", partitionRecord);
      metrics.incListPartitionsCount();
      receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
    }

    List<NewPartition> newPartitions = metadataTableDao.readNewPartitionsIncludingDeleted();

    for (NewPartition newPartition : newPartitions) {
      processNewPartitionsAction.processNewPartition(newPartition, receiver);
    }
  }
}
