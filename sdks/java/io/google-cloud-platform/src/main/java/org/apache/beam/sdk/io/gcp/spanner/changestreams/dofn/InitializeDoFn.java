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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A DoFn responsible for initializing the change stream Connector. It handles the creation of the
 * partition metadata table and the insertion of a fake partition (see {@link
 * InitialPartition#PARTITION_TOKEN}), which will be used to dispatch the change streams query.
 */
public class InitializeDoFn extends DoFn<byte[], PartitionMetadata> implements Serializable {

  private static final long serialVersionUID = -8921188388649003102L;

  private final long heartbeatMillis;

  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  // The change streams query start time
  private final com.google.cloud.Timestamp startTimestamp;
  // The change streams query end time
  private final com.google.cloud.Timestamp endTimestamp;

  public InitializeDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      com.google.cloud.Timestamp startTimestamp,
      com.google.cloud.Timestamp endTimestamp,
      long heartbeatMillis) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.startTimestamp = startTimestamp;
    this.endTimestamp = endTimestamp;
    this.heartbeatMillis = heartbeatMillis;
  }

  @ProcessElement
  public void processElement(OutputReceiver<PartitionMetadata> receiver) {
    PartitionMetadataDao partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    if (!partitionMetadataDao.tableExists()) {
      // Creates partition metadata table and associated indexes
      daoFactory.getPartitionMetadataAdminDao().createPartitionMetadataTable();
      createFakeParentPartition();
    }
    if (daoFactory.getTvfNameList().isEmpty()) {
      // For IMMUTABLE_KEY_RANGE change stream or MUTABLE_KEY_RANGE change stream without the
      // specified tvf name list.
      final PartitionMetadata initialPartition =
          Optional.ofNullable(
                  partitionMetadataDao.getPartition(
                      PartitionMetadataDao.composePartitionTokenWithTvfName(
                          InitialPartition.PARTITION_TOKEN,
                          ChangeStreamsConstants.DEFAULT_TVF_NAME)))
              .map(mapperFactory.partitionMetadataMapper()::from)
              .orElseThrow(
                  () ->
                      new IllegalStateException("Initial partition not found in metadata table."));
      receiver.output(initialPartition);
    } else {
      // For MUTABLE_KEY_RANGE change stream with the specified tvf name list.
      // We only need to output ONE initial partition to the next stage because
      // DetectNewPartitionsDoFn will discover all fake parent partitions created in the same
      // Spanner
      // transaction with the same CreatedAt timestamp and schedule them properly in on batch.
      String firstTvfName = daoFactory.getTvfNameList().get(0);
      final PartitionMetadata initialPartition =
          Optional.ofNullable(
                  partitionMetadataDao.getPartition(
                      PartitionMetadataDao.composePartitionTokenWithTvfName(
                          InitialPartition.PARTITION_TOKEN, firstTvfName)))
              .map(mapperFactory.partitionMetadataMapper()::from)
              .orElseThrow(
                  () ->
                      new IllegalStateException("Initial partition not found in metadata table."));
      receiver.output(initialPartition);
    }
  }

  /**
   * Creates an initial partition in the partition metadata table to serve as the parent of all the
   * partitions in the change stream query. This initial partition will be used to dispatch the
   * first change streams query in the job. The heartbeat interval to be used will be the one
   * specified in {@link InitializeDoFn#DEFAULT_HEARTBEAT_MILLIS}.
   */
  private void createFakeParentPartition() {
    if (daoFactory.getTvfNameList().isEmpty()) {
      // For IMMUTABLE_KEY_RANGE change stream or MUTABLE_KEY_RANGE
      // change stream without the specified tvf name list.
      PartitionMetadata parentPartition =
          PartitionMetadata.newBuilder()
              .setPartitionToken(InitialPartition.PARTITION_TOKEN)
              .setTvfName(ChangeStreamsConstants.DEFAULT_TVF_NAME)
              .setStartTimestamp(startTimestamp)
              .setEndTimestamp(endTimestamp)
              .setHeartbeatMillis(heartbeatMillis)
              .setState(State.CREATED)
              .setWatermark(startTimestamp)
              .build();
      daoFactory.getPartitionMetadataDao().insert(parentPartition);
    } else {
      // For MUTABLE_KEY_RANGE change stream with the specified tvf name list.
      List<PartitionMetadata> parentPartitions = new ArrayList<>();
      for (String tvfName : daoFactory.getTvfNameList()) {
        PartitionMetadata parentPartition =
            PartitionMetadata.newBuilder()
                .setPartitionToken(InitialPartition.PARTITION_TOKEN)
                .setTvfName(tvfName)
                .setStartTimestamp(startTimestamp)
                .setEndTimestamp(endTimestamp)
                .setHeartbeatMillis(heartbeatMillis)
                .setState(State.CREATED)
                .setWatermark(startTimestamp)
                .build();
        parentPartitions.add(parentPartition);
      }
      daoFactory.getPartitionMetadataDao().insert(parentPartitions);
    }
  }
}
