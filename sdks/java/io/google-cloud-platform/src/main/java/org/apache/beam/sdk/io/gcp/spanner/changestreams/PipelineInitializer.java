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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import com.google.cloud.Timestamp;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;

/**
 * This class is responsible for initializing the Connector. It handles the creation of the
 * partition metadata table and the insertion of a fake partition (see {@link
 * InitialPartition#PARTITION_TOKEN}), which will be used to dispatch the change streams query.
 */
public class PipelineInitializer {

  /** Heartbeat interval for all change stream queries will be of 2 seconds. */
  // Be careful when changing this interval, as it needs to be less than the checkpointing interval
  // in Dataflow. Otherwise, if there are no records within checkpoint intervals, the consuming of
  // a change stream query might get stuck.
  private static final long DEFAULT_HEARTBEAT_MILLIS = 2000;

  /**
   * Creates a partition metadata table and creates an initial partition to serve as the parent of
   * all the partitions in the change stream query. This initial partition will be used to dispatch
   * the first change streams query in the job. The heartbeat interval to be used will be the one
   * specified in {@link PipelineInitializer#DEFAULT_HEARTBEAT_MILLIS}.
   *
   * @param partitionMetadataAdminDao DAO used to create the partition metadata table
   * @param partitionMetadataDao DAO used to insert the initial partition
   * @param inclusiveStartAt the change streams query start time
   * @param inclusiveEndAt the change streams query end time
   */
  public static void initialize(
      PartitionMetadataAdminDao partitionMetadataAdminDao,
      PartitionMetadataDao partitionMetadataDao,
      Timestamp inclusiveStartAt,
      @Nullable Timestamp inclusiveEndAt) {
    partitionMetadataAdminDao.createPartitionMetadataTable();
    createFakeParentPartition(partitionMetadataDao, inclusiveStartAt, inclusiveEndAt);
  }

  private static void createFakeParentPartition(
      PartitionMetadataDao partitionMetadataDao,
      Timestamp inclusiveStartAt,
      @Nullable Timestamp inclusiveEndAt) {
    PartitionMetadata parentPartition =
        PartitionMetadata.newBuilder()
            .setPartitionToken(InitialPartition.PARTITION_TOKEN)
            .setParentTokens(InitialPartition.PARENT_TOKENS)
            .setStartTimestamp(inclusiveStartAt)
            .setEndTimestamp(inclusiveEndAt)
            .setHeartbeatMillis(DEFAULT_HEARTBEAT_MILLIS)
            .setState(State.CREATED)
            .setWatermark(inclusiveStartAt)
            .build();
    partitionMetadataDao.insert(parentPartition);
  }
}
