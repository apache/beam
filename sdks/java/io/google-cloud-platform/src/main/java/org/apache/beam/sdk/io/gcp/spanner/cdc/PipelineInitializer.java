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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.Timestamp;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataAdminDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;

public class PipelineInitializer {

  private static final long DEFAULT_HEARTBEAT_MILLIS = 5000;

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
            .setInclusiveStart(true)
            .setEndTimestamp(inclusiveEndAt)
            .setInclusiveEnd(true)
            .setHeartbeatMillis(DEFAULT_HEARTBEAT_MILLIS)
            .setState(State.CREATED)
            .build();
    partitionMetadataDao.insert(parentPartition);
  }
}
