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
package org.apache.beam.sdk.io.gcp.spanner.cdc.dao;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class PartitionMetadataDao {

  // Metadata table column names
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_PARENT_TOKEN = "ParentToken";
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  public static final String COLUMN_INCLUSIVE_START = "InclusiveStart";
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  public static final String COLUMN_INCLUSIVE_END = "InclusiveEnd";
  public static final String COLUMN_HEARTBEAT_SECONDS = "HeartbeatSeconds";
  public static final String COLUMN_STATE = "State";
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  public static final String COLUMN_UPDATED_AT = "UpdatedAt";

  private final DatabaseClient databaseClient;
  private final String tableName;

  public PartitionMetadataDao(DatabaseClient databaseClient, String tableName) {
    this.databaseClient = databaseClient;
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public Timestamp insert(PartitionMetadata partitionMetadata) {
    Mutation mutation =
        Mutation.newInsertBuilder(this.tableName)
            .set(COLUMN_PARTITION_TOKEN)
            .to(partitionMetadata.getPartitionToken())
            .set(COLUMN_PARENT_TOKEN)
            .toStringArray(partitionMetadata.getParentTokens())
            .set(COLUMN_START_TIMESTAMP)
            .to(partitionMetadata.getStartTimestamp())
            .set(COLUMN_INCLUSIVE_START)
            .to(partitionMetadata.isInclusiveStart())
            .set(COLUMN_END_TIMESTAMP)
            .to(partitionMetadata.getEndTimestamp())
            .set(COLUMN_INCLUSIVE_END)
            .to(partitionMetadata.isInclusiveEnd())
            .set(COLUMN_HEARTBEAT_SECONDS)
            .to(partitionMetadata.getHeartbeatSeconds())
            .set(COLUMN_STATE)
            .to(partitionMetadata.getState().toString())
            .set(COLUMN_CREATED_AT)
            .to(partitionMetadata.getCreatedAt())
            .set(COLUMN_UPDATED_AT)
            .to(partitionMetadata.getUpdatedAt())
            .build();
    return databaseClient.write(ImmutableList.of(mutation));
  }
}
