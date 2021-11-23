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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_CREATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_FINISHED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_RUNNING_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_SCHEDULED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_WATERMARK;

import com.google.cloud.spanner.ResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

public class PartitionMetadataMapper {

  PartitionMetadataMapper() {}

  public PartitionMetadata from(ResultSet resultSet) {
    return PartitionMetadata.newBuilder()
        .setPartitionToken(resultSet.getString(COLUMN_PARTITION_TOKEN))
        .setParentTokens(Sets.newHashSet(resultSet.getStringList(COLUMN_PARENT_TOKENS)))
        .setStartTimestamp(resultSet.getTimestamp(COLUMN_START_TIMESTAMP))
        .setEndTimestamp(
            !resultSet.isNull(COLUMN_END_TIMESTAMP)
                ? resultSet.getTimestamp(COLUMN_END_TIMESTAMP)
                : null)
        .setHeartbeatMillis(resultSet.getLong(COLUMN_HEARTBEAT_MILLIS))
        .setState(State.valueOf(resultSet.getString(COLUMN_STATE)))
        .setWatermark(resultSet.getTimestamp(COLUMN_WATERMARK))
        .setCreatedAt(resultSet.getTimestamp(COLUMN_CREATED_AT))
        .setScheduledAt(
            !resultSet.isNull(COLUMN_SCHEDULED_AT)
                ? resultSet.getTimestamp(COLUMN_SCHEDULED_AT)
                : null)
        .setRunningAt(
            !resultSet.isNull(COLUMN_RUNNING_AT) ? resultSet.getTimestamp(COLUMN_RUNNING_AT) : null)
        .setFinishedAt(
            !resultSet.isNull(COLUMN_FINISHED_AT)
                ? resultSet.getTimestamp(COLUMN_FINISHED_AT)
                : null)
        .build();
  }
}
