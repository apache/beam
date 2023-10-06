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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/** This class is responsible for transforming a {@link Struct} to a {@link PartitionMetadata}. */
public class PartitionMetadataMapper {

  PartitionMetadataMapper() {}

  /**
   * Transforms a {@link Struct} representing a partition metadata row into a {@link
   * PartitionMetadata} model. The {@link Struct} is expected to have the following fields:
   *
   * <ul>
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_PARTITION_TOKEN}: non-nullable {@link String}
   *       representing the partition unique identifier.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_PARENT_TOKENS}: non-nullable {@link List} of
   *       {@link String} representing the partition parents' unique identifiers.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_START_TIMESTAMP}: non-nullable {@link Timestamp}
   *       representing the start time at which the partition started existing in Cloud Spanner.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_END_TIMESTAMP}: non-nullable {@link Timestamp}
   *       representing the end time for querying this partition.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_HEARTBEAT_MILLIS}: non-nullable {@link Long}
   *       representing the number of milliseconds after the stream is idle, which a heartbeat
   *       record will be emitted.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_STATE}: non-nullable {@link String} representing
   *       the {@link State} in which the partition is in.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_WATERMARK}: non-nullable {@link Timestamp}
   *       representing the time for which all records with a timestamp less than it have been
   *       processed.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_CREATED_AT}: non-nullable {@link Timestamp}
   *       representing the time at which this partition was first detected.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_SCHEDULED_AT}: nullable {@link Timestamp}
   *       representing the time at which this partition was scheduled to be queried.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_RUNNING_AT}: nullable {@link Timestamp}
   *       representing the time at which the connector started processing this partition.
   *   <li>{@link PartitionMetadataAdminDao#COLUMN_FINISHED_AT}: nullable {@link Timestamp}
   *       representing the time at which the connector finished processing this partition.
   * </ul>
   *
   * @param row the {@link Struct} row to be converted. It should contain all the fields as
   *     specified above.
   * @return a {@link PartitionMetadata} with the mapped {@link Struct} field values.
   */
  public PartitionMetadata from(Struct row) {
    return PartitionMetadata.newBuilder()
        .setPartitionToken(row.getString(COLUMN_PARTITION_TOKEN))
        .setParentTokens(Sets.newHashSet(row.getStringList(COLUMN_PARENT_TOKENS)))
        .setStartTimestamp(row.getTimestamp(COLUMN_START_TIMESTAMP))
        .setEndTimestamp(row.getTimestamp(COLUMN_END_TIMESTAMP))
        .setHeartbeatMillis(row.getLong(COLUMN_HEARTBEAT_MILLIS))
        .setState(State.valueOf(row.getString(COLUMN_STATE)))
        .setWatermark(row.getTimestamp(COLUMN_WATERMARK))
        .setCreatedAt(row.getTimestamp(COLUMN_CREATED_AT))
        .setScheduledAt(
            !row.isNull(COLUMN_SCHEDULED_AT) ? row.getTimestamp(COLUMN_SCHEDULED_AT) : null)
        .setRunningAt(!row.isNull(COLUMN_RUNNING_AT) ? row.getTimestamp(COLUMN_RUNNING_AT) : null)
        .setFinishedAt(
            !row.isNull(COLUMN_FINISHED_AT) ? row.getTimestamp(COLUMN_FINISHED_AT) : null)
        .build();
  }
}
