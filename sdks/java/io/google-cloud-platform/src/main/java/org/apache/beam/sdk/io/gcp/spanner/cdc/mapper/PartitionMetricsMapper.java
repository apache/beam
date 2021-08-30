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
package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_CREATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_DELETED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_FINISHED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_LAST_PROCESSED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_LAST_UPDATED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_QUERY_STARTED_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_RUNNING_AT;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetricsAdminDao.COLUMN_SCHEDULED_AT;

import com.google.cloud.spanner.ResultSet;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetrics;

public class PartitionMetricsMapper {

  public PartitionMetrics from(ResultSet resultSet) {
    return PartitionMetrics.newBuilder()
        .setPartitionToken(resultSet.getString(COLUMN_PARTITION_TOKEN))
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
        .setDeletedAt(
            !resultSet.isNull(COLUMN_DELETED_AT) ? resultSet.getTimestamp(COLUMN_DELETED_AT) : null)
        .setQueryStartedAt(
            !resultSet.isNull(COLUMN_QUERY_STARTED_AT)
                ? resultSet.getTimestamp(COLUMN_QUERY_STARTED_AT)
                : null)
        .setRecordsProcessed(
            !resultSet.isNull(COLUMN_RECORDS_PROCESSED)
                ? resultSet.getLong(COLUMN_RECORDS_PROCESSED)
                : null)
        .setLastProcessedAt(
            !resultSet.isNull(COLUMN_LAST_PROCESSED_AT)
                ? resultSet.getTimestamp(COLUMN_LAST_PROCESSED_AT)
                : null)
        .setLastUpdatedAt(
            !resultSet.isNull(COLUMN_LAST_UPDATED_AT)
                ? resultSet.getTimestamp(COLUMN_LAST_UPDATED_AT)
                : null)
        .build();
  }
}
