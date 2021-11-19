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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dao;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// TODO: Add java docs
public class PartitionMetricsAdminDao {

  // Metrics table column names
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  public static final String COLUMN_SCHEDULED_AT = "ScheduledAt";
  public static final String COLUMN_RUNNING_AT = "RunningAt";
  public static final String COLUMN_FINISHED_AT = "FinishedAt";
  public static final String COLUMN_QUERY_STARTED_AT = "QueryStartedAt";
  public static final String COLUMN_RECORDS_PROCESSED = "RecordsProcessed";
  public static final String COLUMN_LAST_PROCESSED_AT = "LastProcessedAt";
  public static final String COLUMN_LAST_UPDATED_AT = "LastUpdatedAt";

  private static final int TIMEOUT_MINUTES = 10;
  private static final int TTL_AFTER_PARTITION_FINISHED_DAYS = 1;

  private final DatabaseAdminClient databaseAdminClient;
  private final String instanceId;
  private final String databaseId;
  private final String tableName;

  PartitionMetricsAdminDao(
      DatabaseAdminClient databaseAdminClient,
      String instanceId,
      String databaseId,
      String tableName) {
    this.databaseAdminClient = databaseAdminClient;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.tableName = tableName;
  }

  public void createPartitionMetricsTable() {
    final String metricsCreateStmt =
        "CREATE TABLE "
            + tableName
            + " ("
            + COLUMN_PARTITION_TOKEN
            + " STRING(MAX) NOT NULL,"
            + COLUMN_CREATED_AT
            + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_SCHEDULED_AT
            + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_RUNNING_AT
            + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_FINISHED_AT
            + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_QUERY_STARTED_AT
            + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_RECORDS_PROCESSED
            + " INT64 NOT NULL,"
            + COLUMN_LAST_PROCESSED_AT
            + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
            + COLUMN_LAST_UPDATED_AT
            + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
            + ") PRIMARY KEY (PartitionToken)"
            + ", ROW DELETION POLICY (OLDER_THAN("
            + COLUMN_FINISHED_AT
            + ", INTERVAL "
            + TTL_AFTER_PARTITION_FINISHED_DAYS
            + " DAY))";
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(metricsCreateStmt), null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (ExecutionException | TimeoutException e) {
      // If the operation failed or timed out during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  public void deletePartitionMetricsTable() {
    final String metricsDropStmt = "DROP TABLE " + tableName;
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(
            instanceId, databaseId, Collections.singletonList(metricsDropStmt), null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (ExecutionException | TimeoutException e) {
      // If the operation failed or timed out during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }
}
