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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Data access object for creating and dropping the partition metadata table.
 *
 * <p>The partition metadata table will be used to keep the state of a partition as the Connector is
 * performing change stream queries.
 */
public class PartitionMetadataAdminDao {

  /** Metadata table column name for the partition token. */
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  /** Metadata table column name for parent partition tokens. */
  public static final String COLUMN_PARENT_TOKENS = "ParentTokens";
  /**
   * Metadata table column name for the timestamp to start the change stream query of the partition.
   */
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  /**
   * Metadata table column name for the timestamp to end the change stream query of the partition.
   */
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  /** Metadata table column name for the change stream query heartbeat interval in millis. */
  public static final String COLUMN_HEARTBEAT_MILLIS = "HeartbeatMillis";
  /**
   * Metadata table column name for the state that the partition is currently in. Possible states
   * can be seen in {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State}.
   */
  public static final String COLUMN_STATE = "State";
  /** Metadata table column name for the current watermark of the partition. */
  public static final String COLUMN_WATERMARK = "Watermark";
  /** Metadata table column name for the timestamp at which the partition row was first created. */
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  /**
   * Metadata table column name for the timestamp at which the partition was scheduled by the {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.DetectNewPartitionsDoFn} SDF.
   */
  public static final String COLUMN_SCHEDULED_AT = "ScheduledAt";
  /**
   * Metadata table column name for the timestamp at which the partition was marked as running by
   * the {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * SDF.
   */
  public static final String COLUMN_RUNNING_AT = "RunningAt";
  /**
   * Metadata table column name for the timestamp at which the partition was marked as finished by
   * the {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * SDF.
   */
  public static final String COLUMN_FINISHED_AT = "FinishedAt";

  private static final int TIMEOUT_MINUTES = 10;
  private static final int TTL_AFTER_PARTITION_FINISHED_DAYS = 1;

  private final DatabaseAdminClient databaseAdminClient;
  private final String instanceId;
  private final String databaseId;
  private final PartitionMetadataTableNames names;
  private final Dialect dialect;

  /**
   * Constructs the partition metadata admin dao.
   *
   * @param databaseAdminClient the {@link DatabaseAdminClient} to be used to manage the metadata
   *     table
   * @param instanceId the instance where the metadata table will reside
   * @param databaseId the database where the metadata table will reside
   * @param names the names of the metadata table ddl objects
   */
  PartitionMetadataAdminDao(
      DatabaseAdminClient databaseAdminClient,
      String instanceId,
      String databaseId,
      PartitionMetadataTableNames names,
      Dialect dialect) {
    this.databaseAdminClient = databaseAdminClient;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.names = names;
    this.dialect = dialect;
  }

  /**
   * Creates the metadata table in the given instance, database configuration, with the constructor
   * specified table name. The operation is intended to complete in {@link
   * PartitionMetadataAdminDao#TIMEOUT_MINUTES} minutes and specifies a TTL of partition rows after
   * they are marked as FINISHED as {@link
   * PartitionMetadataAdminDao#TTL_AFTER_PARTITION_FINISHED_DAYS} days.
   */
  public void createPartitionMetadataTable() {
    List<String> ddl = new ArrayList<>();
    if (this.isPostgres()) {
      // Literals need be added around literals to preserve casing.
      ddl.add(
          "CREATE TABLE \""
              + names.getTableName()
              + "\"(\""
              + COLUMN_PARTITION_TOKEN
              + "\" text NOT NULL,\""
              + COLUMN_PARENT_TOKENS
              + "\" text[] NOT NULL,\""
              + COLUMN_START_TIMESTAMP
              + "\" timestamptz NOT NULL,\""
              + COLUMN_END_TIMESTAMP
              + "\" timestamptz NOT NULL,\""
              + COLUMN_HEARTBEAT_MILLIS
              + "\" BIGINT NOT NULL,\""
              + COLUMN_STATE
              + "\" text NOT NULL,\""
              + COLUMN_WATERMARK
              + "\" timestamptz NOT NULL,\""
              + COLUMN_CREATED_AT
              + "\" SPANNER.COMMIT_TIMESTAMP NOT NULL,\""
              + COLUMN_SCHEDULED_AT
              + "\" SPANNER.COMMIT_TIMESTAMP,\""
              + COLUMN_RUNNING_AT
              + "\" SPANNER.COMMIT_TIMESTAMP,\""
              + COLUMN_FINISHED_AT
              + "\" SPANNER.COMMIT_TIMESTAMP,"
              + " PRIMARY KEY (\""
              + COLUMN_PARTITION_TOKEN
              + "\")"
              + ")"
              + " TTL INTERVAL '"
              + TTL_AFTER_PARTITION_FINISHED_DAYS
              + " days' ON \""
              + COLUMN_FINISHED_AT
              + "\"");
      ddl.add(
          "CREATE INDEX \""
              + names.getWatermarkIndexName()
              + "\" on \""
              + names.getTableName()
              + "\" (\""
              + COLUMN_WATERMARK
              + "\") INCLUDE (\""
              + COLUMN_STATE
              + "\")");
      ddl.add(
          "CREATE INDEX \""
              + names.getCreatedAtIndexName()
              + "\" ON \""
              + names.getTableName()
              + "\" (\""
              + COLUMN_CREATED_AT
              + "\",\""
              + COLUMN_START_TIMESTAMP
              + "\")");
    } else {
      ddl.add(
          "CREATE TABLE "
              + names.getTableName()
              + " ("
              + COLUMN_PARTITION_TOKEN
              + " STRING(MAX) NOT NULL,"
              + COLUMN_PARENT_TOKENS
              + " ARRAY<STRING(MAX)> NOT NULL,"
              + COLUMN_START_TIMESTAMP
              + " TIMESTAMP NOT NULL,"
              + COLUMN_END_TIMESTAMP
              + " TIMESTAMP NOT NULL,"
              + COLUMN_HEARTBEAT_MILLIS
              + " INT64 NOT NULL,"
              + COLUMN_STATE
              + " STRING(MAX) NOT NULL,"
              + COLUMN_WATERMARK
              + " TIMESTAMP NOT NULL,"
              + COLUMN_CREATED_AT
              + " TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),"
              + COLUMN_SCHEDULED_AT
              + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
              + COLUMN_RUNNING_AT
              + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
              + COLUMN_FINISHED_AT
              + " TIMESTAMP OPTIONS (allow_commit_timestamp=true),"
              + ") PRIMARY KEY ("
              + COLUMN_PARTITION_TOKEN
              + "),"
              + " ROW DELETION POLICY (OLDER_THAN("
              + COLUMN_FINISHED_AT
              + ", INTERVAL "
              + TTL_AFTER_PARTITION_FINISHED_DAYS
              + " DAY))");
      ddl.add(
          "CREATE INDEX "
              + names.getWatermarkIndexName()
              + " on "
              + names.getTableName()
              + " ("
              + COLUMN_WATERMARK
              + ") STORING ("
              + COLUMN_STATE
              + ")");
      ddl.add(
          "CREATE INDEX "
              + names.getCreatedAtIndexName()
              + " ON "
              + names.getTableName()
              + " ("
              + COLUMN_CREATED_AT
              + ","
              + COLUMN_START_TIMESTAMP
              + ")");
    }
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(instanceId, databaseId, ddl, null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (ExecutionException | TimeoutException e) {
      // If the operation failed or timed out during execution, expose the cause.
      if (e.getCause() != null) {
        throw (SpannerException) e.getCause();
      } else {
        throw SpannerExceptionFactory.asSpannerException(e);
      }
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  /**
   * Drops the metadata table. This operation should complete in {@link
   * PartitionMetadataAdminDao#TIMEOUT_MINUTES} minutes.
   */
  public void deletePartitionMetadataTable(List<String> indexes) {
    List<String> ddl = new ArrayList<>();
    if (this.isPostgres()) {
      indexes.forEach(index -> ddl.add("DROP INDEX \"" + index + "\""));
      ddl.add("DROP TABLE \"" + names.getTableName() + "\"");
    } else {
      indexes.forEach(index -> ddl.add("DROP INDEX " + index));
      ddl.add("DROP TABLE " + names.getTableName());
    }
    OperationFuture<Void, UpdateDatabaseDdlMetadata> op =
        databaseAdminClient.updateDatabaseDdl(instanceId, databaseId, ddl, null);
    try {
      // Initiate the request which returns an OperationFuture.
      op.get(TIMEOUT_MINUTES, TimeUnit.MINUTES);
    } catch (ExecutionException | TimeoutException e) {
      // If the operation failed or timed out during execution, expose the cause.
      if (e.getCause() != null) {
        throw (SpannerException) e.getCause();
      } else {
        throw SpannerExceptionFactory.asSpannerException(e);
      }
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  private boolean isPostgres() {
    return this.dialect == Dialect.POSTGRESQL;
  }
}
