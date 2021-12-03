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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_WATERMARK;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Value;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Data access object for the Connector metadata tables. */
public class PartitionMetadataDao {
  private static final Tracer TRACER = Tracing.getTracer();

  private final String metadataTableName;
  private final String metricsTableName;
  private final DatabaseClient databaseClient;
  private final PartitionMetadataMapper mapper;

  /**
   * Constructs a partition metadata dao object given the generated name of the tables.
   *
   * @param metadataTableName the name of the partition metadata table
   * @param metricsTableName the name of the partition metrics table
   * @param databaseClient the {@link DatabaseClient} to perform queries
   * @param mapper mapper from a {@link ResultSet} row to a {@link PartitionMetadata} model
   */
  PartitionMetadataDao(
      String metadataTableName,
      String metricsTableName,
      DatabaseClient databaseClient,
      PartitionMetadataMapper mapper) {
    this.metadataTableName = metadataTableName;
    this.metricsTableName = metricsTableName;
    this.databaseClient = databaseClient;
    this.mapper = mapper;
  }

  /**
   * Fetches all the partitions from the partition metadata table that are in the given state and
   * returns them ordered by the {@link PartitionMetadataAdminDao#COLUMN_START_TIMESTAMP} column in
   * ascending order.
   *
   * @param state the {@link State} to fetch all the partitions
   * @return a {@link ResultSet} with the partition rows fetched
   */
  public ResultSet getPartitionsInState(State state) {
    try (Scope scope =
        TRACER.spanBuilder("getPartitionsInState").setRecordEvents(true).startScopedSpan()) {
      final Statement statement =
          Statement.newBuilder(
                  "SELECT * FROM "
                      + metadataTableName
                      + " WHERE State = @state"
                      + " ORDER BY "
                      + PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP
                      + " ASC")
              .bind("state")
              .to(state.toString())
              .build();
      return databaseClient.singleUse().executeQuery(statement);
    }
  }

  /**
   * Fetches the earliest partition watermark from the partition metadata table that is not in a
   * {@link State#FINISHED} state.
   *
   * @return the earliest partition watermark which is not in a {@link State#FINISHED} state.
   */
  public @Nullable Timestamp getUnfinishedMinWatermark() {
    final Statement statement =
        Statement.newBuilder(
                "SELECT "
                    + COLUMN_WATERMARK
                    + " FROM "
                    + metadataTableName
                    + " WHERE "
                    + COLUMN_STATE
                    + " != @state"
                    + " ORDER BY "
                    + COLUMN_WATERMARK
                    + " ASC LIMIT 1")
            .bind("state")
            .to(State.FINISHED.name())
            .build();
    try (Scope scope =
            TRACER.spanBuilder("getMinCurrentWatermark").setRecordEvents(true).startScopedSpan();
        ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
      if (resultSet.next()) {
        return resultSet.getTimestamp(COLUMN_WATERMARK);
      }
      return null;
    }
  }

  /**
   * Inserts the partition metadata alongside initial metrics entry.
   *
   * @param row the partition metadata to be inserted
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp insert(PartitionMetadata row) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.insert(row));
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates a partition row to {@link State#SCHEDULED} state.
   *
   * @param partitionToken the partition unique identifier
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToScheduled(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.updateToScheduled(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates a partition row to {@link State#RUNNING} state.
   *
   * @param partitionToken the partition unique identifier
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToRunning(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.updateToRunning(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates a partition row to {@link State#FINISHED} state.
   *
   * @param partitionToken the partition unique identifier
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToFinished(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.updateToFinished(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates the partition metrics indicating when the change stream query started at.
   *
   * @param partitionToken the partition unique identifier
   */
  @SuppressWarnings("return.type.incompatible")
  public void updateQueryStartedAt(String partitionToken) {
    runInTransaction(
        transaction -> {
          final long recordsProcessed = transaction.getRecordsProcessed(partitionToken);
          if (recordsProcessed == 0) {
            transaction.updateQueryStartedAt(partitionToken);
          }
          return null;
        });
  }

  /**
   * Update the partition metrics indicating how many records were streamed so far for the change
   * stream query.
   *
   * @param partitionToken the partition unique identifier
   * @param recordsIncrement the number of records to update the current count to
   */
  @SuppressWarnings("return.type.incompatible")
  public void updateRecordsProcessed(String partitionToken, long recordsIncrement) {
    runInTransaction(
        transaction -> {
          final long recordsProcessed = transaction.getRecordsProcessed(partitionToken);
          transaction.updateRecordsProcessed(recordsProcessed + recordsIncrement, partitionToken);
          return null;
        });
  }

  /**
   * Update the partition watermark to the given timestamp.
   *
   * @param partitionToken the partition unique identifier
   * @param watermark the new partition watermark
   */
  public void updateWatermark(String partitionToken, Timestamp watermark) {
    runInTransaction(transaction -> transaction.updateWatermark(partitionToken, watermark));
  }

  /**
   * Runs a given function in a transaction context. The transaction object is given as the
   * parameter to the input function. If the function returns successfully, it will be committed. If
   * the function throws an exception it will be rolled back.
   *
   * @param <T> the return type to be returned from the input transactional function
   * @param callable the function to be executed within the transaction context
   * @return a transaction result containing the result from the function and a commit timestamp for
   *     the read / write transaction
   */
  public <T> TransactionResult<T> runInTransaction(Function<InTransactionContext, T> callable) {
    final TransactionRunner readWriteTransaction = databaseClient.readWriteTransaction();
    final T result =
        readWriteTransaction.run(
            transaction -> {
              final InTransactionContext transactionContext =
                  new InTransactionContext(
                      metadataTableName, metricsTableName, mapper, transaction);
              return callable.apply(transactionContext);
            });
    return new TransactionResult<>(result, readWriteTransaction.getCommitTimestamp());
  }

  /** Represents the execution of a read / write transaction in Cloud Spanner. */
  public static class InTransactionContext {

    private static final Tracer TRACER = Tracing.getTracer();
    private final String metadataTableName;
    private final String metricsTableName;
    private final TransactionContext transaction;
    private final PartitionMetadataMapper mapper;
    private final Map<State, String> stateToTimestampColumn;

    /**
     * Constructs a context to execute a user defined function transactionally.
     *
     * @param metadataTableName the name of the partition metadata table
     * @param metricsTableName the name of the partition metrics table
     * @param mapper mapper from a {@link ResultSet} row to a {@link PartitionMetadata} model
     * @param transaction the underlying client library transaction to be executed
     */
    public InTransactionContext(
        String metadataTableName,
        String metricsTableName,
        PartitionMetadataMapper mapper,
        TransactionContext transaction) {
      this.metadataTableName = metadataTableName;
      this.metricsTableName = metricsTableName;
      this.transaction = transaction;
      this.mapper = mapper;
      this.stateToTimestampColumn = new HashMap<>();
      stateToTimestampColumn.put(State.CREATED, PartitionMetadataAdminDao.COLUMN_CREATED_AT);
      stateToTimestampColumn.put(State.SCHEDULED, PartitionMetadataAdminDao.COLUMN_SCHEDULED_AT);
      stateToTimestampColumn.put(State.RUNNING, PartitionMetadataAdminDao.COLUMN_RUNNING_AT);
      stateToTimestampColumn.put(State.FINISHED, PartitionMetadataAdminDao.COLUMN_FINISHED_AT);
    }

    /**
     * Inserts the partition metadata alongside initial metrics entry.
     *
     * @param row the partition metadata to be inserted
     */
    public Void insert(PartitionMetadata row) {
      try (Scope scope = TRACER.spanBuilder("insert").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL,
                AttributeValue.stringAttributeValue(row.getPartitionToken()));
        transaction.buffer(
            ImmutableList.of(
                createInsertMetadataMutationFrom(row), createInsertMetricMutationFrom(row)));
        return null;
      }
    }

    /**
     * Updates a partition row to {@link State#SCHEDULED} state.
     *
     * @param partitionToken the partition unique identifier
     */
    public Void updateToScheduled(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("updateToScheduled").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            ImmutableList.of(
                createUpdateMetadataStateMutationFrom(partitionToken, State.SCHEDULED),
                createUpdateMetricStateMutationFrom(partitionToken, State.SCHEDULED)));
        return null;
      }
    }

    /**
     * Updates a partition row to {@link State#RUNNING} state.
     *
     * @param partitionToken the partition unique identifier
     */
    public Void updateToRunning(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("updateToRunning").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            ImmutableList.of(
                createUpdateMetadataStateMutationFrom(partitionToken, State.RUNNING),
                createUpdateMetricStateMutationFrom(partitionToken, State.RUNNING)));
        return null;
      }
    }

    /**
     * Updates a partition row to {@link State#FINISHED} state.
     *
     * @param partitionToken the partition unique identifier
     */
    public Void updateToFinished(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("updateToRunning").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            ImmutableList.of(
                createUpdateMetadataStateMutationFrom(partitionToken, State.FINISHED),
                createUpdateMetricStateMutationFrom(partitionToken, State.FINISHED)));
        return null;
      }
    }

    /**
     * Update the partition metrics indicating when the change stream query started at.
     *
     * @param partitionToken the partition unique identifier
     */
    public void updateQueryStartedAt(String partitionToken) {
      Mutation mutation =
          Mutation.newUpdateBuilder(metricsTableName)
              .set(PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN)
              .to(partitionToken)
              .set(PartitionMetricsAdminDao.COLUMN_QUERY_STARTED_AT)
              .to(Value.COMMIT_TIMESTAMP)
              .set(PartitionMetricsAdminDao.COLUMN_LAST_UPDATED_AT)
              .to(Value.COMMIT_TIMESTAMP)
              .build();
      transaction.buffer(mutation);
    }

    /**
     * Update the partition metrics indicating how many records were streamed so far for the change
     * stream query.
     *
     * @param partitionToken the partition unique identifier
     * @param recordsProcessed the number of records to update the current count to
     */
    public void updateRecordsProcessed(long recordsProcessed, String partitionToken) {
      Mutation mutation =
          Mutation.newUpdateBuilder(metricsTableName)
              .set(PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN)
              .to(partitionToken)
              .set(PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED)
              .to(recordsProcessed)
              .set(PartitionMetricsAdminDao.COLUMN_LAST_PROCESSED_AT)
              .to(Value.COMMIT_TIMESTAMP)
              .set(PartitionMetricsAdminDao.COLUMN_LAST_UPDATED_AT)
              .to(Value.COMMIT_TIMESTAMP)
              .build();
      transaction.buffer(mutation);
    }

    /**
     * Update the partition watermark to the given timestamp.
     *
     * @param partitionToken the partition unique identifier
     * @param watermark the new partition watermark
     * @return the commit timestamp of the read / write transaction
     */
    public Void updateWatermark(String partitionToken, Timestamp watermark) {
      try (Scope scope =
          TRACER.spanBuilder("updateCurrentWatermark").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(createUpdateMetadataWatermarkMutationFrom(partitionToken, watermark));
        return null;
      }
    }

    /**
     * Returns the number of records processed for the given partition so far.
     *
     * @param partitionToken the partition unique identifier
     * @return the number of records processed for the partition so far or 0 if the partition row
     *     does not exist
     */
    public long getRecordsProcessed(String partitionToken) {
      // TODO: Use readRow when java-spanner version >= 6.13.0
      try (ResultSet resultSet =
          transaction.executeQuery(
              Statement.newBuilder(
                      "SELECT "
                          + PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED
                          + " FROM "
                          + metricsTableName
                          + " WHERE "
                          + PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN
                          + " = @partitionToken")
                  .bind("partitionToken")
                  .to(partitionToken)
                  .build())) {
        if (resultSet.next()) {
          return resultSet.getLong(PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED);
        } else {
          return 0L;
        }
      }
    }

    /**
     * Fetches the partition metadata row data for the given partition token.
     *
     * @param partitionToken the partition unique identifier
     * @return the partition metadata for the given token if it exists. Otherwise, it throws a
     *     {@link PartitionNotFoundException}
     */
    public @Nullable PartitionMetadata getPartition(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("getPartition").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        try (ResultSet resultSet =
            transaction.executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM "
                            + metadataTableName
                            + " WHERE "
                            + PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN
                            + " = @partition")
                    .bind("partition")
                    .to(partitionToken)
                    .build())) {
          if (resultSet.next()) {
            return mapper.from(resultSet);
          }
          return null;
        }
      }
    }

    private Mutation createInsertMetadataMutationFrom(PartitionMetadata partitionMetadata) {
      return Mutation.newInsertBuilder(metadataTableName)
          .set(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionMetadata.getPartitionToken())
          .set(PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS)
          .toStringArray(partitionMetadata.getParentTokens())
          .set(PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP)
          .to(partitionMetadata.getStartTimestamp())
          .set(PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP)
          .to(partitionMetadata.getEndTimestamp())
          .set(PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS)
          .to(partitionMetadata.getHeartbeatMillis())
          .set(COLUMN_STATE)
          .to(partitionMetadata.getState().toString())
          .set(COLUMN_WATERMARK)
          .to(partitionMetadata.getWatermark())
          .set(PartitionMetadataAdminDao.COLUMN_CREATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createInsertMetricMutationFrom(PartitionMetadata partitionMetadata) {
      return Mutation.newInsertBuilder(metricsTableName)
          .set(PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionMetadata.getPartitionToken())
          .set(PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED)
          .to(0)
          .set(PartitionMetricsAdminDao.COLUMN_CREATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .set(PartitionMetricsAdminDao.COLUMN_LAST_UPDATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createUpdateMetadataStateMutationFrom(String partitionToken, State state) {
      final String timestampColumn = stateToTimestampColumn.get(state);
      if (timestampColumn == null) {
        throw new IllegalArgumentException("No timestamp column name found for state " + state);
      }
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(COLUMN_STATE)
          .to(state.toString())
          .set(timestampColumn)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createUpdateMetricStateMutationFrom(String partitionToken, State state) {
      WriteBuilder mutationBuilder =
          Mutation.newUpdateBuilder(metricsTableName)
              .set(PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN)
              .to(partitionToken)
              .set(PartitionMetricsAdminDao.COLUMN_LAST_UPDATED_AT)
              .to(Value.COMMIT_TIMESTAMP);
      switch (state) {
        case SCHEDULED:
          mutationBuilder =
              mutationBuilder
                  .set(PartitionMetricsAdminDao.COLUMN_SCHEDULED_AT)
                  .to(Value.COMMIT_TIMESTAMP);
          break;
        case RUNNING:
          mutationBuilder =
              mutationBuilder
                  .set(PartitionMetricsAdminDao.COLUMN_RUNNING_AT)
                  .to(Value.COMMIT_TIMESTAMP);
          break;
        case FINISHED:
          mutationBuilder =
              mutationBuilder
                  .set(PartitionMetricsAdminDao.COLUMN_FINISHED_AT)
                  .to(Value.COMMIT_TIMESTAMP);
          break;
        case CREATED:
        default:
          throw new IllegalArgumentException(
              String.format("State %s should not be set in a metadata update.", state));
      }
      return mutationBuilder.build();
    }

    private Mutation createUpdateMetadataWatermarkMutationFrom(
        String partitionToken, Timestamp watermark) {
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(COLUMN_WATERMARK)
          .to(watermark)
          .build();
    }
  }

  /**
   * Represents a result from executing a Cloud Spanner read / write transaction. It encapsulates
   * the return from the transaction function and a commit timestamp.
   *
   * @param <T> the return type of the transaction execution
   */
  public static class TransactionResult<T> {
    @Nullable private final T result;
    private final Timestamp commitTimestamp;

    public TransactionResult(@Nullable T result, Timestamp commitTimestamp) {
      this.result = result;
      this.commitTimestamp = commitTimestamp;
    }

    /** Returns the result of the transaction execution. */
    public @Nullable T getResult() {
      return result;
    }

    /** Returns the commit timestamp of the read / write transaction. */
    public Timestamp getCommitTimestamp() {
      return commitTimestamp;
    }

    @Override
    public String toString() {
      return "CommitResponse{" + "result=" + result + ", commitTimestamp=" + commitTimestamp + '}';
    }
  }
}
