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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

// TODO: Add java docs
public class PartitionMetadataDao {
  private static final Tracer TRACER = Tracing.getTracer();

  private final String metadataTableName;
  private final String metricsTableName;
  private final DatabaseClient databaseClient;
  private final PartitionMetadataMapper mapper;

  public PartitionMetadataDao(
      String metadataTableName,
      String metricsTableName,
      DatabaseClient databaseClient,
      PartitionMetadataMapper mapper) {
    this.metadataTableName = metadataTableName;
    this.metricsTableName = metricsTableName;
    this.databaseClient = databaseClient;
    this.mapper = mapper;
  }

  public long countChildPartitionsInStates(
      String partitionToken, List<PartitionMetadata.State> states) {
    try (Scope scope =
        TRACER
            .spanBuilder("countChildPartitionsInStates")
            .setRecordEvents(true)
            .startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
      final Statement statement =
          Statement.newBuilder(
                  "SELECT COUNT(*)"
                      + " FROM "
                      + metadataTableName
                      + " WHERE @partition IN UNNEST ("
                      + PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS
                      + ")"
                      + " AND "
                      + PartitionMetadataAdminDao.COLUMN_STATE
                      + " IN UNNEST (@states)")
              .bind("partition")
              .to(partitionToken)
              .bind("states")
              .toStringArray(states.stream().map(State::toString).collect(Collectors.toList()))
              .build();
      try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
        resultSet.next();
        return resultSet.getLong(0);
      }
    }
  }

  public long countExistingParents(String partitionToken) {
    try (Scope scope =
        TRACER.spanBuilder("countExistingParents").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
      final Statement statement =
          Statement.newBuilder(
                  "SELECT COUNT(*)"
                      + " FROM "
                      + metadataTableName
                      + " WHERE "
                      + PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN
                      + " IN UNNEST (("
                      + " SELECT "
                      + PartitionMetadataAdminDao.COLUMN_PARENT_TOKENS
                      + " FROM "
                      + metadataTableName
                      + " WHERE "
                      + PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN
                      + " = "
                      + "@partition"
                      + "))")
              .bind("partition")
              .to(partitionToken)
              .build();
      try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
        resultSet.next();
        return resultSet.getLong(0);
      }
    }
  }

  public long countPartitions() {
    try (Scope scope =
        TRACER.spanBuilder("countPartitions").setRecordEvents(true).startScopedSpan()) {
      final Statement statement =
          Statement.newBuilder("SELECT COUNT(*) FROM " + metadataTableName).build();
      try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
        resultSet.next();
        return resultSet.getLong(0);
      }
    }
  }

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

  public Timestamp getMinCurrentWatermark() {
    final Statement statement =
        Statement.of(
            "SELECT "
                + PartitionMetadataAdminDao.COLUMN_CURRENT_WATERMARK
                + " FROM "
                + metadataTableName
                + " ORDER BY "
                + PartitionMetadataAdminDao.COLUMN_CURRENT_WATERMARK
                + " ASC LIMIT 1");
    try (Scope scope =
            TRACER.spanBuilder("getMinCurrentWatermark").setRecordEvents(true).startScopedSpan();
        ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
      if (resultSet.next()) {
        return resultSet.getTimestamp(PartitionMetadataAdminDao.COLUMN_CURRENT_WATERMARK);
      }
      return null;
    }
  }

  public Timestamp insert(PartitionMetadata row) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.insert(row));
    return transactionResult.getCommitTimestamp();
  }

  public Timestamp updateToScheduled(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.updateToScheduled(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

  public Timestamp updateToRunning(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.updateToRunning(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

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

  public void updateRecordsProcessed(String partitionToken, long recordsIncrement) {
    runInTransaction(
        transaction -> {
          final long recordsProcessed = transaction.getRecordsProcessed(partitionToken);
          transaction.updateRecordsProcessed(recordsProcessed + recordsIncrement, partitionToken);
          return null;
        });
  }

  public Timestamp updateCurrentWatermark(String partitionToken, Timestamp watermark) {
    final TransactionResult<Object> transactionResult =
        runInTransaction(
            transaction -> transaction.updateCurrentWatermark(partitionToken, watermark));
    return transactionResult.getCommitTimestamp();
  }

  public Timestamp delete(String partitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.delete(partitionToken));
    return transactionResult.getCommitTimestamp();
  }

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

  public static class InTransactionContext {

    private static final Tracer TRACER = Tracing.getTracer();
    private final String metadataTableName;
    private final String metricsTableName;
    private final TransactionContext transaction;
    private final PartitionMetadataMapper mapper;
    private final Map<State, String> stateToTimestampColumn;

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

    public Void updateToFinished(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("updateToFinished").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            ImmutableList.of(
                createUpdateMetadataStateMutationFrom(partitionToken, State.FINISHED),
                createUpdateMetricStateMutationFrom(partitionToken, State.RUNNING)));
        return null;
      }
    }

    public Void updateCurrentWatermark(String partitionToken, Timestamp watermark) {
      try (Scope scope =
          TRACER.spanBuilder("updateCurrentWatermark").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            createUpdateMetadataCurrentWatermarkMutationFrom(partitionToken, watermark));
        return null;
      }
    }

    public Void delete(String partitionToken) {
      try (Scope scope = TRACER.spanBuilder("delete").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(
            ImmutableList.of(
                createDeleteMetadataMutationFrom(partitionToken),
                createDeleteMetricMutationFrom(partitionToken)));
        return null;
      }
    }

    public long getRecordsProcessed(String partitionToken) {
      return transaction
          .readRow(
              metricsTableName,
              Key.of(partitionToken),
              ImmutableList.of(PartitionMetricsAdminDao.COLUMN_RECORDS_PROCESSED))
          .getLong(0);
    }

    public long countPartitionsInStates(
        Set<String> partitionTokens, List<PartitionMetadata.State> states) {
      try (Scope scope =
          TRACER.spanBuilder("countPartitionsInStates").setRecordEvents(true).startScopedSpan()) {
        try (ResultSet resultSet =
            transaction.executeQuery(
                Statement.newBuilder(
                        "SELECT COUNT(*)"
                            + " FROM "
                            + metadataTableName
                            + " WHERE "
                            + PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN
                            + " IN UNNEST (@partitions)"
                            + " AND "
                            + PartitionMetadataAdminDao.COLUMN_STATE
                            + " IN UNNEST (@states)")
                    .bind("partitions")
                    .toStringArray(partitionTokens)
                    .bind("states")
                    .toStringArray(
                        states.stream().map(State::toString).collect(Collectors.toList()))
                    .build())) {
          resultSet.next();
          return resultSet.getLong(0);
        }
      }
    }

    public PartitionMetadata getPartition(String partitionToken) {
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
          .set(PartitionMetadataAdminDao.COLUMN_INCLUSIVE_START)
          .to(partitionMetadata.isInclusiveStart())
          .set(PartitionMetadataAdminDao.COLUMN_END_TIMESTAMP)
          .to(partitionMetadata.getEndTimestamp())
          .set(PartitionMetadataAdminDao.COLUMN_INCLUSIVE_END)
          .to(partitionMetadata.isInclusiveEnd())
          .set(PartitionMetadataAdminDao.COLUMN_HEARTBEAT_MILLIS)
          .to(partitionMetadata.getHeartbeatMillis())
          .set(PartitionMetadataAdminDao.COLUMN_STATE)
          .to(partitionMetadata.getState().toString())
          .set(PartitionMetadataAdminDao.COLUMN_CURRENT_WATERMARK)
          .to(partitionMetadata.getStartTimestamp())
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
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(PartitionMetadataAdminDao.COLUMN_STATE)
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

    private Mutation createUpdateMetadataCurrentWatermarkMutationFrom(
        String partitionToken, Timestamp watermark) {
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(PartitionMetadataAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(PartitionMetadataAdminDao.COLUMN_CURRENT_WATERMARK)
          .to(watermark)
          .build();
    }

    private Mutation createDeleteMetadataMutationFrom(String partitionToken) {
      return Mutation.delete(metadataTableName, Key.of(partitionToken));
    }

    private Mutation createDeleteMetricMutationFrom(String partitionToken) {
      return Mutation.newUpdateBuilder(metricsTableName)
          .set(PartitionMetricsAdminDao.COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(PartitionMetricsAdminDao.COLUMN_DELETED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }
  }

  public static class TransactionResult<T> {
    private final T result;
    private final Timestamp commitTimestamp;

    public TransactionResult(T result, Timestamp commitTimestamp) {
      this.result = result;
      this.commitTimestamp = commitTimestamp;
    }

    public T getResult() {
      return result;
    }

    public Timestamp getCommitTimestamp() {
      return commitTimestamp;
    }

    @Override
    public String toString() {
      return "CommitResponse{" + "result=" + result + ", commitTimestamp=" + commitTimestamp + '}';
    }
  }
}
