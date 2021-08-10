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

// TODO: Add java docs
public class PartitionMetadataDao {
  private static final Tracer TRACER = Tracing.getTracer();

  // Metadata table column names
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_PARENT_TOKENS = "ParentTokens";
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  public static final String COLUMN_INCLUSIVE_START = "InclusiveStart";
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  public static final String COLUMN_INCLUSIVE_END = "InclusiveEnd";
  public static final String COLUMN_HEARTBEAT_MILLIS = "HeartbeatMillis";
  public static final String COLUMN_STATE = "State";
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  public static final String COLUMN_SCHEDULED_AT = "ScheduledAt";
  public static final String COLUMN_RUNNING_AT = "RunningAt";
  public static final String COLUMN_FINISHED_AT = "FinishedAt";

  private final String tableName;
  private final DatabaseClient databaseClient;
  private final PartitionMetadataMapper mapper;

  public PartitionMetadataDao(
      String tableName, DatabaseClient databaseClient, PartitionMetadataMapper mapper) {
    this.tableName = tableName;
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
                      + tableName
                      + " WHERE @partition IN UNNEST ("
                      + COLUMN_PARENT_TOKENS
                      + ")"
                      + " AND "
                      + COLUMN_STATE
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
                      + tableName
                      + " WHERE "
                      + COLUMN_PARTITION_TOKEN
                      + " IN UNNEST (("
                      + " SELECT "
                      + COLUMN_PARENT_TOKENS
                      + " FROM "
                      + tableName
                      + " WHERE "
                      + COLUMN_PARTITION_TOKEN
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
      final Statement statement = Statement.newBuilder("SELECT COUNT(*) FROM " + tableName).build();
      try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
        resultSet.next();
        return resultSet.getLong(0);
      }
    }
  }

  public ResultSet getPartitionsInState(State state) {
    try (Scope scope =
        TRACER.spanBuilder("getPartitionsInState").setRecordEvents(true).startScopedSpan()) {
      Statement statement =
          Statement.newBuilder("SELECT * FROM " + tableName + " WHERE State = @state")
              .bind("state")
              .to(state.toString())
              .build();
      return databaseClient.singleUse().executeQuery(statement);
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
                  new InTransactionContext(tableName, mapper, transaction);
              return callable.apply(transactionContext);
            });
    return new TransactionResult<>(result, readWriteTransaction.getCommitTimestamp());
  }

  public static class InTransactionContext {

    private static final Tracer TRACER = Tracing.getTracer();
    private final String tableName;
    private final TransactionContext transaction;
    private final PartitionMetadataMapper mapper;
    private final Map<State, String> stateToTimestampColumn;

    public InTransactionContext(
        String tableName, PartitionMetadataMapper mapper, TransactionContext transaction) {
      this.tableName = tableName;
      this.transaction = transaction;
      this.mapper = mapper;
      this.stateToTimestampColumn = new HashMap<>();
      stateToTimestampColumn.put(State.CREATED, COLUMN_CREATED_AT);
      stateToTimestampColumn.put(State.SCHEDULED, COLUMN_SCHEDULED_AT);
      stateToTimestampColumn.put(State.RUNNING, COLUMN_RUNNING_AT);
      stateToTimestampColumn.put(State.FINISHED, COLUMN_FINISHED_AT);
    }

    public Void insert(PartitionMetadata row) {
      try (Scope scope = TRACER.spanBuilder("insert").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL,
                AttributeValue.stringAttributeValue(row.getPartitionToken()));
        transaction.buffer(createInsertMutationFrom(row));
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
        transaction.buffer(createUpdateMutationFrom(partitionToken, State.SCHEDULED));
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
        transaction.buffer(createUpdateMutationFrom(partitionToken, State.RUNNING));
        return null;
      }
    }

    public Void updateToFinished(String partitionToken) {
      try (Scope scope =
          TRACER.spanBuilder("updateToFinished").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(createUpdateMutationFrom(partitionToken, State.FINISHED));
        return null;
      }
    }

    public Void delete(String partitionToken) {
      try (Scope scope = TRACER.spanBuilder("delete").setRecordEvents(true).startScopedSpan()) {
        TRACER
            .getCurrentSpan()
            .putAttribute(
                PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(partitionToken));
        transaction.buffer(createDeleteMutationFrom(partitionToken));
        return null;
      }
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
                            + tableName
                            + " WHERE "
                            + COLUMN_PARTITION_TOKEN
                            + " IN UNNEST (@partitions)"
                            + " AND "
                            + COLUMN_STATE
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
                            + tableName
                            + " WHERE "
                            + COLUMN_PARTITION_TOKEN
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

    private Mutation createInsertMutationFrom(PartitionMetadata partitionMetadata) {
      return Mutation.newInsertBuilder(tableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(partitionMetadata.getPartitionToken())
          .set(COLUMN_PARENT_TOKENS)
          .toStringArray(partitionMetadata.getParentTokens())
          .set(COLUMN_START_TIMESTAMP)
          .to(partitionMetadata.getStartTimestamp())
          .set(COLUMN_INCLUSIVE_START)
          .to(partitionMetadata.isInclusiveStart())
          .set(COLUMN_END_TIMESTAMP)
          .to(partitionMetadata.getEndTimestamp())
          .set(COLUMN_INCLUSIVE_END)
          .to(partitionMetadata.isInclusiveEnd())
          .set(COLUMN_HEARTBEAT_MILLIS)
          .to(partitionMetadata.getHeartbeatMillis())
          .set(COLUMN_STATE)
          .to(partitionMetadata.getState().toString())
          .set(COLUMN_CREATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createUpdateMutationFrom(String partitionToken, State state) {
      final String timestampColumn = stateToTimestampColumn.get(state);
      return Mutation.newUpdateBuilder(tableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(COLUMN_STATE)
          .to(state.toString())
          .set(timestampColumn)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createDeleteMutationFrom(String partitionToken) {
      return Mutation.delete(tableName, Key.of(partitionToken));
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
