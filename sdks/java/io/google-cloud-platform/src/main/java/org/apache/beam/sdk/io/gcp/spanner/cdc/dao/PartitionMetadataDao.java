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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;

// TODO: Add java docs
public class PartitionMetadataDao {

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
  public static final String COLUMN_UPDATED_AT = "UpdatedAt";

  private final String tableName;
  private final DatabaseClient databaseClient;

  public PartitionMetadataDao(String tableName, DatabaseClient databaseClient) {
    this.tableName = tableName;
    this.databaseClient = databaseClient;
  }

  public long countChildPartitionsInStates(
      String partitionToken, List<PartitionMetadata.State> states) {
    Statement statement =
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

  public long countExistingParents(String partitionToken) {
    Statement statement =
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

  public long countPartitions() {
    Statement statement = Statement.newBuilder("SELECT COUNT(*) FROM " + tableName).build();
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(statement)) {
      resultSet.next();
      return resultSet.getLong(0);
    }
  }

  public ResultSet getPartitionsInState(State state) {
    Statement statement =
        Statement.newBuilder("SELECT * FROM " + tableName + " WHERE State = @state")
            .bind("state")
            .to(state.toString())
            .build();
    return databaseClient.singleUse().executeQuery(statement);
  }

  public void insert(PartitionMetadata row) {
    runInTransaction(transaction -> transaction.insert(row));
  }

  public void updateState(String partitionToken, PartitionMetadata.State state) {
    runInTransaction(transaction -> transaction.updateState(partitionToken, state));
  }

  public void delete(String partitionToken) {
    runInTransaction(transaction -> transaction.delete(partitionToken));
  }

  public <T> T runInTransaction(Function<InTransactionContext, T> callable) {
    return databaseClient
        .readWriteTransaction()
        .run(
            transaction -> {
              final InTransactionContext transactionContext =
                  new InTransactionContext(tableName, transaction);
              return callable.apply(transactionContext);
            });
  }

  public static class InTransactionContext {

    private final String tableName;
    private final TransactionContext transaction;

    public InTransactionContext(String tableName, TransactionContext transaction) {
      this.tableName = tableName;
      this.transaction = transaction;
    }

    public Void insert(PartitionMetadata row) {
      transaction.buffer(createInsertMutationFrom(row));
      return null;
    }

    public Void updateState(String partitionToken, PartitionMetadata.State state) {
      transaction.buffer(createUpdateMutationFrom(partitionToken, state));
      return null;
    }

    public Void delete(String partitionToken) {
      transaction.buffer(createDeleteMutationFrom(partitionToken));
      return null;
    }

    public long countPartitionsInStates(
        Set<String> partitionTokens, List<PartitionMetadata.State> states) {
      try (final ResultSet resultSet =
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
                  .toStringArray(states.stream().map(State::toString).collect(Collectors.toList()))
                  .build())) {
        resultSet.next();
        return resultSet.getLong(0);
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
          .set(COLUMN_UPDATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createUpdateMutationFrom(String partitionToken, State state) {
      return Mutation.newUpdateBuilder(tableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(partitionToken)
          .set(COLUMN_STATE)
          .to(state.toString())
          .build();
    }

    private Mutation createDeleteMutationFrom(String partitionToken) {
      return Mutation.delete(tableName, Key.of(partitionToken));
    }
  }
}
