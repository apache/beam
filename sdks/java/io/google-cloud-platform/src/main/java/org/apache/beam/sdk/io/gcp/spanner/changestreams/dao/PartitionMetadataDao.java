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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_TVF_NAME;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.PARTITION_TOKEN_TVF_NAME_DELIMITER;
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
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Data access object for the Connector metadata tables. */
public class PartitionMetadataDao {

  private final String metadataTableName;
  private final DatabaseClient databaseClient;
  private final Dialect dialect;

  /**
   * Constructs a partition metadata dao object given the generated name of the tables.
   *
   * @param metadataTableName the name of the partition metadata table
   * @param databaseClient the {@link DatabaseClient} to perform queries
   */
  PartitionMetadataDao(String metadataTableName, DatabaseClient databaseClient, Dialect dialect) {
    this.metadataTableName = metadataTableName;
    this.databaseClient = databaseClient;
    this.dialect = dialect;
  }

  /**
   * Checks whether the metadata table already exists in the database.
   *
   * @return true if the table exists, false if the table does not exist.
   */
  public boolean tableExists() {
    final String checkTableExistsStmt =
        "SELECT t.table_name FROM information_schema.tables AS t "
            + "WHERE t.table_catalog = '' AND "
            + "t.table_schema = '' AND "
            + "t.table_name = '"
            + metadataTableName
            + "'";
    try (ResultSet queryResultSet =
        databaseClient
            .singleUseReadOnlyTransaction()
            .executeQuery(
                Statement.of(checkTableExistsStmt), Options.tag("query=checkTableExists"))) {
      return queryResultSet.next();
    }
  }

  /**
   * Finds all indexes for the metadata table.
   *
   * @return a list of index names for the metadata table.
   */
  public List<String> findAllTableIndexes() {
    String indexesStmt;
    if (this.isPostgres()) {
      indexesStmt =
          "SELECT index_name FROM information_schema.indexes"
              + " WHERE table_schema = 'public'"
              + " AND table_name = '"
              + metadataTableName
              + "' AND index_type != 'PRIMARY_KEY'";
    } else {
      indexesStmt =
          "SELECT index_name FROM information_schema.indexes"
              + " WHERE table_schema = ''"
              + " AND table_name = '"
              + metadataTableName
              + "' AND index_type != 'PRIMARY_KEY'";
    }

    List<String> result = new ArrayList<>();
    try (ResultSet queryResultSet =
        databaseClient
            .singleUseReadOnlyTransaction()
            .executeQuery(Statement.of(indexesStmt), Options.tag("query=findAllTableIndexes"))) {
      while (queryResultSet.next()) {
        result.add(queryResultSet.getString("index_name"));
      }
    }
    return result;
  }

  public static String composePartitionTokenWithTvfName(String partitionToken, String tvfName) {
    if (tvfName == null || tvfName.isEmpty()) {
      return partitionToken;
    }
    return partitionToken + PARTITION_TOKEN_TVF_NAME_DELIMITER + tvfName;
  }

  public static String extractPartitionToken(String composedPartitionToken) {
    int index = composedPartitionToken.indexOf(PARTITION_TOKEN_TVF_NAME_DELIMITER);
    if (index == -1) {
      return composedPartitionToken;
    }
    return composedPartitionToken.substring(0, index);
  }

  public static String extractTvfName(String composedPartitionToken) {
    int index = composedPartitionToken.indexOf(PARTITION_TOKEN_TVF_NAME_DELIMITER);
    if (index == -1) {
      return DEFAULT_TVF_NAME;
    }
    return composedPartitionToken.substring(index + PARTITION_TOKEN_TVF_NAME_DELIMITER.length());
  }

  /**
   * Fetches the partition metadata row data for the given composedPartitionToken.
   *
   * @param composedPartitionToken the partition unique identifier
   * @return the partition metadata for the given composedPartitionToken if it exists as a struct.
   *     Otherwise, it returns null.
   */
  public @Nullable Struct getPartition(String composedPartitionToken) {
    Statement statement;
    if (this.isPostgres()) {
      statement =
          Statement.newBuilder(
                  "SELECT * FROM \""
                      + metadataTableName
                      + "\" WHERE \""
                      + COLUMN_PARTITION_TOKEN
                      + "\" = $1")
              .bind("p1")
              .to(composedPartitionToken)
              .build();
    } else {
      statement =
          Statement.newBuilder(
                  "SELECT * FROM "
                      + metadataTableName
                      + " WHERE "
                      + COLUMN_PARTITION_TOKEN
                      + " = @partition")
              .bind("partition")
              .to(composedPartitionToken)
              .build();
    }
    try (ResultSet resultSet =
        databaseClient.singleUse().executeQuery(statement, Options.tag("query=getPartition"))) {
      if (resultSet.next()) {
        return resultSet.getCurrentRowAsStruct();
      }
      return null;
    }
  }

  /**
   * Fetches the earliest partition watermark from the partition metadata table that is not in a
   * {@link State#FINISHED} state.
   *
   * @return the earliest partition watermark which is not in a {@link State#FINISHED} state.
   */
  public @Nullable Timestamp getUnfinishedMinWatermarkFrom(Timestamp sinceTimestamp) {
    Statement statement;
    final String minWatermark = "min_watermark";
    if (this.isPostgres()) {
      statement =
          Statement.newBuilder(
                  "SELECT MIN(\""
                      + COLUMN_WATERMARK
                      + "\") as "
                      + minWatermark
                      + " FROM \""
                      + metadataTableName
                      + "\" WHERE \""
                      + COLUMN_STATE
                      + "\" != $1"
                      + " AND \""
                      + COLUMN_WATERMARK
                      + "\" >= $2")
              .bind("p1")
              .to(State.FINISHED.name())
              .bind("p2")
              .to(sinceTimestamp)
              .build();
    } else {
      statement =
          Statement.newBuilder(
                  "SELECT MIN("
                      + COLUMN_WATERMARK
                      + ") as "
                      + minWatermark
                      + " FROM "
                      + metadataTableName
                      + " WHERE "
                      + COLUMN_STATE
                      + " != @state"
                      + " AND "
                      + COLUMN_WATERMARK
                      + " >= @since;")
              .bind("state")
              .to(State.FINISHED.name())
              .bind("since")
              .to(sinceTimestamp)
              .build();
    }
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(statement, Options.tag("query=getUnfinishedMinWatermarkFrom"))) {
      if (resultSet.next() && !resultSet.isNull(minWatermark)) {
        return resultSet.getTimestamp(minWatermark);
      }
      return null;
    }
  }

  /**
   * Fetches all partitions with a {@link PartitionMetadataAdminDao#COLUMN_CREATED_AT} less than the
   * given timestamp. The results are ordered by the {@link
   * PartitionMetadataAdminDao#COLUMN_CREATED_AT} and {@link
   * PartitionMetadataAdminDao#COLUMN_START_TIMESTAMP} columns in ascending order.
   */
  public ResultSet getAllPartitionsCreatedAfter(Timestamp timestamp) {
    Statement statement;
    if (this.isPostgres()) {
      statement =
          Statement.newBuilder(
                  "SELECT * FROM \""
                      + metadataTableName
                      + "\" WHERE \""
                      + COLUMN_CREATED_AT
                      + "\" > $1"
                      + " ORDER BY \""
                      + COLUMN_CREATED_AT
                      + "\" ASC"
                      + ", \""
                      + COLUMN_START_TIMESTAMP
                      + "\" ASC")
              .bind("p1")
              .to(timestamp)
              .build();
    } else {
      statement =
          Statement.newBuilder(
                  "SELECT * FROM "
                      + metadataTableName
                      + " WHERE "
                      + COLUMN_CREATED_AT
                      + " > @timestamp"
                      + " ORDER BY "
                      + COLUMN_CREATED_AT
                      + " ASC"
                      + ", "
                      + COLUMN_START_TIMESTAMP
                      + " ASC")
              .bind("timestamp")
              .to(timestamp)
              .build();
    }
    return databaseClient
        .singleUse()
        .executeQuery(statement, Options.tag("query=getAllPartitionsCreatedAfter"));
  }

  /**
   * Counts all partitions with a {@link PartitionMetadataAdminDao#COLUMN_CREATED_AT} less than the
   * given timestamp.
   */
  public long countPartitionsCreatedAfter(Timestamp timestamp) {
    Statement statement;
    if (this.isPostgres()) {
      statement =
          Statement.newBuilder(
                  "SELECT COUNT(*) as count FROM \""
                      + metadataTableName
                      + "\" WHERE \""
                      + COLUMN_CREATED_AT
                      + "\" > $1")
              .bind("p1")
              .to(timestamp)
              .build();
    } else {
      statement =
          Statement.newBuilder(
                  "SELECT COUNT(*) as count FROM "
                      + metadataTableName
                      + " WHERE "
                      + COLUMN_CREATED_AT
                      + " > @timestamp")
              .bind("timestamp")
              .to(timestamp)
              .build();
    }

    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(statement, Options.tag("query=countPartitionsCreatedAfter"))) {
      if (resultSet.next()) {
        return resultSet.getLong("count");
      } else {
        return 0;
      }
    }
  }

  private boolean isPostgres() {
    return this.dialect == Dialect.POSTGRESQL;
  }

  /**
   * Inserts the partition metadata.
   *
   * @param row the partition metadata to be inserted
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp insert(PartitionMetadata row) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.insert(row), "InsertsPartitionMetadata");
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Inserts the partition metadata in batch.
   *
   * @param rows the partition metadata objects to be inserted
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp insert(List<PartitionMetadata> rows) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(transaction -> transaction.insert(rows), "InsertsPartitionMetadataBatch");
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates multiple partition row to {@link State#SCHEDULED} state.
   *
   * @param composedPartitionTokens the partitions' unique identifiers
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToScheduled(List<String> composedPartitionTokens) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(
            transaction -> transaction.updateToScheduled(composedPartitionTokens),
            "updateToScheduled");
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates a partition row to {@link State#RUNNING} state.
   *
   * @param composedPartitionToken the partition unique identifier
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToRunning(String composedPartitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(
            transaction -> transaction.updateToRunning(composedPartitionToken), "updateToRunning");
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Updates a partition row to {@link State#FINISHED} state.
   *
   * @param composedPartitionToken the partition unique identifier
   * @return the commit timestamp of the read / write transaction
   */
  public Timestamp updateToFinished(String composedPartitionToken) {
    final TransactionResult<Void> transactionResult =
        runInTransaction(
            transaction -> transaction.updateToFinished(composedPartitionToken),
            "updateToFinished");
    return transactionResult.getCommitTimestamp();
  }

  /**
   * Update the partition watermark to the given timestamp.
   *
   * @param composedPartitionToken the partition unique identifier
   * @param watermark the new partition watermark
   */
  public void updateWatermark(String composedPartitionToken, Timestamp watermark) {
    runInTransaction(
        transaction -> transaction.updateWatermark(composedPartitionToken, watermark),
        "updateWatermark");
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
                  new InTransactionContext(metadataTableName, transaction, this.dialect);
              return callable.apply(transactionContext);
            });
    return new TransactionResult<>(result, readWriteTransaction.getCommitTimestamp());
  }

  public <T> TransactionResult<T> runInTransaction(
      Function<InTransactionContext, T> callable, String tagName) {
    final TransactionRunner readWriteTransaction =
        databaseClient.readWriteTransaction(Options.tag(tagName));
    final T result =
        readWriteTransaction.run(
            transaction -> {
              final InTransactionContext transactionContext =
                  new InTransactionContext(metadataTableName, transaction, this.dialect);
              return callable.apply(transactionContext);
            });
    return new TransactionResult<>(result, readWriteTransaction.getCommitTimestamp());
  }

  /** Represents the execution of a read / write transaction in Cloud Spanner. */
  public static class InTransactionContext {
    private static final Logger LOG = LoggerFactory.getLogger(InTransactionContext.class);

    private final String metadataTableName;
    private final TransactionContext transaction;
    private final Map<State, String> stateToTimestampColumn;
    private final Dialect dialect;

    /**
     * Constructs a context to execute a user defined function transactionally.
     *
     * @param metadataTableName the name of the partition metadata table
     * @param transaction the underlying client library transaction to be executed
     * @param dialect the dialect of the database.
     */
    public InTransactionContext(
        String metadataTableName, TransactionContext transaction, Dialect dialect) {
      this.metadataTableName = metadataTableName;
      this.transaction = transaction;
      this.stateToTimestampColumn = new HashMap<>();
      this.dialect = dialect;
      stateToTimestampColumn.put(State.CREATED, COLUMN_CREATED_AT);
      stateToTimestampColumn.put(State.SCHEDULED, COLUMN_SCHEDULED_AT);
      stateToTimestampColumn.put(State.RUNNING, COLUMN_RUNNING_AT);
      stateToTimestampColumn.put(State.FINISHED, COLUMN_FINISHED_AT);
    }

    /**
     * Inserts the partition metadata.
     *
     * @param row the partition metadata to be inserted
     */
    public Void insert(PartitionMetadata row) {
      transaction.buffer(ImmutableList.of(createInsertMetadataMutationFrom(row)));
      return null;
    }

    /**
     * Inserts the partition metadata in batch.
     *
     * @param rows the partition metadata objects to be inserted
     */
    public Void insert(List<PartitionMetadata> rows) {
      List<Mutation> mutations = new ArrayList<>();
      for (PartitionMetadata row : rows) {
        mutations.add(createInsertMetadataMutationFrom(row));
      }
      transaction.buffer(mutations);
      return null;
    }

    /**
     * Updates multiple partition rows to {@link State#SCHEDULED} state.
     *
     * @param composedPartitionTokens the partitions' unique identifiers
     */
    public Void updateToScheduled(List<String> composedPartitionTokens) {
      HashSet<String> tokens = new HashSet<>();
      Statement statement = getPartitionsMatchingState(composedPartitionTokens, State.CREATED);
      try (ResultSet resultSet =
          transaction.executeQuery(statement, Options.tag("getPartitionsMatchingState=CREATED"))) {
        while (resultSet.next()) {
          tokens.add(resultSet.getString(COLUMN_PARTITION_TOKEN));
        }
      }

      for (String composedPartitionToken : composedPartitionTokens) {
        if (!tokens.contains(composedPartitionToken)) {
          LOG.info("[{}] Did not update to be SCHEDULED", composedPartitionToken);
          continue;
        }

        LOG.info("[{}] Successfully updating to be SCHEDULED", composedPartitionToken);
        transaction.buffer(
            ImmutableList.of(
                createUpdateMetadataStateMutationFrom(composedPartitionToken, State.SCHEDULED)));
      }
      return null;
    }

    /**
     * Updates a partition row to {@link State#RUNNING} state.
     *
     * @param composedPartitionToken the partition unique identifier
     */
    public Void updateToRunning(String composedPartitionToken) {
      Statement statement =
          getPartitionsMatchingState(
              Collections.singletonList(composedPartitionToken), State.SCHEDULED);

      try (ResultSet resultSet =
          transaction.executeQuery(
              statement, Options.tag("getPartitionsMatchingState=SCHEDULED"))) {
        if (!resultSet.next()) {
          LOG.info("[{}] Did not update to be RUNNING", composedPartitionToken);
          return null;
        }
      }
      LOG.info("[{}] Successfully updating to be RUNNING", composedPartitionToken);
      transaction.buffer(
          ImmutableList.of(
              createUpdateMetadataStateMutationFrom(composedPartitionToken, State.RUNNING)));
      return null;
    }

    /**
     * Updates a partition row to {@link State#FINISHED} state.
     *
     * @param composedPartitionToken the partition unique identifier
     */
    public Void updateToFinished(String composedPartitionToken) {
      LOG.info("[{}] Successfully updating to be FINISHED", composedPartitionToken);
      transaction.buffer(
          ImmutableList.of(
              createUpdateMetadataStateMutationFrom(composedPartitionToken, State.FINISHED)));
      return null;
    }

    /**
     * Update the partition watermark to the given timestamp iff the partition watermark in metadata
     * table is smaller than the given watermark.
     *
     * @param composedPartitionToken the partition unique identifier
     * @param watermark the new partition watermark
     * @return the commit timestamp of the read / write transaction
     */
    public Void updateWatermark(String composedPartitionToken, Timestamp watermark) {
      Struct row =
          transaction.readRow(
              metadataTableName,
              Key.of(composedPartitionToken),
              Collections.singleton(COLUMN_WATERMARK));
      if (row == null) {
        LOG.error("[{}] Failed to read Watermark column", composedPartitionToken);
        return null;
      }
      Timestamp partitionWatermark = row.getTimestamp(COLUMN_WATERMARK);
      if (partitionWatermark.compareTo(watermark) < 0) {
        transaction.buffer(
            createUpdateMetadataWatermarkMutationFrom(composedPartitionToken, watermark));
      }
      return null;
    }

    /**
     * Fetches the partition metadata row data for the given composedPartitionToken.
     *
     * @param composedPartitionToken the partition unique identifier
     * @return the partition metadata for the given composedPartitionToken if it exists as a struct.
     *     Otherwise, it returns null.
     */
    public @Nullable Struct getPartition(String composedPartitionToken) {
      Statement statement;
      if (this.dialect == Dialect.POSTGRESQL) {
        statement =
            Statement.newBuilder(
                    "SELECT * FROM \""
                        + metadataTableName
                        + "\" WHERE \""
                        + COLUMN_PARTITION_TOKEN
                        + "\" = $1")
                .bind("p1")
                .to(composedPartitionToken)
                .build();

      } else {
        statement =
            Statement.newBuilder(
                    "SELECT * FROM "
                        + metadataTableName
                        + " WHERE "
                        + COLUMN_PARTITION_TOKEN
                        + " = @partition")
                .bind("partition")
                .to(composedPartitionToken)
                .build();
      }
      try (ResultSet resultSet =
          transaction.executeQuery(
              statement, Options.tag("getPartitionMetadataRowForGivenPartitionToken"))) {
        if (resultSet.next()) {
          return resultSet.getCurrentRowAsStruct();
        }
        return null;
      }
    }

    private Mutation createInsertMetadataMutationFrom(PartitionMetadata partitionMetadata) {
      return Mutation.newInsertBuilder(metadataTableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(
              composePartitionTokenWithTvfName(
                  partitionMetadata.getPartitionToken(), partitionMetadata.getTvfName()))
          .set(COLUMN_PARENT_TOKENS)
          .toStringArray(partitionMetadata.getParentTokens())
          .set(COLUMN_START_TIMESTAMP)
          .to(partitionMetadata.getStartTimestamp())
          .set(COLUMN_END_TIMESTAMP)
          .to(partitionMetadata.getEndTimestamp())
          .set(COLUMN_HEARTBEAT_MILLIS)
          .to(partitionMetadata.getHeartbeatMillis())
          .set(COLUMN_STATE)
          .to(partitionMetadata.getState().toString())
          .set(COLUMN_WATERMARK)
          .to(partitionMetadata.getWatermark())
          .set(COLUMN_CREATED_AT)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Statement getPartitionsMatchingState(
        List<String> composedPartitionTokens, State state) {
      Statement statement;
      if (this.dialect == Dialect.POSTGRESQL) {
        StringBuilder sqlStringBuilder =
            new StringBuilder("SELECT * FROM \"" + metadataTableName + "\"");
        sqlStringBuilder.append(" WHERE \"");
        sqlStringBuilder.append(COLUMN_STATE + "\" = " + "'" + state.toString() + "'");
        if (!composedPartitionTokens.isEmpty()) {
          sqlStringBuilder.append(" AND \"");
          sqlStringBuilder.append(COLUMN_PARTITION_TOKEN);
          sqlStringBuilder.append("\"");
          sqlStringBuilder.append(" = ANY (Array[");
          sqlStringBuilder.append(
              composedPartitionTokens.stream()
                  .map(s -> "'" + s + "'")
                  .collect(Collectors.joining(",")));
          sqlStringBuilder.append("])");
        }
        statement = Statement.newBuilder(sqlStringBuilder.toString()).build();
      } else {
        statement =
            Statement.newBuilder(
                    "SELECT * FROM "
                        + metadataTableName
                        + " WHERE "
                        + COLUMN_PARTITION_TOKEN
                        + " IN UNNEST(@composedPartitionTokens) AND "
                        + COLUMN_STATE
                        + " = @state")
                .bind("composedPartitionTokens")
                .to(Value.stringArray(new ArrayList<>(composedPartitionTokens)))
                .bind("state")
                .to(state.toString())
                .build();
      }
      return statement;
    }

    private Mutation createUpdateMetadataStateMutationFrom(
        String composedPartitionToken, State state) {
      final String timestampColumn = stateToTimestampColumn.get(state);
      if (timestampColumn == null) {
        throw new IllegalArgumentException("No timestamp column name found for state " + state);
      }
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(composedPartitionToken)
          .set(COLUMN_STATE)
          .to(state.toString())
          .set(timestampColumn)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }

    private Mutation createUpdateMetadataWatermarkMutationFrom(
        String composedPartitionToken, Timestamp watermark) {
      return Mutation.newUpdateBuilder(metadataTableName)
          .set(COLUMN_PARTITION_TOKEN)
          .to(composedPartitionToken)
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
