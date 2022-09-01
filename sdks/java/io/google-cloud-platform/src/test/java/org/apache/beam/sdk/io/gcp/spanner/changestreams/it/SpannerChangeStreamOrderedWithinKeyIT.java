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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.it;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end test of Cloud Spanner Change Streams Transactions Ordered Within Key. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamOrderedWithinKeyIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamOrderedWithinKeyIT.class);

  @ClassRule public static final IntegrationTestEnv ENV = new IntegrationTestEnv();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static String projectId;
  private static String instanceId;
  private static String databaseId;
  private static String tableName;
  private static String changeStreamName;
  private static DatabaseClient databaseClient;

  @BeforeClass
  public static void setup() throws InterruptedException, ExecutionException, TimeoutException {
    projectId = ENV.getProjectId();
    instanceId = ENV.getInstanceId();
    databaseId = ENV.getDatabaseId();
    tableName = ENV.createSingersTable();
    changeStreamName = ENV.createChangeStreamFor(tableName);
    databaseClient = ENV.getDatabaseClient();
  }

  @Test
  public void testOrderedWithinKey() {
    LOG.info("Test pipeline: " + pipeline.toString());
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);

    // Commit a initial transaction to get the timestamp to start reading from.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(insertRecordMutation(0));
    final com.google.cloud.Timestamp startTimestamp = databaseClient.write(mutations);

    // Get the timestamp of the last committed transaction to get the end timestamp.
    final com.google.cloud.Timestamp endTimestamp = writeTransactionsToDatabase();

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withInclusiveStartAt(startTimestamp)
                    .withInclusiveEndAt(endTimestamp))
            .apply(ParDo.of(new BreakRecordByModFn()))
            .apply(ParDo.of(new KeyByIdFn()))
            .apply(ParDo.of(new KeyValueByCommitTimestampAndRecordSequenceFn<>()))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(2))))
            .apply(GroupByKey.create())
            .apply(ParDo.of(new ToStringFn()));

    // Assert that the returned PCollection contains one entry per key for the committed
    // transactions, and that each entry contains the mutations in commit timestamp order.
    // Note that if inserts and updates to the same key are in the same transaction, the change
    // record for that transaction will only contain a record for the last update for that key.
    // Note that if an insert then a delete for a key happens in the same transaction, there will be
    // change records for that key.
    PAssert.that(tokens)
        .containsInAnyOrder(
            "{\"SingerId\":\"0\"}\n"
                + "{\"FirstName\":\"Inserting mutation 0\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 1\"};"
                + "{};"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 2\"};"
                + "{};",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 3\"};"
                + "{};",
            "{\"SingerId\":\"4\"}\n"
                + "{\"FirstName\":\"Inserting mutation 4\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"5\"}\n"
                + "{\"FirstName\":\"Updating mutation 5\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 5\"};"
                + "{};");
    pipeline.run().waitUntilFinish();
  }

  private static class BreakRecordByModFn extends DoFn<DataChangeRecord, DataChangeRecord> {
    private static final long serialVersionUID = 543765692722095666L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record, OutputReceiver<DataChangeRecord> outputReceiver) {
      final ChangeStreamRecordMetadata fakeChangeStreamMetadata =
          ChangeStreamRecordMetadata.newBuilder()
              .withPartitionToken("1")
              .withRecordTimestamp(com.google.cloud.Timestamp.ofTimeMicroseconds(2L))
              .withPartitionStartTimestamp(com.google.cloud.Timestamp.ofTimeMicroseconds(3L))
              .withPartitionEndTimestamp(com.google.cloud.Timestamp.ofTimeMicroseconds(4L))
              .withPartitionCreatedAt(com.google.cloud.Timestamp.ofTimeMicroseconds(5L))
              .withPartitionScheduledAt(com.google.cloud.Timestamp.ofTimeMicroseconds(6L))
              .withPartitionRunningAt(com.google.cloud.Timestamp.ofTimeMicroseconds(7L))
              .withQueryStartedAt(com.google.cloud.Timestamp.ofTimeMicroseconds(8L))
              .withRecordStreamStartedAt(com.google.cloud.Timestamp.ofTimeMicroseconds(9L))
              .withRecordStreamEndedAt(com.google.cloud.Timestamp.ofTimeMicroseconds(10L))
              .withRecordReadAt(com.google.cloud.Timestamp.ofTimeMicroseconds(11L))
              .withTotalStreamTimeMillis(12L)
              .withNumberOfRecordsRead(13L)
              .build();
      record.getMods().stream()
          .map(
              mod ->
                  new DataChangeRecord(
                      record.getPartitionToken(),
                      record.getCommitTimestamp(),
                      record.getServerTransactionId(),
                      record.isLastRecordInTransactionInPartition(),
                      record.getRecordSequence(),
                      record.getTableName(),
                      record.getRowType(),
                      Collections.singletonList(mod),
                      record.getModType(),
                      record.getValueCaptureType(),
                      record.getNumberOfRecordsInTransaction(),
                      record.getNumberOfPartitionsInTransaction(),
                      record.getTransactionTag(),
                      record.isSystemTransaction(),
                      fakeChangeStreamMetadata))
          .forEach(outputReceiver::output);
    }
  }

  private static class KeyByIdFn extends DoFn<DataChangeRecord, KV<String, DataChangeRecord>> {
    private static final long serialVersionUID = 5121794565566403405L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<KV<String, DataChangeRecord>> outputReceiver) {
      outputReceiver.output(KV.of(record.getMods().get(0).getKeysJson(), record));
    }
  }

  private static class KeyValueByCommitTimestampAndRecordSequenceFn<K>
      extends DoFn<KV<K, DataChangeRecord>, KV<K, KV<SortKey, DataChangeRecord>>> {
    private static final long serialVersionUID = -4059137464869088056L;

    @ProcessElement
    public void processElement(
        @Element KV<K, DataChangeRecord> recordByKey,
        OutputReceiver<KV<K, KV<SortKey, DataChangeRecord>>> outputReceiver) {
      final K key = recordByKey.getKey();
      final DataChangeRecord record = recordByKey.getValue();
      outputReceiver.output(
          KV.of(
              key,
              KV.of(
                  new SortKey(record.getCommitTimestamp(), record.getServerTransactionId()),
                  record)));
    }
  }

  private static class ToStringFn
      extends DoFn<KV<String, Iterable<KV<SortKey, DataChangeRecord>>>, String> {
    private static final long serialVersionUID = -2573561902102768101L;

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<KV<SortKey, DataChangeRecord>>> recordsByKey,
        OutputReceiver<String> outputReceiver) {
      final List<KV<SortKey, DataChangeRecord>> sortedRecords =
          StreamSupport.stream(recordsByKey.getValue().spliterator(), false)
              .sorted(Comparator.comparing(KV::getKey))
              .collect(Collectors.toList());

      final StringBuilder builder = new StringBuilder();
      builder.append(recordsByKey.getKey());
      builder.append("\n");
      for (KV<SortKey, DataChangeRecord> record : sortedRecords) {
        builder.append(record.getValue().getMods().get(0).getNewValuesJson() + ";");
      }

      outputReceiver.output(builder.toString());
    }
  }

  public static class SortKey implements Serializable, Comparable<SortKey> {
    private static final long serialVersionUID = 6923689796764239980L;
    private com.google.cloud.Timestamp commitTimestamp;
    private String transactionId;

    public SortKey() {}

    public SortKey(com.google.cloud.Timestamp commitTimestamp, String transactionId) {
      this.commitTimestamp = commitTimestamp;
      this.transactionId = transactionId;
    }

    public com.google.cloud.Timestamp getCommitTimestamp() {
      return commitTimestamp;
    }

    public void setCommitTimestamp(com.google.cloud.Timestamp commitTimestamp) {
      this.commitTimestamp = commitTimestamp;
    }

    public String getTransactionId() {
      return transactionId;
    }

    public void setTransactionId(String transactionId) {
      this.transactionId = transactionId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SortKey sortKey = (SortKey) o;
      return Objects.equals(commitTimestamp, sortKey.commitTimestamp)
          && Objects.equals(transactionId, sortKey.transactionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitTimestamp, transactionId);
    }

    @Override
    public int compareTo(SortKey other) {
      return Comparator.<SortKey>comparingDouble(
              sortKey ->
                  sortKey.getCommitTimestamp().getSeconds()
                      + sortKey.getCommitTimestamp().getNanos() / 1000000000.0)
          .thenComparing(sortKey -> sortKey.getTransactionId())
          .compare(this, other);
    }
  }

  private static com.google.cloud.Timestamp writeTransactionsToDatabase() {
    List<Mutation> mutations = new ArrayList<>();

    // 1. Commit a transaction to insert Singer 1 and Singer 2 into the table.
    mutations.add(insertRecordMutation(1));
    mutations.add(insertRecordMutation(2));
    com.google.cloud.Timestamp t1 = databaseClient.write(mutations);
    LOG.debug("The first transaction committed with timestamp: " + t1.toString());
    mutations.clear();

    // 2. Commmit a transaction to insert Singer 4 and remove Singer 1 from the table.
    mutations.add(updateRecordMutation(1));
    mutations.add(insertRecordMutation(4));
    com.google.cloud.Timestamp t2 = databaseClient.write(mutations);
    LOG.debug("The second transaction committed with timestamp: " + t2.toString());
    mutations.clear();

    // 3. Commit a transaction to insert Singer 3 and Singer 5.
    mutations.add(deleteRecordMutation(1));
    mutations.add(insertRecordMutation(3));
    mutations.add(insertRecordMutation(5));
    mutations.add(updateRecordMutation(5));
    com.google.cloud.Timestamp t3 = databaseClient.write(mutations);
    LOG.debug("The third transaction committed with timestamp: " + t3.toString());
    mutations.clear();

    // 4. Commit a transaction to update Singer 3 and Singer 2 in the table.
    mutations.add(updateRecordMutation(3));
    mutations.add(updateRecordMutation(2));
    com.google.cloud.Timestamp t4 = databaseClient.write(mutations);
    LOG.debug("The fourth transaction committed with timestamp: " + t4.toString());
    mutations.clear();

    // 5. Commit a transaction to delete 4, insert 1, delete 3, update 5.
    mutations.add(deleteRecordMutation(4));
    mutations.add(insertRecordMutation(1));
    mutations.add(deleteRecordMutation(3));
    mutations.add(updateRecordMutation(5));
    com.google.cloud.Timestamp t5 = databaseClient.write(mutations);

    LOG.debug("The fifth transaction committed with timestamp: " + t5.toString());
    mutations.clear();

    // 6. Commit a transaction to delete Singers 5, insert singers 6.
    mutations.add(deleteRecordMutation(5));
    mutations.add(insertRecordMutation(6));
    mutations.add(deleteRecordMutation(6));
    com.google.cloud.Timestamp t6 = databaseClient.write(mutations);
    LOG.debug("The sixth transaction committed with timestamp: " + t6.toString());
    mutations.clear();

    // 7. Delete remaining rows from database.
    mutations.add(deleteRecordMutation(1));
    mutations.add(deleteRecordMutation(2));
    mutations.add(deleteRecordMutation(0));
    com.google.cloud.Timestamp t7 = databaseClient.write(mutations);
    LOG.debug("The seventh transaction committed with timestamp: " + t7.toString());

    return t7;
  }

  // Create an update mutation.
  private static Mutation updateRecordMutation(long singerId) {
    return Mutation.newUpdateBuilder(tableName)
        .set("SingerId")
        .to(singerId)
        .set("FirstName")
        .to("Updating mutation " + singerId)
        .build();
  }

  // Create an insert mutation.
  private static Mutation insertRecordMutation(long singerId) {
    return Mutation.newInsertBuilder(tableName)
        .set("SingerId")
        .to(singerId)
        .set("FirstName")
        .to("Inserting mutation " + singerId)
        .build();
  }

  // Create a delete mutation.
  private static Mutation deleteRecordMutation(long singerId) {
    return Mutation.delete(tableName, KeySet.newBuilder().addKey(Key.of(singerId)).build());
  }
}
