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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end test of Cloud Spanner Change Streams Transactions Ordered Within Key Globally. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamOrderedWithinKeyGloballyIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamOrderedWithinKeyGloballyIT.class);

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
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);

    // Get the time increment interval at which to flush data changes ordered by key.
    final long timeIncrementInSeconds = 70;

    // Commit a initial transaction to get the timestamp to start reading from.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(insertRecordMutation(0));
    final com.google.cloud.Timestamp startTimestamp = databaseClient.write(mutations);

    // This will be the first batch of transactions that will have strict timestamp ordering
    // per key.
    writeTransactionsToDatabase();

    // Sleep the time increment interval.
    try {
      Thread.sleep(timeIncrementInSeconds * 1000);
    } catch (InterruptedException e) {
      System.out.println(e);
    }

    // This will be the second batch of transactions that will have strict timestamp ordering
    // per key.
    writeTransactionsToDatabase();

    // Sleep the time increment interval.
    try {
      Thread.sleep(timeIncrementInSeconds * 1000);
    } catch (InterruptedException e) {
      System.out.println(e);
    }

    // This will be the final batch of transactions that will have strict timestamp ordering
    // per key.
    com.google.cloud.Timestamp endTimestamp = writeTransactionsToDatabase();

    LOG.debug(
        "Reading change streams from {} to {}", startTimestamp.toString(), endTimestamp.toString());

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
            .apply(ParDo.of(new KeyValueByCommitTimestampAndTransactionIdFn<>()))
            .apply(
                ParDo.of(new BufferKeyUntilOutputTimestamp(endTimestamp, timeIncrementInSeconds)))
            .apply(ParDo.of(new ToStringFn()));

    // Assert that the returned PCollection contains one entry per key for the committed
    // transactions, and that each entry contains the mutations in commit timestamp order.
    // Note that if inserts and updates to the same key are in the same transaction, the change
    // record for that transaction will only contain a record for the last update for that key.
    // Note that if an insert then a delete for a key happens in the same transaction, there will be
    // change records for that key.
    PAssert.that(tokens)
        .containsInAnyOrder(
            // First batch of records ordered within key.
            "{\"SingerId\":\"0\"}\n"
                + "{\"FirstName\":\"Inserting mutation 0\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 1\"};"
                + "Deleted record;"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 2\"};"
                + "Deleted record;",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 3\"};"
                + "Deleted record;",
            "{\"SingerId\":\"4\"}\n"
                + "{\"FirstName\":\"Inserting mutation 4\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"5\"}\n"
                + "{\"FirstName\":\"Updating mutation 5\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 5\"};"
                + "Deleted record;",

            // Second batch of records ordered within key.
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 1\"};"
                + "Deleted record;"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 2\"};"
                + "Deleted record;",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 3\"};"
                + "Deleted record;",
            "{\"SingerId\":\"4\"}\n"
                + "{\"FirstName\":\"Inserting mutation 4\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"5\"}\n"
                + "{\"FirstName\":\"Updating mutation 5\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 5\"};"
                + "Deleted record;",

            // Third batch of records ordered within key.
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 1\"};"
                + "Deleted record;"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 2\"};"
                + "Deleted record;",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 3\"};"
                + "Deleted record;",
            "{\"SingerId\":\"4\"}\n"
                + "{\"FirstName\":\"Inserting mutation 4\",\"LastName\":null,\"SingerInfo\":null};"
                + "Deleted record;",
            "{\"SingerId\":\"5\"}\n"
                + "{\"FirstName\":\"Updating mutation 5\",\"LastName\":null,\"SingerInfo\":null};"
                + "{\"FirstName\":\"Updating mutation 5\"};"
                + "Deleted record;");

    pipeline.run().waitUntilFinish();
  }

  // Data change records may contain multiple mods if there are multiple primary keys.
  // Break each data change record into one or more data change records, one per each mod.
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
                      fakeChangeStreamMetadata))
          .forEach(outputReceiver::output);
    }
  }

  // Key the data change record by its commit timestamp and server transaction ID.
  private static class KeyValueByCommitTimestampAndTransactionIdFn<K>
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

  // Key the data change record by its JSON key string.
  private static class KeyByIdFn extends DoFn<DataChangeRecord, KV<String, DataChangeRecord>> {
    private static final long serialVersionUID = 5121794565566403405L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<KV<String, DataChangeRecord>> outputReceiver) {
      outputReceiver.output(KV.of(record.getMods().get(0).getKeysJson(), record));
    }
  }

  // Timers and buffers are per-key.
  // Buffer each data change record until the watermark passes the timestamp at which we want
  // to output the buffered data change records.
  // We utilize a looping timer to determine when to flush the buffer:
  //
  // 1. When we see a data change record for a key for the first time, we will set the timer to
  //    fire at the data change record's commit timestamp.
  // 2. Then, when the timer fires, if the current timer's expiration time is before the pipeline
  //    end time, if set, we still have data left to process. We will set the next timer to the
  //    current timer's expiration time plus incrementIntervalInSeconds.
  // 3. Otherwise, we will not set a timer.
  //
  private static class BufferKeyUntilOutputTimestamp
      extends DoFn<
          KV<String, KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>,
          KV<
              String,
              Iterable<
                  KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>>> {
    private static final long serialVersionUID = 5050535558953049259L;

    private final long incrementIntervalInSeconds;
    private final @Nullable Instant pipelineEndTime;

    private BufferKeyUntilOutputTimestamp(
        @Nullable com.google.cloud.Timestamp endTimestamp, long incrementIntervalInSeconds) {
      this.incrementIntervalInSeconds = incrementIntervalInSeconds;
      if (endTimestamp != null) {
        this.pipelineEndTime = new Instant(endTimestamp.toSqlTimestamp());
      } else {
        pipelineEndTime = null;
      }
    }

    @SuppressWarnings("unused")
    @TimerId("timer")
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @SuppressWarnings("unused")
    @StateId("buffer")
    private final StateSpec<
            BagState<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>>
        buffer = StateSpecs.bag();

    @SuppressWarnings("unused")
    @StateId("keySeen")
    private final StateSpec<ValueState<Boolean>> keySeen = StateSpecs.value(BooleanCoder.of());

    @ProcessElement
    public void process(
        @Element
            KV<String, KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                element,
        @StateId("buffer")
            BagState<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer,
        @StateId("keySeen") ValueState<Boolean> keySeen) {
      buffer.add(element.getValue());

      // Only set the timer if this is the first time we are receiving a data change record
      // with this key.
      Boolean hasKeyBeenSeen = keySeen.read();
      if (hasKeyBeenSeen == null) {
        Instant commitTimestamp =
            new Instant(element.getValue().getValue().getCommitTimestamp().toSqlTimestamp());
        Instant outputTimestamp =
            commitTimestamp.plus(Duration.standardSeconds(incrementIntervalInSeconds));
        LOG.debug("Setting timer at {} for key {}", outputTimestamp.toString(), element.getKey());
        timer.set(outputTimestamp);
        keySeen.write(true);
      }
    }

    @OnTimer("timer")
    public void onExpiry(
        OnTimerContext context,
        @StateId("buffer")
            BagState<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer) {
      if (!buffer.isEmpty().read()) {
        final List<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
            records =
                StreamSupport.stream(buffer.read().spliterator(), false)
                    .collect(Collectors.toList());
        buffer.clear();

        List<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
            recordsToOutput = new ArrayList<>();
        for (KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord> record :
            records) {
          Instant recordCommitTimestamp =
              new Instant(record.getValue().getCommitTimestamp().toSqlTimestamp());
          final String recordString =
              record.getValue().getMods().get(0).getNewValuesJson().isEmpty()
                  ? "Deleted record"
                  : record.getValue().getMods().get(0).getNewValuesJson();
          // When the watermark passes time T, this means that all records with event time < T
          // have been processed and successfully committed. Since the timer fires when the
          // watermark passes the expiration time, we should only output records with event time
          // < expiration time.
          if (recordCommitTimestamp.isBefore(context.timestamp())) {
            LOG.debug(
                "Outputting record with key {} and value \"{}\" at expiration timestamp {}",
                record.getValue().getMods().get(0).getKeysJson(),
                recordString,
                context.timestamp().toString());
            recordsToOutput.add(record);
          } else {
            LOG.debug(
                "Expired at {} but adding record with key {} and value {} back to buffer "
                    + "due to commit timestamp {}",
                context.timestamp().toString(),
                record.getValue().getMods().get(0).getKeysJson(),
                recordString,
                recordCommitTimestamp.toString());
            buffer.add(record);
          }
        }

        // Output records, if there are any to output.
        if (!recordsToOutput.isEmpty()) {
          context.outputWithTimestamp(
              KV.of(
                  recordsToOutput.get(0).getValue().getMods().get(0).getKeysJson(),
                  recordsToOutput),
              context.timestamp());
          LOG.debug(
              "Expired at {}, outputting records for key {}",
              context.timestamp().toString(),
              recordsToOutput.get(0).getValue().getMods().get(0).getKeysJson());
        } else {
          LOG.debug("Expired at {} with no records", context.timestamp().toString());
        }
      }

      Instant nextTimer =
          context.timestamp().plus(Duration.standardSeconds(incrementIntervalInSeconds));
      if (pipelineEndTime == null || context.timestamp().isBefore(pipelineEndTime)) {
        // If the current timer's timestamp is before the pipeline end time, or there is no
        // pipeline end time, we still have data left to process.
        LOG.debug("Setting next timer to {}", nextTimer.toString());
        timer.set(nextTimer);
      } else {
        LOG.debug(
            "Timer not being set as exceeded pipeline end time: " + pipelineEndTime.toString());
      }
    }
  }

  // Output a string representation of the iterable of data change records ordered by commit
  // timestamp and record sequence per key.
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
        builder.append(
            record.getValue().getMods().get(0).getNewValuesJson().isEmpty()
                ? "Deleted record;"
                : record.getValue().getMods().get(0).getNewValuesJson() + ";");
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
