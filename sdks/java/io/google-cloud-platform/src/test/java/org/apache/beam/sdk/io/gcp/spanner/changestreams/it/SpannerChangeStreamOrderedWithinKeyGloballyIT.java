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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
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
    final long timeIncrementInSeconds = 2;

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
      LOG.error(e.toString(), e);
    }

    // This will be the second batch of transactions that will have strict timestamp ordering
    // per key.
    writeTransactionsToDatabase();

    // Sleep the time increment interval.
    try {
      Thread.sleep(timeIncrementInSeconds * 1000);
    } catch (InterruptedException e) {
      LOG.error(e.toString(), e);
    }

    // This will be the final batch of transactions that will have strict timestamp ordering
    // per key.
    com.google.cloud.Timestamp endTimestamp = writeTransactionsToDatabase();

    LOG.info(
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
            .apply(ParDo.of(new BufferKeyUntilOutputTimestamp(timeIncrementInSeconds)))
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
                + "{};",
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",

            // Second batch of records ordered within key.
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",

            // Third batch of records ordered within key.
            "{\"SingerId\":\"1\"}\n"
                + "{\"FirstName\":\"Inserting mutation 1\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"2\"}\n"
                + "{\"FirstName\":\"Inserting mutation 2\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};",
            "{\"SingerId\":\"3\"}\n"
                + "{\"FirstName\":\"Inserting mutation 3\",\"LastName\":null,\"SingerInfo\":null};"
                + "{};");

    pipeline
        .runWithAdditionalOptionArgs(Collections.singletonList("--streaming"))
        .waitUntilFinish();
  }

  // Data change records may contain multiple mods if there are multiple primary keys.
  // Break each data change record into one or more data change records, one per each mod.
  private static class BreakRecordByModFn extends DoFn<DataChangeRecord, DataChangeRecord> {
    private static final long serialVersionUID = 543765692722095666L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record, OutputReceiver<DataChangeRecord> outputReceiver) {
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
                      record.getMetadata()))
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

    private BufferKeyUntilOutputTimestamp(long incrementIntervalInSeconds) {
      this.incrementIntervalInSeconds = incrementIntervalInSeconds;
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
    @StateId("seenKey")
    private final StateSpec<ValueState<String>> seenKey = StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    public void process(
        @Element
            KV<String, KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                element,
        @StateId("buffer")
            BagState<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer,
        @StateId("seenKey") ValueState<String> seenKey) {
      buffer.add(element.getValue());

      // Only set the timer if this is the first time we are receiving a data change record
      // with this key.
      String hasKeyBeenSeen = seenKey.read();
      if (hasKeyBeenSeen == null) {
        Instant commitTimestamp =
            new Instant(element.getValue().getValue().getCommitTimestamp().toSqlTimestamp());
        Instant outputTimestamp =
            commitTimestamp.plus(Duration.standardSeconds(incrementIntervalInSeconds));
        LOG.info("Setting timer at {} for key {}", outputTimestamp.toString(), element.getKey());
        timer.set(outputTimestamp);
        seenKey.write(element.getKey());
      }
    }

    @OnTimer("timer")
    public void onExpiry(
        OnTimerContext context,
        @StateId("buffer")
            BagState<KV<SpannerChangeStreamOrderedWithinKeyGloballyIT.SortKey, DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer,
        @StateId("seenKey") ValueState<String> seenKey) {
      String keyForTimer = seenKey.read();
      Instant timerContextTimestamp = context.timestamp();
      LOG.info(
          "Timer reached expiration time for key {} and for timestamp {}",
          keyForTimer,
          timerContextTimestamp);
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
          final String recordString = record.getValue().getMods().get(0).getNewValuesJson();
          // When the watermark passes time T, this means that all records with event time < T
          // have been processed and successfully committed. Since the timer fires when the
          // watermark passes the expiration time, we should only output records with event time
          // < expiration time.
          if (recordCommitTimestamp.isBefore(timerContextTimestamp)) {
            LOG.info(
                "Outputting record with key {} and value \"{}\" at expiration timestamp {}",
                record.getValue().getMods().get(0).getKeysJson(),
                recordString,
                timerContextTimestamp.toString());
            recordsToOutput.add(record);
          } else {
            LOG.info(
                "Expired at {} but adding record with key {} and value {} back to buffer "
                    + "due to commit timestamp {}",
                timerContextTimestamp.toString(),
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
              timerContextTimestamp);
          LOG.info(
              "Expired at {}, outputting records for key and context timestamp {}",
              timerContextTimestamp.toString(),
              recordsToOutput.get(0).getValue().getMods().get(0).getKeysJson());
        } else {
          LOG.info("Expired at {} with no records", timerContextTimestamp.toString());
        }
      }

      Instant nextTimer =
          timerContextTimestamp.plus(Duration.standardSeconds(incrementIntervalInSeconds));
      if (buffer.isEmpty() != null && !buffer.isEmpty().read()) {
        LOG.info("Setting next timer to {} for key {}", nextTimer.toString(), keyForTimer);
        timer.set(nextTimer);
      } else {
        LOG.info("Timer not being set since the buffer is empty for key {} ", keyForTimer);
        seenKey.clear();
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

  private com.google.cloud.Timestamp writeTransactionsToDatabase() {
    List<Mutation> mutations = new ArrayList<>();

    // 1. Commit a transaction to insert Singer 1 and Singer 2 into the table.
    mutations.add(insertRecordMutation(1));
    mutations.add(insertRecordMutation(2));
    com.google.cloud.Timestamp t1 = databaseClient.write(mutations);
    LOG.info("The first transaction committed with timestamp: " + t1.toString());
    mutations.clear();

    // 2. Commmit a transaction to insert Singer 3 and remove Singer 1 from the table.
    mutations.add(insertRecordMutation(3));
    mutations.add(deleteRecordMutation(1));
    com.google.cloud.Timestamp t2 = databaseClient.write(mutations);
    LOG.info("The second transaction committed with timestamp: " + t2.toString());
    mutations.clear();

    // 3. Commit a transaction to delete Singer 2 and Singer 3 from the table.
    mutations.add(deleteRecordMutation(2));
    mutations.add(deleteRecordMutation(3));
    com.google.cloud.Timestamp t3 = databaseClient.write(mutations);
    LOG.info("The third transaction committed with timestamp: " + t3.toString());
    mutations.clear();

    // 4. Commit a transaction to delete Singer 0.
    mutations.add(deleteRecordMutation(0));
    com.google.cloud.Timestamp t4 = databaseClient.write(mutations);
    LOG.info("The fourth transaction committed with timestamp: " + t4.toString());
    return t4;
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
