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

import com.google.cloud.Timestamp;
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
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
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

/**
 * End-to-end test of Cloud Spanner Change Streams with strict commit timestamp and transaction
 * ordering.
 */
@RunWith(JUnit4.class)
public class SpannerChangeStreamOrderedByTimestampAndTransactionIdIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.class);

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
  public void testTransactionBoundaries() {
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    // Commit a initial transaction to get the timestamp to start reading from.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(insertRecordMutation(0, "FirstName0", "LastName0"));
    final long timeIncrementInSeconds = 2;
    final Timestamp startTimestamp = databaseClient.write(mutations);
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

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withInclusiveStartAt(startTimestamp)
                    .withInclusiveEndAt(endTimestamp))
            .apply(
                ParDo.of(
                    new SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.KeyBySortKeyFn()))
            .apply(
                ParDo.of(
                    new SpannerChangeStreamOrderedByTimestampAndTransactionIdIT
                        .CreateArtificialKeyFn()))
            .apply(
                ParDo.of(
                    new BufferRecordsUntilOutputTimestamp(endTimestamp, timeIncrementInSeconds)))
            .apply(
                ParDo.of(new SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.ToStringFn()));

    // Assert that the returned PCollection contains all six transactions (in string representation)
    // and that each transaction contains, in order, the list of mutations added.
    PAssert.that(tokens)
        .containsInAnyOrder(
            // Insert Singer 0 into the table.
            "{\"SingerId\":\"0\"},INSERT\n"

                // Insert Singer 1 and 2 into the table,
                + "{\"SingerId\":\"1\"}{\"SingerId\":\"2\"},INSERT\n"

                // Delete Singer 1 and Insert Singer 3 into the table.
                + "{\"SingerId\":\"1\"},DELETE\n"
                + "{\"SingerId\":\"3\"},INSERT\n"

                // Delete Singers 2 and 3.
                + "{\"SingerId\":\"2\"}{\"SingerId\":\"3\"},DELETE\n"

                // Delete Singer 0.
                + "{\"SingerId\":\"0\"},DELETE\n",

            // Second batch of transactions.
            // Insert Singer 1 and 2 into the table,
            "{\"SingerId\":\"1\"}{\"SingerId\":\"2\"},INSERT\n"

                // Delete Singer 1 and Insert Singer 3 into the table.
                + "{\"SingerId\":\"1\"},DELETE\n"
                + "{\"SingerId\":\"3\"},INSERT\n"

                // Delete Singers 2 and 3.
                + "{\"SingerId\":\"2\"}{\"SingerId\":\"3\"},DELETE\n",

            // Third batch of transactions.
            // Insert Singer 1 and 2 into the table,
            "{\"SingerId\":\"1\"}{\"SingerId\":\"2\"},INSERT\n"

                // Delete Singer 1 and Insert Singer 3 into the table.
                + "{\"SingerId\":\"1\"},DELETE\n"
                + "{\"SingerId\":\"3\"},INSERT\n"

                // Delete Singers 2 and 3.
                + "{\"SingerId\":\"2\"}{\"SingerId\":\"3\"},DELETE\n");

    pipeline
        .runWithAdditionalOptionArgs(Collections.singletonList("--streaming"))
        .waitUntilFinish();
  }

  // KeyByTransactionIdFn takes in a DataChangeRecord and outputs a key-value pair of
  // {SortKey, DataChangeRecord}
  private static class KeyBySortKeyFn
      extends DoFn<
          DataChangeRecord,
          KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>> {

    private static final long serialVersionUID = 1270485392415293532L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<
                KV<
                    SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                    DataChangeRecord>>
            outputReceiver) {
      outputReceiver.output(
          KV.of(
              new SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey(
                  record.getCommitTimestamp(), record.getServerTransactionId()),
              record));
    }
  }

  // CreateArtificialKeyFn keys each input element by an artifical byte key. This is because buffers
  // and timers are per key and window, and we want to buffer all data change records in a time
  // interval, rather than buffer per key.
  private static class CreateArtificialKeyFn
      extends DoFn<
          KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>,
          KV<
              byte[],
              KV<
                  SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                  DataChangeRecord>>> {
    private static final long serialVersionUID = -3363057370822294686L;

    @ProcessElement
    public void processElement(
        @Element
            KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>
                element,
        OutputReceiver<
                KV<
                    byte[],
                    KV<
                        SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                        DataChangeRecord>>>
            outputReceiver) {
      outputReceiver.output(KV.of(new byte[0], element));
    }
  }

  // Timers and buffers are per-key.
  // Buffer each data change record until the watermark passes the timestamp at which we want
  // to output the buffered data change records.
  // We utilize a looping timer to determine when to flush the buffer:
  //
  // 1. When we see a data change record for the first time (i.e. no data change records in
  //    the buffer), we will set the timer to fire at an interval after the data change record's
  //    timestamp.
  // 2. Then, when the timer fires, if the current timer's expiration time is before the pipeline
  //    end time, if set, we still have data left to process. We will set the next timer to the
  //    current timer's expiration time plus incrementIntervalInSeconds.
  // 3. Otherwise, we will not set a timer.
  //
  private static class BufferRecordsUntilOutputTimestamp
      extends DoFn<
          KV<
              byte[],
              KV<
                  SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                  DataChangeRecord>>,
          Iterable<
              KV<
                  SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                  DataChangeRecord>>> {
    private static final long serialVersionUID = 5050535558953049259L;

    private final long incrementIntervalInSeconds;
    private final @Nullable Instant pipelineEndTime;

    private BufferRecordsUntilOutputTimestamp(
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
            BagState<
                KV<
                    SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                    DataChangeRecord>>>
        buffer = StateSpecs.bag();

    @SuppressWarnings("unused")
    @StateId("keySeen")
    private final StateSpec<ValueState<Boolean>> keySeen = StateSpecs.value(BooleanCoder.of());

    @ProcessElement
    public void process(
        @Element
            KV<
                    byte[],
                    KV<
                        SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                        DataChangeRecord>>
                element,
        @StateId("buffer")
            BagState<
                    KV<
                        SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                        DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer,
        @StateId("keySeen") ValueState<Boolean> keySeen) {
      buffer.add(element.getValue());

      // Only set the timer if this is the first time we are receiving a data change record
      // with this key.
      Boolean hasKeyBeenSeen = keySeen.read();
      if (hasKeyBeenSeen == null) {
        Instant commitTimestamp =
            new Instant(element.getValue().getKey().getCommitTimestamp().toSqlTimestamp());
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
            BagState<
                    KV<
                        SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                        DataChangeRecord>>
                buffer,
        @TimerId("timer") Timer timer) {
      if (!buffer.isEmpty().read()) {
        final List<
                KV<
                    SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                    DataChangeRecord>>
            records =
                StreamSupport.stream(buffer.read().spliterator(), false)
                    .collect(Collectors.toList());
        buffer.clear();

        List<KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>>
            recordsToOutput = new ArrayList<>();
        for (KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>
            record : records) {
          Instant recordCommitTimestamp =
              new Instant(record.getKey().getCommitTimestamp().toSqlTimestamp());
          // When the watermark passes time T, this means that all records with event time < T
          // have been processed and successfully committed. Since the timer fires when the
          // watermark passes the expiration time, we should only output records with event time
          // < expiration time.
          final String recordString = getRecordString(record.getValue());
          if (recordCommitTimestamp.isBefore(context.timestamp())) {
            LOG.debug(
                "Outputting transactions {} with id {} at expiration timestamp {}",
                recordString,
                record.getKey().toString(),
                context.timestamp().toString());
            recordsToOutput.add(record);
          } else {
            LOG.debug(
                "Expired at {} but adding transaction {} back to buffer "
                    + "due to commit timestamp {}",
                context.timestamp().toString(),
                recordString,
                recordCommitTimestamp.toString());
            buffer.add(record);
          }
        }

        // Output records, if there are any to output.
        if (!recordsToOutput.isEmpty()) {
          context.outputWithTimestamp(recordsToOutput, context.timestamp());
          LOG.debug(
              "Expired at {}, outputting records for key {}",
              context.timestamp().toString(),
              recordsToOutput.get(0).getKey().toString());
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

  // ToStringFn takes in a list of key-value pairs of SortKey, Iterable<DataChangeRecord> and
  // outputs a string representation.
  private static class ToStringFn
      extends DoFn<
          Iterable<
              KV<
                  SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                  DataChangeRecord>>,
          String> {

    private static final long serialVersionUID = 2307936669684679038L;

    @ProcessElement
    public void processElement(
        @Element
            Iterable<
                    KV<
                        SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey,
                        DataChangeRecord>>
                element,
        OutputReceiver<String> outputReceiver) {
      final StringBuilder builder = new StringBuilder();

      List<KV<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey, DataChangeRecord>>
          sortedTransactions =
              StreamSupport.stream(element.spliterator(), false)
                  .sorted((kv1, kv2) -> kv1.getKey().compareTo(kv2.getKey()))
                  .collect(Collectors.toList());

      sortedTransactions.forEach(
          record -> {
            builder.append(getRecordString(record.getValue()));
          });
      outputReceiver.output(builder.toString());
    }
  }

  // Get a string representation of the mods and the mod type in the data change record.
  private static String getRecordString(DataChangeRecord record) {
    final StringBuilder builder = new StringBuilder();
    String modString = "";
    for (Mod mod : record.getMods()) {
      modString += mod.getKeysJson();
    }
    builder.append(String.join(",", modString, record.getModType().toString()));
    builder.append("\n");
    return builder.toString();
  }

  private Timestamp writeTransactionsToDatabase() {
    List<Mutation> mutations = new ArrayList<>();

    // 1. Commit a transaction to insert Singer 1 and Singer 2 into the table.
    mutations.add(insertRecordMutation(1, "FirstName1", "LastName2"));
    mutations.add(insertRecordMutation(2, "FirstName2", "LastName2"));
    Timestamp t1 = databaseClient.write(mutations);
    LOG.debug("The first transaction committed with timestamp: " + t1.toString());
    mutations.clear();

    // 2. Commmit a transaction to insert Singer 3 and remove Singer 1 from the table.
    mutations.add(insertRecordMutation(3, "FirstName3", "LastName3"));
    mutations.add(deleteRecordMutation(1));
    Timestamp t2 = databaseClient.write(mutations);
    LOG.debug("The second transaction committed with timestamp: " + t2.toString());
    mutations.clear();

    // 3. Commit a transaction to delete Singer 2 and Singer 3 from the table.
    mutations.add(deleteRecordMutation(2));
    mutations.add(deleteRecordMutation(3));
    Timestamp t3 = databaseClient.write(mutations);
    LOG.debug("The third transaction committed with timestamp: " + t3.toString());
    mutations.clear();

    // 4. Commit a transaction to delete Singer 0.
    mutations.add(deleteRecordMutation(0));
    Timestamp t4 = databaseClient.write(mutations);
    LOG.debug("The fourth transaction committed with timestamp: " + t4.toString());
    return t4;
  }

  // Create an insert mutation.
  private static Mutation insertRecordMutation(long singerId, String firstName, String lastName) {
    return Mutation.newInsertBuilder(tableName)
        .set("SingerId")
        .to(singerId)
        .set("FirstName")
        .to(firstName)
        .set("LastName")
        .to(lastName)
        .build();
  }

  // Create a delete mutation.
  private static Mutation deleteRecordMutation(long singerId) {
    return Mutation.delete(tableName, KeySet.newBuilder().addKey(Key.of(singerId)).build());
  }

  private static class SortKey
      implements Serializable,
          Comparable<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey> {

    private static final long serialVersionUID = 2105939115467195036L;

    private Timestamp commitTimestamp;
    private String transactionId;

    public SortKey() {}

    public SortKey(Timestamp commitTimestamp, String transactionId) {
      this.commitTimestamp = commitTimestamp;
      this.transactionId = transactionId;
    }

    public static long getSerialVersionUID() {
      return serialVersionUID;
    }

    public Timestamp getCommitTimestamp() {
      return commitTimestamp;
    }

    public void setCommitTimestamp(Timestamp commitTimestamp) {
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
      SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey sortKey =
          (SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey) o;
      return Objects.equals(commitTimestamp, sortKey.commitTimestamp)
          && Objects.equals(transactionId, sortKey.transactionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitTimestamp, transactionId);
    }

    @Override
    public int compareTo(SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey other) {
      return Comparator
          .<SpannerChangeStreamOrderedByTimestampAndTransactionIdIT.SortKey>comparingDouble(
              sortKey ->
                  sortKey.getCommitTimestamp().getSeconds()
                      + sortKey.getCommitTimestamp().getNanos() / 1000000000.0)
          .thenComparing(sortKey -> sortKey.getTransactionId())
          .compare(this, other);
    }
  }
}
