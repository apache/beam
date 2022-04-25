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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end test of Cloud Spanner Change Streams Transaction Boundaries. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamTransactionBoundariesIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(SpannerChangeStreamTransactionBoundariesIT.class);

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
    LOG.info("Test pipeline: " + pipeline.toString());
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);

    // Commit a initial transaction to get the timestamp to start reading from.
    List<Mutation> mutations = new ArrayList<>();
    mutations.add(insertRecordMutation(0, "FirstName0", "LastName0"));
    final Timestamp startTimestamp = databaseClient.write(mutations);

    // Get the timestamp of the last committed transaction to get the end timestamp.
    final Timestamp endTimestamp = writeTransactionsToDatabase();

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withInclusiveStartAt(startTimestamp)
                    .withInclusiveEndAt(endTimestamp))
            .apply(ParDo.of(new SpannerChangeStreamTransactionBoundariesIT.KeyByTransactionIdFn()))
            .apply(ParDo.of(new SpannerChangeStreamTransactionBoundariesIT.TransactionBoundaryFn()))
            .apply(ParDo.of(new SpannerChangeStreamTransactionBoundariesIT.ToStringFn()));

    // Assert that the returned PCollection contains all six transactions (in string representation)
    // and that each transaction contains, in order, the list of mutations added.
    PAssert.that(tokens)
        .containsInAnyOrder(
            // Insert Singer 0 into the table.
            "{\"SingerId\":\"0\"},INSERT\n",

            // Insert Singer 1 and 2 into the table,
            "{\"SingerId\":\"1\"}{\"SingerId\":\"2\"},INSERT\n",

            // Delete Singer 1 and Insert Singer 3 into the table.
            "{\"SingerId\":\"1\"},DELETE\n" + "{\"SingerId\":\"3\"},INSERT\n",

            // Insert Singers 4, 5, 6 into the table.
            "{\"SingerId\":\"4\"}{\"SingerId\":\"5\"}{\"SingerId\":\"6\"},INSERT\n",

            // Update Singer 6 and Insert Singer 7
            "{\"SingerId\":\"6\"},UPDATE\n" + "{\"SingerId\":\"7\"},INSERT\n",

            // Update Singers 4 and 5 in the table.
            "{\"SingerId\":\"4\"}{\"SingerId\":\"5\"},UPDATE\n",

            // Delete Singers 3, 4, 5 from the table.
            "{\"SingerId\":\"3\"}{\"SingerId\":\"4\"}{\"SingerId\":\"5\"},DELETE\n",

            // Delete Singers 0, 2, 6, 7;
            "{\"SingerId\":\"0\"}{\"SingerId\":\"2\"}{\"SingerId\":\"6\"}"
                + "{\"SingerId\":\"7\"},DELETE\n");

    final PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
  }

  // KeyByTransactionIdFn takes in a DataChangeRecord and outputs a key-value pair of
  // {TransactionId, DataChangeRecord}
  private static class KeyByTransactionIdFn
      extends DoFn<DataChangeRecord, KV<String, DataChangeRecord>> {

    private static final long serialVersionUID = 1270485392415293532L;

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record,
        OutputReceiver<KV<String, DataChangeRecord>> outputReceiver) {
      outputReceiver.output(KV.of(record.getServerTransactionId(), record));
    }
  }

  // TransactionBoundaryFn buffers received key-value pairs of {TransactionId, DataChangeRecord}
  // from KeyByTransactionIdFn and buffers them in groups based on TransactionId.
  // When the number of records buffered is equal to the number of records contained in the
  // entire transaction, this function sorts the DataChangeRecords in the group by record sequence
  // and outputs a key-value pair of SortKey(CommitTimestamp, TransactionId),
  // Iterable<DataChangeRecord>.
  private static class TransactionBoundaryFn
      extends DoFn<
          KV<String, DataChangeRecord>,
          KV<SpannerChangeStreamTransactionBoundariesIT.SortKey, Iterable<DataChangeRecord>>> {

    private static final long serialVersionUID = 5050535558953049259L;

    @SuppressWarnings("UnusedVariable")
    @StateId("buffer")
    private final StateSpec<BagState<DataChangeRecord>> buffer = StateSpecs.bag();

    @SuppressWarnings("UnusedVariable")
    @StateId("count")
    private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();

    @ProcessElement
    public void process(
        ProcessContext context,
        @StateId("buffer") BagState<DataChangeRecord> buffer,
        @StateId("count") ValueState<Integer> countState) {
      final KV<String, DataChangeRecord> element = context.element();
      final DataChangeRecord record = element.getValue();

      buffer.add(record);
      int count = (countState.read() != null ? countState.read() : 0);
      count = count + 1;
      countState.write(count);

      if (count == record.getNumberOfRecordsInTransaction()) {
        final List<DataChangeRecord> sortedRecords =
            StreamSupport.stream(buffer.read().spliterator(), false)
                .sorted(Comparator.comparing(DataChangeRecord::getRecordSequence))
                .collect(Collectors.toList());

        final Instant commitInstant =
            new Instant(sortedRecords.get(0).getCommitTimestamp().toSqlTimestamp().getTime());
        context.outputWithTimestamp(
            KV.of(
                new SpannerChangeStreamTransactionBoundariesIT.SortKey(
                    sortedRecords.get(0).getCommitTimestamp(),
                    sortedRecords.get(0).getServerTransactionId()),
                sortedRecords),
            commitInstant);
        buffer.clear();
        countState.clear();
      }
    }
  }

  // ToStringFn takes in a key-value pair of SortKey, Iterable<DataChangeRecord> and outputs
  // a string representation.
  private static class ToStringFn
      extends DoFn<
          KV<SpannerChangeStreamTransactionBoundariesIT.SortKey, Iterable<DataChangeRecord>>,
          String> {

    private static final long serialVersionUID = 2307936669684679038L;

    @ProcessElement
    public void processElement(
        @Element
            KV<SpannerChangeStreamTransactionBoundariesIT.SortKey, Iterable<DataChangeRecord>>
                element,
        OutputReceiver<String> outputReceiver) {
      final StringBuilder builder = new StringBuilder();
      final Iterable<DataChangeRecord> sortedRecords = element.getValue();
      sortedRecords.forEach(
          record -> {
            // Output the string representation of the mods and the mod type for each data change
            // record.
            String modString = "";
            for (Mod mod : record.getMods()) {
              modString += mod.getKeysJson();
            }
            builder.append(String.join(",", modString, record.getModType().toString()));
            builder.append("\n");
          });
      outputReceiver.output(builder.toString());
    }
  }

  private static class SortKey
      implements Serializable, Comparable<SpannerChangeStreamTransactionBoundariesIT.SortKey> {

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
      SpannerChangeStreamTransactionBoundariesIT.SortKey sortKey =
          (SpannerChangeStreamTransactionBoundariesIT.SortKey) o;
      return Objects.equals(commitTimestamp, sortKey.commitTimestamp)
          && Objects.equals(transactionId, sortKey.transactionId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitTimestamp, transactionId);
    }

    @Override
    public int compareTo(SpannerChangeStreamTransactionBoundariesIT.SortKey other) {
      return Comparator.<SpannerChangeStreamTransactionBoundariesIT.SortKey>comparingDouble(
              sortKey ->
                  sortKey.getCommitTimestamp().getSeconds()
                      + sortKey.getCommitTimestamp().getNanos() / 1000000000.0)
          .thenComparing(sortKey -> sortKey.getTransactionId())
          .compare(this, other);
    }
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

    // 3. Commit a transaction to insert Singer 4 and Singer 5 and Singer 6 into the table.
    mutations.add(insertRecordMutation(4, "FirstName4", "LastName4"));
    mutations.add(insertRecordMutation(5, "FirstName5", "LastName5"));
    mutations.add(insertRecordMutation(6, "FirstName6", "LastName6"));
    Timestamp t3 = databaseClient.write(mutations);
    LOG.debug("The third transaction committed with timestamp: " + t3.toString());
    mutations.clear();

    // 4. Commit a transaction to insert Singer 7 and update Singer 6 in the table.
    mutations.add(insertRecordMutation(7, "FirstName7", "LastName7"));
    mutations.add(updateRecordMutation(6, "FirstName5", "LastName5"));
    Timestamp t4 = databaseClient.write(mutations);
    LOG.debug("The fourth transaction committed with timestamp: " + t4.toString());
    mutations.clear();

    // 5. Commit a transaction to update Singer 4 and Singer 5 in the table.
    mutations.add(updateRecordMutation(4, "FirstName9", "LastName9"));
    mutations.add(updateRecordMutation(5, "FirstName9", "LastName9"));
    Timestamp t5 = databaseClient.write(mutations);
    LOG.debug("The fifth transaction committed with timestamp: " + t5.toString());
    mutations.clear();

    // 6. Commit a transaction to delete Singers 3, 4, 5.
    mutations.add(deleteRecordMutation(3));
    mutations.add(deleteRecordMutation(4));
    mutations.add(deleteRecordMutation(5));
    Timestamp t6 = databaseClient.write(mutations);
    mutations.clear();
    LOG.debug("The sixth transaction committed with timestamp: " + t6.toString());

    // 7. Commit a transaction to delete Singers 0, 2, 6, 7.
    mutations.add(deleteRecordMutation(0));
    mutations.add(deleteRecordMutation(2));
    mutations.add(deleteRecordMutation(6));
    mutations.add(deleteRecordMutation(7));
    Timestamp t7 = databaseClient.write(mutations);
    LOG.debug("The seventh transaction committed with timestamp: " + t7.toString());

    return t7;
  }

  // Create an update mutation.
  private static Mutation updateRecordMutation(long singerId, String firstName, String lastName) {
    return Mutation.newUpdateBuilder(tableName)
        .set("SingerId")
        .to(singerId)
        .set("FirstName")
        .to(firstName)
        .set("LastName")
        .to(lastName)
        .build();
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
}
