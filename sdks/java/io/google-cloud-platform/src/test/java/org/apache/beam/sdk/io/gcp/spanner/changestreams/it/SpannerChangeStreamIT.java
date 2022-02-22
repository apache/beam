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
import com.google.cloud.spanner.Mutation;
import com.google.gson.Gson;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamIT {

  @ClassRule public static final IntegrationTestEnv ENV = new IntegrationTestEnv();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static String instanceId;
  private static String projectId;
  private static String databaseId;
  private static String tableName;
  private static String changeStreamName;
  private static DatabaseClient databaseClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    projectId = ENV.getProjectId();
    instanceId = ENV.getInstanceId();
    databaseId = ENV.getDatabaseId();
    tableName = ENV.createSingersTable();
    changeStreamName = ENV.createChangeStreamFor(tableName);
    databaseClient = ENV.getDatabaseClient();
  }

  @Before
  public void before() {
    pipeline.getOptions().as(ChangeStreamTestPipelineOptions.class).setStreaming(true);
    pipeline.getOptions().as(ChangeStreamTestPipelineOptions.class).setBlockOnRun(false);
  }

  @Test
  public void testReadSpannerChangeStream() {
    // Defines how many rows are going to be inserted / updated / deleted in the test
    final int numRows = 5;
    // Inserts numRows rows and uses the first commit timestamp as the startAt for reading the
    // change stream
    final Pair<Timestamp, Timestamp> insertTimestamps = insertRows(numRows);
    final Timestamp startAt = insertTimestamps.getLeft();
    // Updates the created rows
    updateRows(numRows);
    // Delete the created rows and uses the last commit timestamp as the endAt for reading the
    // change stream
    final Pair<Timestamp, Timestamp> deleteTimestamps = deleteRows(numRows);
    final Timestamp endAt = deleteTimestamps.getRight();

    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withInclusiveStartAt(startAt)
                    .withInclusiveEndAt(endAt))
            .apply(ParDo.of(new ModsToString()));

    // Each row is composed by the following data
    // <mod type, singer id, old first name, old last name, new first name, new last name>
    PAssert.that(tokens)
        .containsInAnyOrder(
            "INSERT,1,null,null,First Name 1,Last Name 1",
            "INSERT,2,null,null,First Name 2,Last Name 2",
            "INSERT,3,null,null,First Name 3,Last Name 3",
            "INSERT,4,null,null,First Name 4,Last Name 4",
            "INSERT,5,null,null,First Name 5,Last Name 5",
            "UPDATE,1,First Name 1,Last Name 1,Updated First Name 1,Updated Last Name 1",
            "UPDATE,2,First Name 2,Last Name 2,Updated First Name 2,Updated Last Name 2",
            "UPDATE,3,First Name 3,Last Name 3,Updated First Name 3,Updated Last Name 3",
            "UPDATE,4,First Name 4,Last Name 4,Updated First Name 4,Updated Last Name 4",
            "UPDATE,5,First Name 5,Last Name 5,Updated First Name 5,Updated Last Name 5",
            "DELETE,1,Updated First Name 1,Updated Last Name 1,null,null",
            "DELETE,2,Updated First Name 2,Updated Last Name 2,null,null",
            "DELETE,3,Updated First Name 3,Updated Last Name 3,null,null",
            "DELETE,4,Updated First Name 4,Updated Last Name 4,null,null",
            "DELETE,5,Updated First Name 5,Updated Last Name 5,null,null");
    pipeline.run().waitUntilFinish();
  }

  private static Pair<Timestamp, Timestamp> insertRows(int n) {
    final Timestamp firstCommitTimestamp = insertRow(1);
    for (int i = 2; i < n; i++) {
      insertRow(i);
    }
    final Timestamp lastCommitTimestamp = insertRow(n);
    return Pair.of(firstCommitTimestamp, lastCommitTimestamp);
  }

  private static Pair<Timestamp, Timestamp> updateRows(int n) {
    final Timestamp firstCommitTimestamp = updateRow(1);
    for (int i = 2; i < n; i++) {
      updateRow(i);
    }
    final Timestamp lastCommitTimestamp = updateRow(n);
    return Pair.of(firstCommitTimestamp, lastCommitTimestamp);
  }

  private static Pair<Timestamp, Timestamp> deleteRows(int n) {
    final Timestamp firstCommitTimestamp = deleteRow(1);
    for (int i = 2; i < n; i++) {
      deleteRow(i);
    }
    final Timestamp lastCommitTimestamp = deleteRow(n);
    return Pair.of(firstCommitTimestamp, lastCommitTimestamp);
  }

  private static Timestamp insertRow(int singerId) {
    return databaseClient.write(
        Collections.singletonList(
            Mutation.newInsertBuilder(tableName)
                .set("SingerId")
                .to(singerId)
                .set("FirstName")
                .to("First Name " + singerId)
                .set("LastName")
                .to("Last Name " + singerId)
                .build()));
  }

  private static Timestamp updateRow(int singerId) {
    return databaseClient.write(
        Collections.singletonList(
            Mutation.newUpdateBuilder(tableName)
                .set("SingerId")
                .to(singerId)
                .set("FirstName")
                .to("Updated First Name " + singerId)
                .set("LastName")
                .to("Updated Last Name " + singerId)
                .build()));
  }

  private static Timestamp deleteRow(int singerId) {
    return databaseClient.write(
        Collections.singletonList(Mutation.delete(tableName, Key.of(singerId))));
  }

  private static class ModsToString extends DoFn<DataChangeRecord, String> {
    private transient Gson gson;

    @Setup
    public void setup() {
      gson = new Gson();
    }

    @ProcessElement
    public void processElement(
        @Element DataChangeRecord record, OutputReceiver<String> outputReceiver) {
      final Mod mod = record.getMods().get(0);
      final Map<String, String> keys = gson.fromJson(mod.getKeysJson(), Map.class);
      final Map<String, String> oldValues =
          Optional.ofNullable(mod.getOldValuesJson())
              .map(nonNullValues -> gson.fromJson(nonNullValues, Map.class))
              .orElseGet(Collections::emptyMap);
      final Map<String, String> newValues =
          Optional.ofNullable(mod.getNewValuesJson())
              .map(nonNullValues -> gson.fromJson(nonNullValues, Map.class))
              .orElseGet(Collections::emptyMap);

      final String modsAsString =
          String.join(
              ",",
              record.getModType().toString(),
              keys.get("SingerId"),
              oldValues.get("FirstName"),
              oldValues.get("LastName"),
              newValues.get("FirstName"),
              newValues.get("LastName"));
      final Instant timestamp = new Instant(record.getRecordTimestamp().toSqlTimestamp());

      outputReceiver.outputWithTimestamp(modsAsString, timestamp);
    }
  }
}
