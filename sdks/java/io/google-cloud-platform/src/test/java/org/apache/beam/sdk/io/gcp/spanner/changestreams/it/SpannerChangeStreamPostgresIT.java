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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.Statement;
import com.google.gson.Gson;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.options.ValueProvider;
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

/** End-to-end test of Cloud Spanner CDC Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamPostgresIT {

  @ClassRule
  public static final IntegrationTestEnv ENV = new IntegrationTestEnv(/*isPostgres=*/ true);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static String instanceId;
  private static String projectId;
  private static String databaseId;
  private static String metadataTableName;
  private static String changeStreamTableName;
  private static String changeStreamName;
  private static DatabaseClient databaseClient;
  private static String host = "https://spanner.googleapis.com";

  @BeforeClass
  public static void beforeClass() throws Exception {
    projectId = ENV.getProjectId();
    instanceId = ENV.getInstanceId();
    databaseId = ENV.getDatabaseId();

    metadataTableName = ENV.getMetadataTableName();
    changeStreamTableName = ENV.createSingersTable();
    changeStreamName = ENV.createChangeStreamFor(changeStreamTableName);
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
            .withDatabaseId(databaseId)
            .withHost(ValueProvider.StaticValueProvider.of(host));

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withMetadataTable(metadataTableName)
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

    assertMetadataTableHasBeenDropped();
  }

  private static void assertMetadataTableHasBeenDropped() {
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of("SELECT * FROM \"" + metadataTableName + "\""))) {
      resultSet.next();
      fail(
          "The metadata table "
              + metadataTableName
              + " should had been dropped, but it still exists");
    } catch (SpannerException e) {
      assertEquals(ErrorCode.INVALID_ARGUMENT, e.getErrorCode());
      assertTrue(
          "Error message must contain \"Table not found\"",
          e.getMessage().contains("relation \"" + metadataTableName + "\" does not exist"));
    }
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
    return databaseClient
        .writeWithOptions(
            Collections.singletonList(
                Mutation.newInsertBuilder(changeStreamTableName)
                    .set("SingerId")
                    .to(singerId)
                    .set("FirstName")
                    .to("First Name " + singerId)
                    .set("LastName")
                    .to("Last Name " + singerId)
                    .build()))
        .getCommitTimestamp();
  }

  private static Timestamp updateRow(int singerId) {
    return databaseClient
        .writeWithOptions(
            Collections.singletonList(
                Mutation.newUpdateBuilder(changeStreamTableName)
                    .set("SingerId")
                    .to(singerId)
                    .set("FirstName")
                    .to("Updated First Name " + singerId)
                    .set("LastName")
                    .to("Updated Last Name " + singerId)
                    .build()),
            Options.tag("app=beam;action=update"))
        .getCommitTimestamp();
  }

  private static Timestamp deleteRow(int singerId) {
    return databaseClient
        .writeWithOptions(
            Collections.singletonList(Mutation.delete(changeStreamTableName, Key.of(singerId))),
            Options.tag("app=beam;action=delete"))
        .getCommitTimestamp();
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
