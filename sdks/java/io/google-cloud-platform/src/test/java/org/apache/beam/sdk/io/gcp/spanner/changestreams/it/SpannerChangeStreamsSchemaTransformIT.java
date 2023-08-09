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
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.SpannerChangestreamsReadSchemaTransformProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamsSchemaTransformIT {

  @ClassRule public static final IntegrationTestEnv ENV = new IntegrationTestEnv();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static String instanceId;
  private static String projectId;
  private static String databaseId;
  private static String metadataTableName;
  private static String changeStreamTableName;
  private static String changeStreamName;
  private static DatabaseClient databaseClient;

  @BeforeClass
  public static void beforeClass() throws Exception {
    projectId = ENV.getProjectId();
    instanceId = ENV.getInstanceId();
    databaseId = ENV.getDatabaseId();
    metadataTableName = ENV.getMetadataTableName();
    changeStreamTableName = ENV.createSingersTable();
    changeStreamName = ENV.createChangeStreamFor(changeStreamTableName);
    System.out.println(changeStreamName);
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

    final PCollection<Row> tokens =
        PCollectionRowTuple.empty(pipeline)
            .apply(
                new SpannerChangestreamsReadSchemaTransformProvider()
                    .from(
                        SpannerChangestreamsReadSchemaTransformProvider
                            .SpannerChangestreamsReadConfiguration.builder()
                            .setDatabaseId(databaseId)
                            .setInstanceId(instanceId)
                            .setProjectId(projectId)
                            .setTable(changeStreamTableName)
                            .setChangeStreamName(changeStreamName)
                            .setStartAtTimestamp(startAt.toString())
                            .setEndAtTimestamp(endAt.toString())
                            .build()))
            .get("output")
            .apply(
                Window.<Row>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes());

    assertEquals(
        Schema.builder()
            .addStringField("operation")
            .addStringField("commitTimestamp")
            .addInt64Field("recordSequence")
            .addRowField(
                "rowValues",
                Schema.builder()
                    .addNullableField("singerid", Schema.FieldType.INT64)
                    .addNullableField("firstname", Schema.FieldType.STRING)
                    .addNullableField("lastname", Schema.FieldType.STRING)
                    .addNullableField("singerinfo", Schema.FieldType.BYTES)
                    .setOptions(
                        Schema.Options.builder()
                            .addOptions(
                                Schema.Options.builder()
                                    .setOption(
                                        "primaryKeyColumns",
                                        Schema.FieldType.array(Schema.FieldType.STRING),
                                        Collections.singletonList("singerid"))
                                    .build())
                            .build())
                    .build())
            .build(),
        tokens.getSchema());

    PAssert.that(tokens.apply(Select.fieldNames("operation")))
        .containsInAnyOrder(
            operationRow("INSERT"),
            operationRow("INSERT"),
            operationRow("INSERT"),
            operationRow("INSERT"),
            operationRow("INSERT"),
            operationRow("UPDATE"),
            operationRow("UPDATE"),
            operationRow("UPDATE"),
            operationRow("UPDATE"),
            operationRow("UPDATE"),
            operationRow("DELETE"),
            operationRow("DELETE"),
            operationRow("DELETE"),
            operationRow("DELETE"),
            operationRow("DELETE"));
    pipeline.run().waitUntilFinish();

    assertMetadataTableHasBeenDropped();
  }

  private Row operationRow(String operation) {
    return Row.withSchema(Schema.builder().addField("operation", Schema.FieldType.STRING).build())
        .addValue(operation)
        .build();
  }

  private static void assertMetadataTableHasBeenDropped() {
    try (ResultSet resultSet =
        databaseClient
            .singleUse()
            .executeQuery(Statement.of("SELECT * FROM " + metadataTableName))) {
      resultSet.next();
      fail(
          "The metadata table "
              + metadataTableName
              + " should had been dropped, but it still exists");
    } catch (SpannerException e) {
      assertEquals(ErrorCode.INVALID_ARGUMENT, e.getErrorCode());
      assertTrue(
          "Error message must contain \"Table not found\"",
          e.getMessage().contains("Table not found"));
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
                    .build()),
            Options.tag("app=beam;action=insert"))
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
}
