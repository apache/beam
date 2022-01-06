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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamIT {

  private static final int MAX_TABLE_NAME_LENGTH = 128;
  private static final int MAX_CHANGE_STREAM_NAME_LENGTH = 30;
  private static final String TABLE_NAME_PREFIX = "Singers";
  public static final String SPANNER_HOST = "https://staging-wrenchworks.sandbox.googleapis.com";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends IOTestPipelineOptions, StreamingOptions {
    @Description("Project that hosts Spanner instance")
    @Nullable
    String getProjectId();

    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("test-instance")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID prefix to write to in Spanner")
    @Default.String("change-stream-demo")
    String getDatabaseId();

    void setDatabaseId(String value);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Default.Integer(300)
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);
  }

  private static SpannerTestPipelineOptions options;
  private static String projectId;
  private static String instanceId;
  private static String databaseId;
  private static String tableName;
  private static String changeStreamName;
  private static Spanner spanner;
  private static DatabaseAdminClient databaseAdminClient;
  private static DatabaseClient databaseClient;

  @BeforeClass
  public static void setup() throws InterruptedException, ExecutionException, TimeoutException {
    options = IOITHelper.readIOTestPipelineOptions(SpannerTestPipelineOptions.class);
    projectId =
        options.getProjectId() == null
            ? options.as(GcpOptions.class).getProject()
            : options.getProjectId();
    instanceId = options.getInstanceId();
    databaseId = options.getDatabaseId();
    tableName = generateTableName();
    changeStreamName = generateChangeStreamName();
    spanner =
        SpannerOptions.newBuilder()
            .setHost(SPANNER_HOST)
            .setProjectId(projectId)
            .build()
            .getService();
    databaseAdminClient = spanner.getDatabaseAdminClient();
    databaseClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    createTable(instanceId, databaseId, tableName);
    createChangeStream(instanceId, databaseId, changeStreamName, tableName);
  }

  @AfterClass
  public static void afterClass()
      throws InterruptedException, ExecutionException, TimeoutException {
    databaseAdminClient
        .updateDatabaseDdl(
            instanceId,
            databaseId,
            Arrays.asList("DROP CHANGE STREAM " + changeStreamName, "DROP TABLE " + tableName),
            "op" + RandomUtils.randomAlphaNumeric(8))
        .get(5, TimeUnit.MINUTES);
    spanner.close();
  }

  @Test
  public void testReadSpannerChangeStream() throws InterruptedException {
    final Timestamp commitTimestamp = insertRecords();

    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(StaticValueProvider.of(SPANNER_HOST))
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withDatabaseId(databaseId);
    final Timestamp startAt =
        Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds() - 1, commitTimestamp.getNanos());
    final Timestamp endAt =
        Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds() + 1, commitTimestamp.getNanos());

    pipeline.getOptions().as(SpannerTestPipelineOptions.class).setStreaming(true);
    pipeline.getOptions().as(SpannerTestPipelineOptions.class).setBlockOnRun(false);

    final PCollection<String> tokens =
        pipeline
            .apply(
                SpannerIO.readChangeStream()
                    .withSpannerConfig(spannerConfig)
                    .withChangeStreamName(changeStreamName)
                    .withMetadataDatabase(databaseId)
                    .withInclusiveStartAt(startAt)
                    .withInclusiveEndAt(endAt))
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        record -> {
                          final Gson gson = new Gson();
                          final Mod mod = record.getMods().get(0);
                          final Map<String, String> keys =
                              gson.fromJson(mod.getKeysJson(), Map.class);
                          final Map<String, String> newValues =
                              gson.fromJson(mod.getNewValuesJson(), Map.class);
                          return String.join(
                              ",",
                              keys.get("SingerId"),
                              newValues.get("FirstName"),
                              newValues.get("LastName"));
                        }));

    PAssert.that(tokens).containsInAnyOrder("1,First Name 1,Last Name 1");

    final PipelineResult pipelineResult = pipeline.run();
    Thread.sleep(5_000);
    pipelineResult.waitUntilFinish();
  }

  private static Timestamp insertRecords() {
    return databaseClient.write(
        Collections.singletonList(
            Mutation.newInsertBuilder(tableName)
                .set("SingerId")
                .to(1L)
                .set("FirstName")
                .to("First Name 1")
                .set("LastName")
                .to("Last Name 1")
                .build()));
  }

  private static String generateTableName() {
    return TABLE_NAME_PREFIX
        + "_"
        + RandomUtils.randomAlphaNumeric(MAX_TABLE_NAME_LENGTH - 1 - TABLE_NAME_PREFIX.length());
  }

  // TODO: Check if stream name supports dashes
  private static String generateChangeStreamName() {
    return TABLE_NAME_PREFIX
        + "Stream"
        + RandomUtils.randomAlphaNumeric(
            MAX_CHANGE_STREAM_NAME_LENGTH - 1 - (TABLE_NAME_PREFIX + "Stream").length());
  }

  private static void createTable(String instanceId, String databaseId, String tableName)
      throws InterruptedException, ExecutionException, TimeoutException {
    databaseAdminClient
        .updateDatabaseDdl(
            instanceId,
            databaseId,
            Collections.singletonList(
                "CREATE TABLE "
                    + tableName
                    + " ("
                    + "   SingerId   INT64 NOT NULL,"
                    + "   FirstName  STRING(1024),"
                    + "   LastName   STRING(1024),"
                    + "   SingerInfo BYTES(MAX)"
                    + " ) PRIMARY KEY (SingerId)"),
            "op" + RandomUtils.randomAlphaNumeric(8))
        .get(5, TimeUnit.MINUTES);
  }

  private static void createChangeStream(
      String instanceId, String databaseId, String changeStreamName, String tableName)
      throws InterruptedException, ExecutionException, TimeoutException {
    databaseAdminClient
        .updateDatabaseDdl(
            instanceId,
            databaseId,
            Collections.singletonList(
                "CREATE CHANGE STREAM " + changeStreamName + " FOR " + tableName),
            "op" + RandomUtils.randomAlphaNumeric(8))
        .get(5, TimeUnit.MINUTES);
  }
}
