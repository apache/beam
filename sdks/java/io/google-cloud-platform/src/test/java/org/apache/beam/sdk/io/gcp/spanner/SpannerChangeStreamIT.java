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

import static org.junit.Assert.assertEquals;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end test of Cloud Spanner Source. */
@RunWith(JUnit4.class)
public class SpannerChangeStreamIT {

  private static final String NAMESPACE = SpannerChangeStreamIT.class.getName();

  private static final String READ_ELEMENT_METRIC_NAME = "spanner_change_stream_element_count";

  private static final int MAX_DB_NAME_LENGTH = 30;

  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Pipeline options for this test. */
  public interface SpannerTestPipelineOptions extends IOTestPipelineOptions, StreamingOptions {
    @Description("Project that hosts Spanner instance")
    @Nullable
    String getProjectId();

    void setProjectId(String value);

    @Description("Instance ID to write to in Spanner")
    @Default.String("beam-test")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Database ID prefix to write to in Spanner")
    @Default.String("beam-testdb")
    String getDatabaseIdPrefix();

    void setDatabaseIdPrefix(String value);

    @Description("Change stream name")
    @Default.String("users-change-stream")
    String getChangeStream();

    void setChangeStream(String value);

    @Description("Time to wait for the events to be processed by the read pipeline (in seconds)")
    @Default.Integer(300)
    @Validation.Required
    Integer getReadTimeout();

    void setReadTimeout(Integer readTimeout);
  }

  private static Spanner spanner;
  private static DatabaseAdminClient databaseAdminClient;
  private static SpannerTestPipelineOptions options;
  private static String databaseName;
  private static String project;

  @BeforeClass
  public static void setup() throws InterruptedException, ExecutionException {
    options = IOITHelper.readIOTestPipelineOptions(SpannerTestPipelineOptions.class);

    project = options.getProjectId();
    if (project == null) {
      project = options.as(GcpOptions.class).getProject();
    }

    spanner = SpannerOptions.newBuilder().setProjectId(project).build().getService();

    databaseName = generateDatabaseName();

    databaseAdminClient = spanner.getDatabaseAdminClient();

    // Delete database if exists.
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);

    OperationFuture<Database, CreateDatabaseMetadata> op =
        databaseAdminClient.createDatabase(
            options.getInstanceId(), databaseName, Collections.emptyList());
    op.get();
  }

  private static class MapRecordsToStrings extends SimpleFunction<PartitionMetadata, String> {
    @Override
    public String apply(PartitionMetadata input) {
      return String.format("%s", input.getPartitionToken());
    }
  }

  private static class CountingFn extends DoFn<String, Void> {

    private final Counter elementCounter;

    CountingFn(String namespace, String name) {
      elementCounter = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement() {
      elementCounter.inc(1L);
    }
  }

  private long readElementMetric(PipelineResult result, String namespace, String name) {
    MetricsReader metricsReader = new MetricsReader(result, namespace);
    return metricsReader.getCounterMetric(name);
  }

  private void cancelIfTimeouted(PipelineResult readResult, PipelineResult.State readState)
      throws IOException {
    if (readState == null) {
      readResult.cancel();
    }
  }

  @Test
  public void testQuery() throws Exception {
    SpannerConfig spannerConfig = createSpannerConfig();

    String metadataTable = "test_metadata_table";

    Timestamp now = Timestamp.now();

    p.getOptions().as(SpannerTestPipelineOptions.class).setStreaming(true);
    p.getOptions().as(SpannerTestPipelineOptions.class).setBlockOnRun(false);

    p.apply(
            SpannerIO.readChangeStream()
                .withSpannerConfig(spannerConfig)
                .withChangeStreamName(options.getChangeStream())
                .withInclusiveStartAt(now)
                .withTestMetadataTable(metadataTable))
        .apply("Map records to strings", MapElements.via(new MapRecordsToStrings()))
        .apply("Counting element", ParDo.of(new CountingFn(NAMESPACE, READ_ELEMENT_METRIC_NAME)));

    PipelineResult readResult = p.run();
    PipelineResult.State readState =
        readResult.waitUntilFinish(Duration.standardSeconds(options.getReadTimeout()));

    // Sleep some time to wait for the pipeline to start.
    Thread.sleep(5000);

    makeTestData(metadataTable);

    cancelIfTimeouted(readResult, readState);

    assertEquals(4L, readElementMetric(readResult, NAMESPACE, READ_ELEMENT_METRIC_NAME));
  }

  private void makeTestData(String table) {
    DatabaseClient databaseClient = getDatabaseClient();

    PartitionMetadataDao dao = new PartitionMetadataDao(databaseClient, table);

    PartitionMetadata.Builder builder = PartitionMetadata.newBuilder();
    builder
        .setStartTimestamp(Timestamp.now())
        .setInclusiveStart(true)
        .setHeartbeatSeconds(60L)
        .setState(PartitionMetadata.State.CREATED);

    dao.insert(builder.setPartitionToken("1").setParentTokens(Arrays.asList("2")).build());
    dao.insert(builder.setPartitionToken("2").setParentTokens(Arrays.asList("3")).build());
    dao.insert(builder.setPartitionToken("5").setParentTokens(Arrays.asList("6")).build());
  }

  private SpannerConfig createSpannerConfig() {
    return SpannerConfig.create()
        .withProjectId(project)
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(databaseName);
  }

  private DatabaseClient getDatabaseClient() {
    return spanner.getDatabaseClient(DatabaseId.of(project, options.getInstanceId(), databaseName));
  }

  @AfterClass
  public static void afterClass() {
    databaseAdminClient.dropDatabase(options.getInstanceId(), databaseName);
    spanner.close();
  }

  private static String generateDatabaseName() {
    String random =
        RandomUtils.randomAlphaNumeric(
            MAX_DB_NAME_LENGTH - 1 - options.getDatabaseIdPrefix().length());
    return options.getDatabaseIdPrefix() + "-" + random;
  }
}
