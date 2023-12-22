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
package org.apache.beam.it.gcp.spanner;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * SpannerIO performance tests.
 *
 * <p>Example trigger command for all tests: "mvn test -pl it/google-cloud-platform -am
 * -Dtest=SpannerIOLT \ -Dproject=[gcpProject] -DartifactBucket=[temp bucket]
 * -DfailIfNoTests=false".
 *
 * <p>Example trigger command for specific test: "mvn test -pl it/google-cloud-platform -am \
 * -Dtest="SpannerIOLT#testSpannerWriteAndRead" -Dconfiguration=local -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false".
 */
public class SpannerIOLT extends IOLoadTestBase {
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  private String tableName;
  private SpannerResourceManager resourceManager;
  private Configuration configuration;
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";

  @Before
  public void setup() throws IOException {
    // generate a random table name
    tableName =
        "io_spanner_"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().replace("-", "").substring(0, 10);

    resourceManager = SpannerResourceManager.builder(testName, project, region).build();

    // parse configuration
    String testConfig =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS_PRESET.get(testConfig);
    if (configuration == null) {
      try {
        configuration = Configuration.fromJsonString(testConfig, Configuration.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown test configuration: [%s]. Pass to a valid configuration json, or use"
                    + " config presets: %s",
                testConfig, TEST_CONFIGS_PRESET.keySet()));
      }
    }
    // prepare schema
    String createTable =
        createTableStatement(
            tableName, configuration.numColumns, (int) configuration.valueSizeBytes);
    // Create table
    resourceManager.executeDdlStatement(createTable);
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  private static final Map<String, Configuration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "local",
              Configuration.fromJsonString(
                  "{\"numRecords\":1000,\"valueSizeBytes\":1000,\"pipelineTimeout\":2,\"runner\":\"DirectRunner\"}",
                  Configuration.class), // 1 MB
              "medium",
              Configuration.fromJsonString(
                  "{\"numRecords\":10000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":20,\"runner\":\"DataflowRunner\"}",
                  Configuration.class), // 10 GB
              "large",
              Configuration.fromJsonString(
                  "{\"numRecords\":100000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":80,\"runner\":\"DataflowRunner\"}",
                  Configuration.class) // 100 GB
              );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSpannerWriteAndRead() throws IOException {
    PipelineLauncher.LaunchInfo writeInfo = testWrite();
    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, writeResult);

    PipelineLauncher.LaunchInfo readInfo = testRead();
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, result);
    assertEquals(
        PipelineLauncher.JobState.DONE,
        pipelineLauncher.getJobStatus(project, region, readInfo.jobId()));
    double numRecords =
        pipelineLauncher.getMetric(
            project,
            region,
            readInfo.jobId(),
            getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));
    assertEquals(configuration.numRecords, numRecords, 0.5);

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Map records.out0")
            .setInputPCollectionV2("Map records/ParMultiDo(GenerateMutations).out0")
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();
    try {
      exportMetricsToBigQuery(writeInfo, getMetrics(writeInfo, metricsConfig));
      exportMetricsToBigQuery(readInfo, getMetrics(readInfo, metricsConfig));
    } catch (ParseException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private PipelineLauncher.LaunchInfo testWrite() throws IOException {
    SpannerIO.Write writeTransform =
        SpannerIO.write()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withDatabaseId(resourceManager.getDatabaseId());

    writePipeline
        .apply(GenerateSequence.from(0).to(configuration.numRecords))
        .apply(
            "Map records",
            ParDo.of(
                new GenerateMutations(
                    tableName, configuration.numColumns, (int) configuration.valueSizeBytes)))
        .apply("Write to Spanner", writeTransform);

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-spanner")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  private PipelineLauncher.LaunchInfo testRead() throws IOException {
    SpannerIO.Read readTrabsfirn =
        SpannerIO.read()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withDatabaseId(resourceManager.getDatabaseId())
            .withQuery(String.format("SELECT * FROM %s", tableName));

    readPipeline
        .apply("Read from Spanner", readTrabsfirn)
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-spanner")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.runner)
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for SpannerIO load test. */
  static class Configuration extends SyntheticSourceOptions {

    /**
     * Number of columns (besides the primary key) of each record. The column size is equally
     * distributed as valueSizeBytes/numColumns.
     */
    @JsonProperty public int numColumns = 1;

    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";
  }

  /**
   * Generate a create table sql statement with 1 integer column (Id) and additional numBytesCol
   * columns.
   */
  static String createTableStatement(String tableId, int numBytesCol, int valueSizeBytes) {
    int sizePerCol = valueSizeBytes / numBytesCol;
    StringBuilder statement = new StringBuilder();
    statement.append(String.format("CREATE TABLE %s (Id INT64", tableId));
    for (int col = 0; col < numBytesCol; ++col) {
      statement.append(String.format(",\n COL%d BYTES(%d)", col + 1, sizePerCol));
    }
    statement.append(") PRIMARY KEY(Id)");
    return statement.toString();
  }

  /** Maps long number to the Spanner format record. */
  private static class GenerateMutations extends DoFn<Long, Mutation> implements Serializable {
    private final String table;
    private final int numBytesCol;
    private final int sizePerCol;

    public GenerateMutations(String tableId, int numBytesCol, int valueSizeBytes) {
      checkArgument(valueSizeBytes >= numBytesCol);
      this.table = tableId;
      this.numBytesCol = numBytesCol;
      this.sizePerCol = valueSizeBytes / numBytesCol;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table);
      Long key = Objects.requireNonNull(c.element());
      builder.set("Id").to(key);
      Random random = new Random(key);
      byte[] value = new byte[sizePerCol];
      for (int col = 0; col < numBytesCol; ++col) {
        String name = String.format("COL%d", col + 1);
        random.nextBytes(value);
        builder.set(name).to(ByteArray.copyFrom(value));
      }
      Mutation mutation = builder.build();
      c.output(mutation);
    }
  }
}
