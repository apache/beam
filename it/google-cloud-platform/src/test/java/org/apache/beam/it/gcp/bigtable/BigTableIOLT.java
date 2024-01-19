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
package org.apache.beam.it.gcp.bigtable;

import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateTableId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * BigTableIO performance tests.
 *
 * <p>Example trigger command for all tests: "mvn test -pl it/google-cloud-platform -am
 * -Dtest=BigTableIOLT \ -Dproject=[gcpProject] -DartifactBucket=[temp bucket]
 * -DfailIfNoTests=false".
 *
 * <p>Example trigger command for specific test: "mvn test -pl it/google-cloud-platform -am \
 * -Dtest="BigTableIOLT#testBigtableWriteAndRead" -Dconfiguration=local -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false".
 */
public class BigTableIOLT extends IOLoadTestBase {

  private static final String COLUMN_FAMILY_NAME = "cf";
  private static final long TABLE_MAX_AGE_MINUTES = 100L;

  private BigtableResourceManager resourceManager;
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private Configuration configuration;
  private String tableId;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Before
  public void setup() throws IOException {
    resourceManager =
        BigtableResourceManager.builder(testName, project, CREDENTIALS_PROVIDER).build();

    String testConfig =
        TestProperties.getProperty("configuration", "small", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS.get(testConfig);
    if (configuration == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configs: %s",
              testConfig, TEST_CONFIGS.keySet()));
    }
    // tempLocation needs to be set for bigtable IO writes
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      String tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
  }

  @After
  public void teardown() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  private static final Map<String, Configuration> TEST_CONFIGS =
      ImmutableMap.of(
          "local", Configuration.of(1_000L, 5, "DirectRunner", 1000), // 1MB
          "small", Configuration.of(1_000_000L, 20, "DataflowRunner", 1000), // 1 GB
          "medium", Configuration.of(10_000_000L, 40, "DataflowRunner", 1000), // 10 GB
          "large", Configuration.of(100_000_000L, 100, "DataflowRunner", 1000) // 100 GB
          );

  /** Run integration test with configurations specified by TestProperties. */
  @Test
  public void testBigtableWriteAndRead() throws IOException {

    tableId = generateTableId(testName);
    resourceManager.createTable(
        tableId,
        ImmutableList.of(COLUMN_FAMILY_NAME),
        org.threeten.bp.Duration.ofMinutes(TABLE_MAX_AGE_MINUTES));

    PipelineLauncher.LaunchInfo writeInfo = testWrite();
    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.getPipelineTimeout())));
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, writeResult);

    PipelineLauncher.LaunchInfo readInfo = testRead();
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.getPipelineTimeout())));
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
    assertEquals(configuration.getNumRows(), numRecords, 0.5);

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Map records.out0")
            .setInputPCollectionV2("Map records/ParMultiDo(MapToBigTableFormat).out0")
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

    BigtableIO.Write writeIO =
        BigtableIO.write()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withTableId(tableId);

    writePipeline
        .apply(GenerateSequence.from(0).to(configuration.getNumRows()))
        .apply("Map records", ParDo.of(new MapToBigTableFormat(configuration.getValueSizeBytes())))
        .apply("Write to BigTable", writeIO);

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-bigtable")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.getRunner())
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  private PipelineLauncher.LaunchInfo testRead() throws IOException {
    BigtableIO.Read readIO =
        BigtableIO.read()
            .withoutValidation()
            .withProjectId(project)
            .withInstanceId(resourceManager.getInstanceId())
            .withTableId(tableId);

    readPipeline
        .apply("Read from BigTable", readIO)
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-bigtable")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.getRunner())
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for BigtableIO load test. */
  @AutoValue
  abstract static class Configuration {
    abstract Long getNumRows();

    abstract Integer getPipelineTimeout();

    abstract String getRunner();

    abstract Integer getValueSizeBytes();

    static Configuration of(long numRows, int pipelineTimeout, String runner, int valueSizeBytes) {
      return new AutoValue_BigTableIOLT_Configuration.Builder()
          .setNumRows(numRows)
          .setPipelineTimeout(pipelineTimeout)
          .setRunner(runner)
          .setValueSizeBytes(valueSizeBytes)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setNumRows(long numRows);

      abstract Builder setPipelineTimeout(int timeOutMinutes);

      abstract Builder setRunner(String runner);

      abstract Builder setValueSizeBytes(int valueSizeBytes);

      abstract Configuration build();
    }

    abstract Builder toBuilder();
  }

  /** Maps long number to the BigTable format record. */
  private static class MapToBigTableFormat extends DoFn<Long, KV<ByteString, Iterable<Mutation>>>
      implements Serializable {

    private final int valueSizeBytes;

    public MapToBigTableFormat(int valueSizeBytes) {
      this.valueSizeBytes = valueSizeBytes;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Long index = c.element();

      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", index));
      Random random = new Random(index);
      byte[] valBytes = new byte[this.valueSizeBytes];
      random.nextBytes(valBytes);
      ByteString value = ByteString.copyFrom(valBytes);

      Iterable<Mutation> mutations =
          ImmutableList.of(
              Mutation.newBuilder()
                  .setSetCell(
                      Mutation.SetCell.newBuilder()
                          .setValue(value)
                          .setTimestampMicros(Instant.now().toEpochMilli() * 1000L)
                          .setFamilyName(COLUMN_FAMILY_NAME))
                  .build());
      c.output(KV.of(key, mutations));
    }
  }
}
