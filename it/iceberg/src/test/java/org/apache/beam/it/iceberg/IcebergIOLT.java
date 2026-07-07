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
package org.apache.beam.it.iceberg;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.dataflow.DefaultPipelineLauncher.PipelineMetricsType;
import org.apache.beam.it.common.dataflow.IOLoadTestBase;
import org.apache.beam.it.common.storage.GcsResourceManager;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * IcebergIO performance tests using a Hadoop catalog backed by GCS.
 *
 * <p>The test writes generated rows to an Iceberg table, reads the table back, validates the number
 * of rows, and exports write and read performance metrics.
 *
 * <p>Example trigger command:
 *
 * <pre>
 * ./gradlew :it:iceberg:integrationTest \
 *   --tests "org.apache.beam.it.iceberg.IcebergIOLT" \
 *   -Dconfiguration=local \
 *   -Dproject=[gcpProject] \
 *   -DartifactBucket=[artifactBucket]
 * </pre>
 */
public final class IcebergIOLT extends IOLoadTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static final String NAMESPACE = "default";

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt64Field("id").addByteArrayField("data").build();

  private static final Map<String, Configuration> TEST_CONFIGS =
      ImmutableMap.of(
          "local",
          Configuration.of(1_000L, 5, "DirectRunner", 1_000), // approximately 1 MB
          "small",
          Configuration.of(1_000_000L, 20, "DataflowRunner", 1_000), // approximately 1 GB
          "medium",
          Configuration.of(10_000_000L, 40, "DataflowRunner", 1_000), // approximately 10 GB
          "large",
          Configuration.of(100_000_000L, 100, "DataflowRunner", 1_000) // approximately 100 GB
          );

  private static GcsResourceManager resourceManager;

  private Configuration configuration;
  private IcebergCatalogConfig catalogConfig;
  private TableIdentifier tableIdentifier;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        GcsResourceManager.builder(TestProperties.artifactBucket(), "icebergiolt", CREDENTIALS)
            .build();
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  @Before
  public void setup() {
    configuration = getTestConfiguration();

    String uniqueSuffix =
        DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneOffset.UTC)
                .format(java.time.Instant.now())
            + "-"
            + UUID.randomUUID().toString().substring(0, 10);

    String warehouseDirectory = "icebergiolt-" + uniqueSuffix;
    resourceManager.registerTempDir(warehouseDirectory);

    String warehouseLocation =
        String.format("gs://%s/%s", TestProperties.artifactBucket(), warehouseDirectory);

    tableIdentifier = TableIdentifier.of(NAMESPACE, "table_" + uniqueSuffix.replace("-", "_"));

    Map<String, String> catalogProperties =
        ImmutableMap.of("type", "hadoop", "warehouse", warehouseLocation);

    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("iceberg-load-test")
            .setCatalogProperties(catalogProperties)
            .build();

    setTempLocation(writePipeline);
    setTempLocation(readPipeline);
  }

  /** Writes generated rows to Iceberg and verifies that all rows can be read back. */
  @Test
  public void testIcebergWriteAndRead() throws IOException {
    PipelineLauncher.LaunchInfo writeInfo = testWrite();

    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.getPipelineTimeout())));

    assertThatResult(writeResult).isLaunchFinished();
    assertEquals(
        PipelineLauncher.JobState.DONE,
        pipelineLauncher.getJobStatus(project, region, writeInfo.jobId()));

    PipelineLauncher.LaunchInfo readInfo = testRead();

    PipelineOperator.Result readResult =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.getPipelineTimeout())));

    assertThatResult(readResult).isLaunchFinished();
    assertEquals(
        PipelineLauncher.JobState.DONE,
        pipelineLauncher.getJobStatus(project, region, readInfo.jobId()));

    double numRecords =
        pipelineLauncher.getMetric(
            project,
            region,
            readInfo.jobId(),
            getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));

    assertEquals((double) configuration.getNumRows(), numRecords, 0.5);

    exportMetrics(writeInfo, readInfo);
  }

  private PipelineLauncher.LaunchInfo testWrite() throws IOException {
    writePipeline
        .apply("Generate records", GenerateSequence.from(0).to(configuration.getNumRows()))
        .apply("Map records", ParDo.of(new MapToIcebergFormat(configuration.getValueSizeBytes())))
        .setRowSchema(ROW_SCHEMA)
        .apply("Write to Iceberg", IcebergIO.writeRows(catalogConfig).to(tableIdentifier));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-iceberg")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.getRunner())
            .addParameter(
                "maxNumWorkers",
                TestProperties.getProperty("maxNumWorkers", "10", TestProperties.Type.PROPERTY))
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  private PipelineLauncher.LaunchInfo testRead() throws IOException {
    readPipeline
        .apply("Read from Iceberg", IcebergIO.readRows(catalogConfig).from(tableIdentifier))
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-iceberg")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.getRunner())
            .addParameter(
                "maxNumWorkers",
                TestProperties.getProperty("maxNumWorkers", "10", TestProperties.Type.PROPERTY))
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  private void exportMetrics(
      PipelineLauncher.LaunchInfo writeInfo, PipelineLauncher.LaunchInfo readInfo) {

    /*
     * Only use PCollection names belonging to transforms defined directly in this test.
     * Internal IcebergIO transform names may change when the implementation changes.
     */
    MetricsConfiguration writeMetricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Map records.out0")
            .setInputPCollectionV2("Map records/ParMultiDo(MapToIcebergFormat).out0")
            .build();

    MetricsConfiguration readMetricsConfig =
        MetricsConfiguration.builder()
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();

    try {
      exportMetricsToBigQuery(writeInfo, getMetrics(writeInfo, writeMetricsConfig));

      exportMetricsToBigQuery(readInfo, getMetrics(readInfo, readMetricsConfig));
    } catch (IOException | ParseException | InterruptedException e) {
      throw new RuntimeException("Unable to collect or export IcebergIO load-test metrics.", e);
    }
  }

  private Configuration getTestConfiguration() {
    String testConfig =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);

    Configuration selectedConfiguration = TEST_CONFIGS.get(testConfig);

    if (selectedConfiguration == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown test configuration: [%s]. Known configurations: %s",
              testConfig, TEST_CONFIGS.keySet()));
    }

    return selectedConfiguration;
  }

  private static void setTempLocation(TestPipeline pipeline) {
    String tempBucketName = TestProperties.artifactBucket();

    if (Strings.isNullOrEmpty(tempBucketName)) {
      return;
    }

    String tempLocation = String.format("gs://%s/temp/", tempBucketName);

    pipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);

    pipeline.getOptions().setTempLocation(tempLocation);
  }

  /** Maps a generated sequence value to a deterministic Iceberg-compatible Beam row. */
  private static class MapToIcebergFormat extends DoFn<Long, Row> {

    private final int valueSizeBytes;

    private MapToIcebergFormat(int valueSizeBytes) {
      if (valueSizeBytes <= 0) {
        throw new IllegalArgumentException("valueSizeBytes must be positive.");
      }

      this.valueSizeBytes = valueSizeBytes;
    }

    @ProcessElement
    public void processElement(@Element Long index, OutputReceiver<Row> output) {

      byte[] value = new byte[valueSizeBytes];

      /*
       * Using the row index as the seed makes retries deterministic. Random data also prevents
       * compression from making the generated payload significantly smaller than configured.
       */
      new Random(index).nextBytes(value);

      output.output(Row.withSchema(ROW_SCHEMA).addValues(index, value).build());
    }
  }

  /** Options for the IcebergIO load test. */
  @AutoValue
  abstract static class Configuration {

    abstract long getNumRows();

    abstract int getPipelineTimeout();

    abstract String getRunner();

    abstract int getValueSizeBytes();

    static Configuration of(long numRows, int pipelineTimeout, String runner, int valueSizeBytes) {

      if (numRows <= 0) {
        throw new IllegalArgumentException("numRows must be positive.");
      }

      if (pipelineTimeout <= 0) {
        throw new IllegalArgumentException("pipelineTimeout must be positive.");
      }

      if (valueSizeBytes <= 0) {
        throw new IllegalArgumentException("valueSizeBytes must be positive.");
      }

      return new AutoValue_IcebergIOLT_Configuration.Builder()
          .setNumRows(numRows)
          .setPipelineTimeout(pipelineTimeout)
          .setRunner(runner)
          .setValueSizeBytes(valueSizeBytes)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setNumRows(long numRows);

      abstract Builder setPipelineTimeout(int pipelineTimeout);

      abstract Builder setRunner(String runner);

      abstract Builder setValueSizeBytes(int valueSizeBytes);

      abstract Configuration build();
    }
  }
}
