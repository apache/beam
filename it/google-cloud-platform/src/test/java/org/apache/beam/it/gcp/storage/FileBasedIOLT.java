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
package org.apache.beam.it.gcp.storage;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * FileBasedIO performance tests.
 *
 * <p>Example trigger command for all tests:
 *
 * <pre>
 * mvn test -pl it/google-cloud-platform -am -Dtest="FileBasedIOLT" -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 *
 * <p>Example trigger command for specific test running on direct runner:
 *
 * <pre>
 * mvn test -pl it/google-cloud-platform -am -Dtest="FileBasedIOLT#testTextIOWriteThenRead" \
 * -Dconfiguration=medium -Dproject=[gcpProject] -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 *
 * <p>Example trigger command for specific test and custom data configuration:
 *
 * <pre>mvn test -pl it/google-cloud-platform -am \
 * -Dconfiguration="{\"numRecords\":10000000,\"valueSizeBytes\":750,\"pipelineTimeout\":20,\"runner\":\"DataflowRunner\"}" \
 * -Dtest="FileBasedIOLT#testTextIOWriteThenRead" -Dconfiguration=local -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 */
public class FileBasedIOLT extends IOLoadTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";

  private static GcsResourceManager resourceManager;

  private String filePrefix;

  private Configuration configuration;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final Map<String, FileBasedIOLT.Configuration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "local",
              Configuration.fromJsonString(
                  "{\"numRecords\":1000,\"valueSizeBytes\":750,\"pipelineTimeout\":2,\"runner\":\"DirectRunner\"}",
                  Configuration
                      .class), // 1 MB - bytes get encoded in Base64 so 740 bytes gives 1 kB line
              // size
              "medium",
              Configuration.fromJsonString(
                  "{\"numRecords\":10000000,\"valueSizeBytes\":750,\"pipelineTimeout\":20,\"runner\":\"DataflowRunner\"}",
                  Configuration.class), // 10 GB
              "large",
              Configuration.fromJsonString(
                  "{\"numRecords\":100000000,\"valueSizeBytes\":750,\"pipelineTimeout\":80,\"runner\":\"DataflowRunner\"}",
                  Configuration.class) // 100 GB
              );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        GcsResourceManager.builder(TestProperties.artifactBucket(), "textiolt", CREDENTIALS)
            .build();
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  @Before
  public void setup() {

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

    // filePath needs to be set for TextIO writes
    String tempDirName =
        "textiolt-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    resourceManager.registerTempDir(tempDirName);
    filePrefix = String.format("gs://%s/%s/test", TestProperties.artifactBucket(), tempDirName);
  }

  @Test
  public void testTextIOWriteThenRead() throws IOException {

    TextIO.TypedWrite<String, Object> write =
        TextIO.write()
            .to(filePrefix)
            .withOutputFilenames()
            .withCompression(Compression.valueOf(configuration.compressionType));
    if (configuration.numShards > 0) {
      write = write.withNumShards(configuration.numShards);
    }

    writePipeline
        .apply("Read from source", Read.from(new SyntheticBoundedSource(configuration)))
        .apply("Map records", ParDo.of(new MapKVToString()))
        .apply("Write content to files", write);

    readPipeline
        .apply(TextIO.read().from(filePrefix + "*"))
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig writeOptions =
        PipelineLauncher.LaunchConfig.builder("write-textio")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .build();
    PipelineLauncher.LaunchInfo writeInfo = pipelineLauncher.launch(project, region, writeOptions);
    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if pipeline failed or timeout.
    assertThatResult(writeResult).isLaunchFinished();

    PipelineLauncher.LaunchConfig readOptions =
        PipelineLauncher.LaunchConfig.builder("read-textio")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.runner)
            .build();
    PipelineLauncher.LaunchInfo readInfo = pipelineLauncher.launch(project, region, readOptions);
    PipelineOperator.Result readResult =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if pipeline failed or timeout.
    assertThatResult(readResult).isLaunchFinished();

    // check metrics
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
            .setInputPCollectionV2("Map records/ParMultiDo(MapKVToString).out0")
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

  private static class MapKVToString extends DoFn<KV<byte[], byte[]>, String> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(
          Base64.getEncoder().encodeToString(Objects.requireNonNull(context.element()).getValue()));
    }
  }

  static class Configuration extends SyntheticSourceOptions {
    /** Number of dynamic destinations to write. */
    @JsonProperty public int numShards = 0;

    /** See {@class org.apache.beam.sdk.io.Compression}. */
    @JsonProperty public String compressionType = "UNCOMPRESSED";

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;
  }
}
