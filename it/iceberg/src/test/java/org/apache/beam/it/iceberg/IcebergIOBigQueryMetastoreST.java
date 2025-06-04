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

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromRealDistribution;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.IOStressTestBase;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IcebergIO stress tests using BigQueryMetastore catalog. The test is designed to assess the
 * performance of IcebergIO under various conditions.
 */
public final class IcebergIOBigQueryMetastoreST extends IOStressTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergIOBigQueryMetastoreST.class);
  private static final String BQMS_CATALOG =
      "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  private static final int NUM_PARTITIONS = 10;
  private static final String WRITE_ELEMENT_METRIC_NAME = "write_count";
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private static InfluxDBSettings influxDBSettings;
  private static final Schema SCHEMA =
      Schema.builder().addInt32Field("num").addByteArrayField("bytes").build();
  private static final long SALT = System.nanoTime();
  private static final BigqueryClient BQ_CLIENT =
      new BigqueryClient("IcebergIOBigQueryMetastoreST");
  private static final String DATASET = "managed_iceberg_bqms_load_tests_" + System.nanoTime();;
  private TestConfiguration configuration;
  private String testConfigName;
  private String warehouse;
  private String tableId;
  private Map<String, Object> managedConfig;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TestName test = new TestName();

  @Before
  public void setup() throws Exception {
    // parse configuration
    testConfigName =
        TestProperties.getProperty("configuration", "medium", TestProperties.Type.PROPERTY);
    configuration = TEST_CONFIGS_PRESET.get(testConfigName);
    if (configuration == null) {
      try {
        configuration = TestConfiguration.fromJsonString(testConfigName, TestConfiguration.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Unknown test configuration: [%s]. Pass to a valid configuration json, or use"
                    + " config presets: %s",
                testConfigName, TEST_CONFIGS_PRESET.keySet()));
      }
    }
    String useDataflowRunnerV2FromProps =
        TestProperties.getProperty("useDataflowRunnerV2", "false", TestProperties.Type.PROPERTY);
    if (!useDataflowRunnerV2FromProps.isEmpty()) {
      configuration.useDataflowRunnerV2 = Boolean.parseBoolean(useDataflowRunnerV2FromProps);
    }
    GcpOptions options = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    // tempLocation needs to be set for DataflowRunner
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      String tempLocation = String.format("gs://%s/temp/", tempBucketName);
      options.setTempLocation(tempLocation);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
    // Use streaming pipeline to write and read records
    writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    readPipeline.getOptions().as(StreamingOptions.class).setStreaming(true);

    writePipeline
        .getOptions()
        .as(DataflowPipelineOptions.class)
        .setNumWorkers(configuration.numWorkers);
    readPipeline
        .getOptions()
        .as(DataflowPipelineOptions.class)
        .setNumWorkers(configuration.numWorkers / 5);

    if (configuration.exportMetricsToInfluxDB) {
      configuration.influxHost =
          TestProperties.getProperty("influxHost", "", TestProperties.Type.PROPERTY);
      configuration.influxDatabase =
          TestProperties.getProperty("influxDatabase", "", TestProperties.Type.PROPERTY);
      configuration.influxMeasurement =
          TestProperties.getProperty("influxMeasurement", "", TestProperties.Type.PROPERTY);
    }
    tableId = String.format("%s.%s_%s", DATASET, test.getMethodName(), testConfigName);
    warehouse =
        String.format("%s%s/%s", options.getTempLocation(), getClass().getSimpleName(), SALT);

    managedConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableId)
            .put(
                "catalog_properties",
                ImmutableMap.<String, String>builder()
                    .put("gcp_project", project)
                    .put("gcp_location", "us-central1")
                    .put("warehouse", warehouse)
                    .put("catalog-impl", BQMS_CATALOG)
                    .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                    .build())
            .build();
    BQ_CLIENT.createNewDataset(project, DATASET);
  }

  @After
  public void cleanup() {
    BQ_CLIENT.deleteDataset(project, DATASET);
  }

  private static final Map<String, TestConfiguration> TEST_CONFIGS_PRESET;

  static {
    try {
      TEST_CONFIGS_PRESET =
          ImmutableMap.of(
              "medium",
              TestConfiguration.fromJsonString(
                  "{\"numRecords\":5000000,\"valueSizeBytes\":1000,\"minutes\":10,\"forceNumInitialBundles\":20,"
                      + "\"pipelineTimeout\":60,\"runner\":\"DataflowRunner\",\"numWorkers\":10}",
                  TestConfiguration.class),
              "large",
              TestConfiguration.fromJsonString(
                  //
                  // "{\"numRecords\":100000000,\"valueSizeBytes\":1000,\"minutes\":7,\"forceNumInitialBundles\":20,"
                  //                      +
                  // "\"pipelineTimeout\":240,\"runner\":\"DataflowRunner\",\"numWorkers\":30}",
                  //
                  "{\"numRecords\":1000000000,\"valueSizeBytes\":1000,\"minutes\":30,\"forceNumInitialBundles\":200,"
                      + "\"pipelineTimeout\":240,\"runner\":\"DataflowRunner\",\"numWorkers\":30}",
                  TestConfiguration.class));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Run stress test with configurations specified by TestProperties. */
  @Test
  public void testWriteAndRead() throws IOException, InterruptedException {
    if (configuration.exportMetricsToInfluxDB) {
      influxDBSettings =
          InfluxDBSettings.builder()
              .withHost(configuration.influxHost)
              .withDatabase(configuration.influxDatabase)
              .withMeasurement(configuration.influxMeasurement + "_" + testConfigName)
              .get();
    }

    Catalog catalog =
        CatalogUtil.loadCatalog(
            BQMS_CATALOG,
            "IcebergIOBigQueryMetastoreST",
            ImmutableMap.<String, String>builder()
                .put("gcp_project", project)
                .put("gcp_location", region)
                .put("warehouse", warehouse)
                .build(),
            new Configuration());
    TableIdentifier identifier = TableIdentifier.parse(tableId);
    catalog.createTable(identifier, IcebergUtils.beamSchemaToIcebergSchema(SCHEMA));

    PipelineLauncher.LaunchInfo readInfo = readData();
    LOG.info("Sleeping for 2 min to allow the read job to start up first...");
    Thread.sleep(2 * 60 * 1000);
    PipelineLauncher.LaunchInfo writeInfo = generateDataAndWrite();

    try {
      PipelineOperator.Result writeResult =
          pipelineOperator.waitUntilDone(
              createConfig(writeInfo, Duration.ofMinutes(configuration.pipelineTimeout)));
      assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, writeResult);

      @Nullable
      Double writeNumRecords =
          pipelineLauncher.getMetric(
              project,
              region,
              writeInfo.jobId(),
              getBeamMetricsName(PipelineMetricsType.COUNTER, WRITE_ELEMENT_METRIC_NAME));
      @Nullable
      Double readNumRecords =
          pipelineLauncher.getMetric(
              project,
              region,
              readInfo.jobId(),
              getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));

      String query = String.format("SELECT COUNT(*) as count FROM `%s.%s`", project, tableId);
      List<TableRow> result = BQ_CLIENT.queryUnflattened(query, project, true, true);
      int writtenCount = Integer.parseInt((String) result.get(0).get("count"));

      // load periods with multipliers should result in a greater number of written records
      assertTrue(configuration.numRecords <= writtenCount);

      if (writeNumRecords != null && readNumRecords != null) {
        double marginOfError = Math.abs((writtenCount - writeNumRecords) / writtenCount) * 100;
        assertTrue(
            String.format(
                "Table query shows that %s records were written, but metrics show %s. Margin of error: %%%s",
                writtenCount, writeNumRecords, marginOfError),
            marginOfError > 0.01);

        marginOfError = Math.abs((writeNumRecords - readNumRecords) / writeNumRecords) * 100;
        assertTrue(
            String.format(
                "Metrics show that %s records we written and %s were read, with a margin of error: %%%s",
                writeNumRecords, readNumRecords, marginOfError),
            marginOfError > 0.01);
      }
    } finally {
      // clean up pipelines
      if (pipelineLauncher.getJobStatus(project, region, writeInfo.jobId())
          == PipelineLauncher.JobState.RUNNING) {
        pipelineLauncher.cancelJob(project, region, writeInfo.jobId());
      }
      if (pipelineLauncher.getJobStatus(project, region, readInfo.jobId())
          == PipelineLauncher.JobState.RUNNING) {
        pipelineLauncher.cancelJob(project, region, readInfo.jobId());
      }
    }

    // export metrics
    //    MetricsConfiguration writeMetricsConfig =
    //        MetricsConfiguration.builder()
    //            .setInputPCollection("Reshuffle fanout/Values/Values/Map.out0")
    //            .setInputPCollectionV2("Reshuffle
    // fanout/Values/Values/Map/ParMultiDo(Anonymous).out0")
    //            .setOutputPCollection("Counting element.out0")
    //            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
    //            .build();
    //
    //    MetricsConfiguration readMetricsConfig =
    //        MetricsConfiguration.builder()
    //            .setOutputPCollection("Counting element.out0")
    //            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
    //            .build();

    if (influxDBSettings == null) {
      return;
    }

    //    exportMetrics(
    //        writeInfo, writeMetricsConfig, configuration.exportMetricsToInfluxDB,
    // influxDBSettings);
    //    exportMetrics(
    //        readInfo, readMetricsConfig, configuration.exportMetricsToInfluxDB, influxDBSettings);
  }

  /**
   * The method creates a pipeline to simulate data generation and write operations to an Iceberg
   * table, based on the specified configuration parameters. The stress test involves varying the
   * load dynamically over time, with options to use configurable parameters.
   */
  private PipelineLauncher.LaunchInfo generateDataAndWrite() throws IOException {
    int totalRowsPerSecond = (int) configuration.numRecords / (configuration.minutes * 60);
    int rowsPerSecondPerSplit = totalRowsPerSecond / configuration.forceNumInitialBundles;
    double delayMillis = 1000 * (1d / rowsPerSecondPerSplit);
    configuration.delayDistribution =
        fromRealDistribution(new ConstantRealDistribution(delayMillis));

    LOG.info(
        "Writing with configuration:\n"
            + "\tnumRows: {}\n"
            + "\tminutes: {}\n"
            + "\tinitialRowsPerSecond: {}\n"
            + "\tbyteSizePerRow: {}\n"
            + "\tnumInitialBundles: {}\n",
        configuration.numRecords,
        configuration.minutes,
        totalRowsPerSecond,
        configuration.valueSizeBytes,
        configuration.forceNumInitialBundles);

    List<LoadPeriod> loadPeriods =
        getLoadPeriods(configuration.minutes, DEFAULT_LOAD_INCREASE_ARRAY);
    System.out.println("loadPeriods: " + loadPeriods);

    PCollection<Row> source =
        writePipeline
            .apply(Read.from(new SyntheticUnboundedSource(configuration)))
            .apply(
                MapElements.into(TypeDescriptors.rows())
                    .via(
                        kv ->
                            Row.withSchema(SCHEMA)
                                .addValues(
                                    ThreadLocalRandom.current().nextInt(NUM_PARTITIONS),
                                    kv.getValue())
                                .build()))
            .setRowSchema(SCHEMA)
            .apply("Apply Variable Load Periods", ParDo.of(new MultiplierDoFn<>(1, loadPeriods)))
            .apply("Counting element", ParDo.of(new CountingFn<>(WRITE_ELEMENT_METRIC_NAME)));

    Map<String, Object> writeConfig = new HashMap<>(managedConfig);
    writeConfig.put("triggering_frequency_seconds", 10);
    source.apply("Iceberg[BQMS] Write", Managed.write(Managed.ICEBERG).withConfig(writeConfig));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-iceberg-bqms")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter(
                "autoscalingAlgorithm",
                DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED
                    .toString())
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers))
            .addParameter("maxNumWorkers", String.valueOf(configuration.maxNumWorkers))
            .addParameter("experiments", configuration.useDataflowRunnerV2 ? "use_runner_v2" : "")
            .addParameter("experiments", "enable_streaming_engine")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** The method reads data from an Iceberg table. */
  private PipelineLauncher.LaunchInfo readData() throws IOException {
    Map<String, Object> readConfig = new HashMap<>(managedConfig);
    readConfig.put("streaming", true);
    readConfig.put("poll_interval_seconds", 30);
    // read from the beginning just in case the write job starts up first.
    readConfig.put("starting_strategy", "earliest");
    readPipeline
        .apply("Iceberg[BQMS] Read", Managed.read(Managed.ICEBERG_CDC).withConfig(readConfig))
        .getSinglePCollection()
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-iceberg-bqms")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("numWorkers", String.valueOf(configuration.numWorkers / 5))
            .addParameter("runner", configuration.runner)
            .addParameter("experiments", configuration.useDataflowRunnerV2 ? "use_runner_v2" : "")
            .build();

    return pipelineLauncher.launch(project, region, options);
  }

  /** Options for Iceberg IO stress test. */
  static class TestConfiguration extends SyntheticSourceOptions {
    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /**
     * Determines whether to use Dataflow runner v2. If set to true, it uses SDF mode for reading
     * from Kafka. Otherwise, Unbounded mode will be used.
     */
    @JsonProperty public boolean useDataflowRunnerV2 = false;

    /** Number of workers for the pipeline. */
    @JsonProperty public int numWorkers = 20;

    /** Maximum number of workers for the pipeline. */
    @JsonProperty public int maxNumWorkers = 100;

    /** Rows will be generated for this many minutes. */
    @JsonProperty public int minutes = 15;

    /**
     * Determines the destination for exporting metrics. If set to true, metrics will be exported to
     * InfluxDB and displayed using Grafana. If set to false, metrics will be exported to BigQuery
     * and displayed with Looker Studio.
     */
    @JsonProperty public boolean exportMetricsToInfluxDB = false;

    /** InfluxDB measurement to publish results to. * */
    @JsonProperty public String influxMeasurement = IcebergIOBigQueryMetastoreST.class.getName();

    /** InfluxDB host to publish metrics. * */
    @JsonProperty public String influxHost;

    /** InfluxDB database to publish metrics. * */
    @JsonProperty public String influxDatabase;
  }
}
