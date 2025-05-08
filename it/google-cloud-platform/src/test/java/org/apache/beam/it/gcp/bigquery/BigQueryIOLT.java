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
package org.apache.beam.it.gcp.bigquery;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.IOLoadTestBase;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * BigQueryIO performance tests.
 *
 * <p>Example trigger command for all tests:
 *
 * <pre>
 * mvn test -pl it/google-cloud-platform -am -Dtest="BigQueryIOLT" -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 *
 * <p>Example trigger command for specific test running on Dataflow runner:
 *
 * <p><b>Maven</b>
 *
 * <pre>
 * mvn test -pl it/google-cloud-platform -am -Dtest="BigQueryIOLT#testAvroFileLoadsWriteThenRead" \
 * -Dconfiguration=medium -Dproject=[gcpProject] -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 *
 * <p><b>Gradle</b>
 *
 * <pre>
 * ./gradlew :it:google-cloud-platform:BigQueryPerformanceTest --tests='BigQueryIOLT.testAvroFileLoadsWriteThenRead' \
 * -Dconfiguration=medium -Dproject=[gcpProject] -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 *
 * <p>Example trigger command for specific test and custom data configuration:
 *
 * <pre>mvn test -pl it/google-cloud-platform -am \
 * -Dconfiguration="{\"numRecords\":1000,\"valueSizeBytes\":1000,\"numColumns\":100,\"pipelineTimeout\":2,\"runner\":\"DataflowRunner\"}" \
 * -Dtest="BigQueryIOLT#testAvroFileLoadsWriteThenRead" -Dconfiguration=local -Dproject=[gcpProject] \
 * -DartifactBucket=[temp bucket] -DfailIfNoTests=false
 * </pre>
 */
public final class BigQueryIOLT extends IOLoadTestBase {

  private static BigQueryResourceManager resourceManager;
  private static String tableQualifier;
  private static final String READ_ELEMENT_METRIC_NAME = "read_count";
  private Configuration configuration;
  private String tempLocation;
  private TableSchema schema;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        BigQueryResourceManager.builder("io-bigquery-lt", project, CREDENTIALS).build();
    resourceManager.createDataset(region);
  }

  @Before
  public void setup() {
    // generate a random table name
    String tableName =
        "io-bq-table-"
            + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                .withZone(ZoneId.of("UTC"))
                .format(java.time.Instant.now())
            + UUID.randomUUID().toString().substring(0, 10);
    tableQualifier = String.format("%s:%s.%s", project, resourceManager.getDatasetId(), tableName);

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
    List<TableFieldSchema> fields = new ArrayList<>(configuration.numColumns);
    for (int idx = 0; idx < configuration.numColumns; ++idx) {
      fields.add(new TableFieldSchema().setName("data_" + idx).setType("BYTES"));
    }
    schema = new TableSchema().setFields(fields);

    // tempLocation needs to be set for bigquery IO writes
    if (!Strings.isNullOrEmpty(tempBucketName)) {
      tempLocation = String.format("gs://%s/temp/", tempBucketName);
      writePipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      writePipeline.getOptions().setTempLocation(tempLocation);
      readPipeline.getOptions().as(TestPipelineOptions.class).setTempRoot(tempLocation);
      readPipeline.getOptions().setTempLocation(tempLocation);
    }
  }

  @AfterClass
  public static void tearDownClass() {
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
                  "{\"numRecords\":10000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":20,\"runner\":\"DataflowRunner\",\"workerMachineType\":\"e2-standard-2\",\"experiments\":\"disable_runner_v2\",\"numWorkers\":\"1\",\"maxNumWorkers\":\"1\"}",
                  Configuration.class), // 10 GB
              "large",
              Configuration.fromJsonString(
                  "{\"numRecords\":100000000,\"valueSizeBytes\":1000,\"pipelineTimeout\":80,\"runner\":\"DataflowRunner\",\"workerMachineType\":\"e2-standard-2\",\"experiments\":\"disable_runner_v2\",\"numWorkers\":\"1\",\"maxNumWorkers\":\"1\",\"numStorageWriteApiStreams\":4,\"storageWriteApiTriggeringFrequencySec\":20}",
                  Configuration.class) // 100 GB
              );
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testAvroFileLoadsWriteThenRead() throws IOException {
    configuration.writeFormat = "AVRO";
    configuration.writeMethod = "FILE_LOADS";
    testWriteAndRead();
  }

  @Test
  public void testJsonFileLoadsWriteThenRead() throws IOException {
    configuration.writeFormat = "JSON";
    configuration.writeMethod = "FILE_LOADS";
    testWriteAndRead();
  }

  @Test
  @Ignore("Avro streaming write is not supported as of Beam v2.45.0")
  public void testAvroStreamingWriteThenRead() throws IOException {
    configuration.writeFormat = "AVRO";
    configuration.writeMethod = "STREAMING_INSERTS";
    testWriteAndRead();
  }

  @Test
  public void testJsonStreamingWriteThenRead() throws IOException {
    configuration.writeFormat = "JSON";
    configuration.writeMethod = "STREAMING_INSERTS";
    testWriteAndRead();
  }

  @Test
  public void testStorageAPIWriteThenRead() throws IOException {
    configuration.readMethod = "DIRECT_READ";
    configuration.writeFormat = "AVRO";
    configuration.writeMethod = "STORAGE_WRITE_API";
    testWriteAndRead();
  }

  /** Run integration test with configurations specified by TestProperties. */
  public void testWriteAndRead() throws IOException {
    WriteFormat writeFormat = WriteFormat.valueOf(configuration.writeFormat);
    BigQueryIO.Write<byte[]> writeIO = null;
    switch (writeFormat) {
      case AVRO:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withNumStorageWriteApiStreams(
                    configuration.numStorageWriteApiStreams) // control the number of streams
                .withAvroFormatFunction(
                    new AvroFormatFn(
                        configuration.numColumns,
                        !("STORAGE_WRITE_API".equalsIgnoreCase(configuration.writeMethod))));
        break;
      case JSON:
        writeIO =
            BigQueryIO.<byte[]>write()
                .withSuccessfulInsertsPropagation(false)
                .withNumStorageWriteApiStreams(
                    configuration.numStorageWriteApiStreams) // control the number of streams
                .withFormatFunction(new JsonFormatFn(configuration.numColumns));
        break;
    }
    testWrite(writeIO);
    testRead();
  }

  private void testWrite(BigQueryIO.Write<byte[]> writeIO) throws IOException {
    BigQueryIO.Write.Method method = BigQueryIO.Write.Method.valueOf(configuration.writeMethod);
    if (method == BigQueryIO.Write.Method.STREAMING_INSERTS) {
      writePipeline.getOptions().as(StreamingOptions.class).setStreaming(true);
    }
    writePipeline
        .apply("Read from source", Read.from(new SyntheticBoundedSource(configuration)))
        .apply("Map records", ParDo.of(new MapKVToV()))
        .apply(
            "Write to BQ",
            writeIO
                .to(tableQualifier)
                .withMethod(method)
                .withSchema(schema)
                .withCustomGcsTempLocation(ValueProvider.StaticValueProvider.of(tempLocation)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("write-bigquery")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(writePipeline)
            .addParameter("runner", configuration.runner)
            .addParameter("workerMachineType", configuration.workerMachineType)
            .addParameter("experiments", configuration.experiments)
            .addParameter("numWorkers", configuration.numWorkers)
            .addParameter("maxNumWorkers", configuration.maxNumWorkers)
            .build();

    PipelineLauncher.LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(
            createConfig(launchInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if pipeline failed.
    assertNotEquals(PipelineOperator.Result.LAUNCH_FAILED, result);

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setInputPCollection("Map records.out0")
            .setInputPCollectionV2("Map records/ParMultiDo(MapKVToV).out0")
            .build();
    try {
      exportMetricsToBigQuery(launchInfo, getMetrics(launchInfo, metricsConfig));
    } catch (ParseException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void testRead() throws IOException {
    BigQueryIO.TypedRead.Method method =
        BigQueryIO.TypedRead.Method.valueOf(configuration.readMethod);

    readPipeline
        .apply("Read from BQ", BigQueryIO.readTableRows().from(tableQualifier).withMethod(method))
        .apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchConfig options =
        PipelineLauncher.LaunchConfig.builder("read-bigquery")
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(readPipeline)
            .addParameter("runner", configuration.runner)
            .addParameter("workerMachineType", configuration.workerMachineType)
            .addParameter("experiments", configuration.experiments)
            .addParameter("numWorkers", configuration.numWorkers)
            .addParameter("maxNumWorkers", configuration.maxNumWorkers)
            .build();

    PipelineLauncher.LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    PipelineOperator.Result result =
        pipelineOperator.waitUntilDone(
            createConfig(launchInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if pipeline failed or timeout.
    assertThatResult(result).isLaunchFinished();

    // check metrics
    double numRecords =
        pipelineLauncher.getMetric(
            project,
            region,
            launchInfo.jobId(),
            getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));
    assertEquals(configuration.numRecords, numRecords, 0.5);

    // export metrics
    MetricsConfiguration metricsConfig =
        MetricsConfiguration.builder()
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build();
    try {
      exportMetricsToBigQuery(launchInfo, getMetrics(launchInfo, metricsConfig));
    } catch (ParseException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static class MapKVToV extends DoFn<KV<byte[], byte[]>, byte[]> {
    @ProcessElement
    public void process(ProcessContext context) {
      context.output(Objects.requireNonNull(context.element()).getValue());
    }
  }

  abstract static class FormatFn<InputT, OutputT> implements SerializableFunction<InputT, OutputT> {
    protected final int numColumns;

    public FormatFn(int numColumns) {
      this.numColumns = numColumns;
    }
  }

  private static class AvroFormatFn extends FormatFn<AvroWriteRequest<byte[]>, GenericRecord> {

    protected final boolean isWrapBytes;

    public AvroFormatFn(int numColumns, boolean isWrapBytes) {
      super(numColumns);
      this.isWrapBytes = isWrapBytes;
    }

    // TODO(https://github.com/apache/beam/issues/26408) eliminate this method once the Beam issue
    // resolved
    private Object maybeWrapBytes(byte[] input) {
      if (isWrapBytes) {
        return ByteBuffer.wrap(input);
      } else {
        return input;
      }
    }

    @Override
    public GenericRecord apply(AvroWriteRequest<byte[]> writeRequest) {
      byte[] data = Objects.requireNonNull(writeRequest.getElement());
      GenericRecord record = new GenericData.Record(writeRequest.getSchema());
      if (numColumns == 1) {
        // only one column, just wrap incoming bytes
        record.put("data_0", maybeWrapBytes(data));
      } else {
        // otherwise, distribute bytes
        int bytePerCol = data.length / numColumns;
        int curIdx = 0;
        for (int idx = 0; idx < numColumns - 1; ++idx) {
          record.put(
              "data_" + idx, maybeWrapBytes(Arrays.copyOfRange(data, curIdx, curIdx + bytePerCol)));
          curIdx += bytePerCol;
        }
        record.put(
            "data_" + (numColumns - 1),
            maybeWrapBytes(Arrays.copyOfRange(data, curIdx, data.length)));
      }
      return record;
    }
  }

  private static class JsonFormatFn extends FormatFn<byte[], TableRow> {

    public JsonFormatFn(int numColumns) {
      super(numColumns);
    }

    @Override
    public TableRow apply(byte[] input) {
      TableRow tableRow = new TableRow();
      Base64.Encoder encoder = Base64.getEncoder();
      if (numColumns == 1) {
        // only one column, just wrap incoming bytes
        tableRow.set("data_0", encoder.encodeToString(input));
      } else {
        // otherwise, distribute bytes
        int bytePerCol = input.length / numColumns;
        int curIdx = 0;
        for (int idx = 0; idx < numColumns - 1; ++idx) {
          tableRow.set(
              "data_" + idx,
              encoder.encodeToString(Arrays.copyOfRange(input, curIdx, curIdx + bytePerCol)));
          curIdx += bytePerCol;
        }
        tableRow.set(
            "data_" + (numColumns - 1),
            encoder.encodeToString(Arrays.copyOfRange(input, curIdx, input.length)));
      }
      return tableRow;
    }
  }

  private enum WriteFormat {
    AVRO,
    JSON
  }

  /** Options for Bigquery IO load test. */
  static class Configuration extends SyntheticSourceOptions {

    /**
     * Number of columns of each record. The column size is equally distributed as
     * valueSizeBytes/numColumns.
     */
    @JsonProperty public int numColumns = 1;

    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 20;

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /** Worker machine type specified to run the pipeline with Dataflow Runner. */
    @JsonProperty public String workerMachineType = "";

    /** Experiments specified to run the pipeline. */
    @JsonProperty public String experiments = "";

    /** Number of workers to start the pipeline. Must be a positive value. */
    @JsonProperty public String numWorkers = "1";

    /** Maximum umber of workers for the pipeline. Must be a positive value. */
    @JsonProperty public String maxNumWorkers = "1";

    /** BigQuery read method: DEFAULT/DIRECT_READ/EXPORT. */
    @JsonProperty public String readMethod = "DEFAULT";

    /** BigQuery write method: DEFAULT/FILE_LOADS/STREAMING_INSERTS/STORAGE_WRITE_API. */
    @JsonProperty public String writeMethod = "DEFAULT";

    /**
     * BigQuery number of streams for write method STORAGE_WRITE_API. 0 let's the runner determine
     * the number of streams. Remark : max limit for open connections per hour is 10K streams.
     */
    @JsonProperty public int numStorageWriteApiStreams = 0;

    /**
     * BigQuery triggering frequency in second in combination with the number of streams for write
     * method STORAGE_WRITE_API.
     */
    @JsonProperty public int storageWriteApiTriggeringFrequencySec = 20;

    /** BigQuery write format: AVRO/JSON. */
    @JsonProperty public String writeFormat = "AVRO";
  }
}
