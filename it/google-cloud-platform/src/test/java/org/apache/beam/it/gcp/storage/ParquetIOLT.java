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

import static org.apache.beam.it.common.utils.ByteSizeUtils.formatBytes;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.dataflow.DefaultPipelineLauncher.PipelineMetricsType;
import org.apache.beam.it.common.storage.GcsIOLoadTestBase;
import org.apache.beam.it.common.storage.GcsResourceManager;
import org.apache.beam.it.common.utils.ByteSizeUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * ParquetIO performance tests on Google Cloud Storage.
 *
 * <p>Reads and writes are kept in two separate pipelines / tests:
 *
 * <ul>
 *   <li>{@code test1ParquetWrite} generates records of the configured shape and writes them as
 *       Parquet files under {@code outputPrefix}.
 *   <li>{@code test2ParquetRead} reads all the Parquet files matching {@code inputFilePattern},
 *       optionally projecting only the first {@code numFieldsToRead} fields.
 * </ul>
 *
 * <p>The methods are ordered by name so that the write test runs first, and the dataset it produces
 * is reused by the read test: running the whole class generates the dataset only once. Running the
 * read test on its own still works, it generates the dataset itself.
 *
 * <p>Both tests enable {@code --gcsPerformanceMetrics=true} so that the {@code gcs_*} client
 * metrics collected by {@link GcsIOLoadTestBase} are exported along with the runner metrics.
 *
 * <h3>Workload shape</h3>
 *
 * <p>The workload is described by two dimensions: {@code numFields} (how many columns a record has)
 * and {@code maxFieldSizeBytes} (how large a single value is). Everything else is held constant so
 * that runs of different shapes stay comparable:
 *
 * <ul>
 *   <li>{@code compressibility = 0.0}, i.e. incompressible payloads, so the bytes written to GCS
 *       match the configured dataset size. Compressible payloads would make the test measure the
 *       Parquet codec rather than the GCS client.
 *   <li>{@code compressionCodec = UNCOMPRESSED}, for the same reason.
 *   <li>{@code numShards} pinned, so that the number and the size of the GCS objects is identical
 *       across runs.
 *   <li>The Dataflow worker pool is pinned: autoscaling off, 3 workers, {@code e2-standard-2}. An
 *       autoscaled pool would give a cheap shape fewer workers than an expensive one, so the
 *       throughput of the two could not be compared.
 * </ul>
 *
 * <h3>Runner v2 or legacy worker</h3>
 *
 * <p>{@code useRunnerV2} picks the Dataflow worker and defaults to Runner v2:
 *
 * <pre>
 * # Runner v2, the default
 * -Dconfiguration=f100_s16
 *
 * # the legacy worker
 * -Dconfiguration='{"preset":"f100_s16","useRunnerV2":false}'
 * </pre>
 *
 * <p>Whichever is chosen, the job is launched with an explicit experiment, {@code use_runner_v2} or
 * {@code disable_runner_v2}. Leaving the choice to the service is not an option here because the
 * container image is resolved on the client, see {@link #dataflowWorkerExperiment()}: a job
 * submitted without an experiment ends up asking for an image tag that does not exist and hangs
 * with the workers in ImagePullBackOff.
 *
 * <p>Note that Runner v2 stages the locally built SDK jars, so a local SDK change is measured as
 * is, while the legacy worker runs the Beam code baked into its container image.
 *
 * <h3>GcsUtil v1 or v2</h3>
 *
 * <p>{@code useGcsUtilV2} routes GCS access through the java-storage client and defaults to off,
 * i.e. to the gcsio based GcsUtilV1 that every pipeline uses today:
 *
 * <pre>
 * -Dconfiguration='{"preset":"f100_s16","useGcsUtilV2":true}'
 * </pre>
 *
 * <p>On Dataflow this is sent as the {@code use_gcsutil_v2} experiment, for the other runners it is
 * set on the pipeline options directly. Either way {@code GcsUtil} logs the version it selected at
 * INFO, which is what to grep for to confirm a run really used the intended client.
 *
 * <p>The number of requests a write costs is decided by the upload chunk size, which {@code
 * gcsUploadBufferSizeBytes} pins when a run needs to sweep it or to rule it out as a variable:
 *
 * <pre>
 * -Dconfiguration='{"preset":"f100_s16","useGcsUtilV2":true,"gcsUploadBufferSizeBytes":"24M"}'
 * </pre>
 *
 * <h3>Configuration</h3>
 *
 * <p>{@code -Dconfiguration} takes either the name of a preset, or a json object. The json object
 * may name a base preset with a {@code "preset"} property and override any of its values, so that
 * one preset can be reused for several runs:
 *
 * <pre>
 * # a preset as is
 * -Dconfiguration=f100_s16
 *
 * # the same shape, but a cheap local run
 * -Dconfiguration='{"preset":"f100_s16","runner":"DirectRunner","totalBytes":"10M"}'
 *
 * # the same shape, reading only the first field of each record
 * -Dconfiguration='{"preset":"f100_s16","numFieldsToRead":1}'
 *
 * # no preset at all, every unset value falls back to the Configuration defaults
 * -Dconfiguration='{"numFields":10,"maxFieldSizeBytes":"1K","totalBytes":"1G"}'
 * </pre>
 *
 * <p>Every byte count, i.e. {@code totalBytes}, {@code maxFieldSizeBytes}, {@code
 * minFieldSizeBytes}, {@code rowGroupSize} and {@code gcsUploadBufferSizeBytes}, is a number of
 * bytes, optionally suffixed with {@code K}, {@code M}, {@code G} or {@code T}. The suffixes are
 * binary, so {@code 1K} is 1024 bytes.
 *
 * <p>Example trigger command:
 *
 * <pre>
 * ./gradlew :it:google-cloud-platform:ParquetPerformanceTest -Dconfiguration=f100_s16 \
 * -Dproject=[gcpProject] -DartifactBucket=[temp bucket]
 * </pre>
 *
 * <p>The gradle task always passes {@code configuration} down, defaulting to {@code local}, so
 * leaving the flag out runs a small local pipeline rather than a full scale one. Every run against
 * a real runner has to name its preset explicitly.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class ParquetIOLT extends GcsIOLoadTestBase {

  private static final String READ_ELEMENT_METRIC_NAME = "read_count";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String DATAFLOW_RUNNER = "DataflowRunner";

  /** Experiment that runs the job on Runner v2, i.e. on the unified worker. */
  private static final String RUNNER_V2_EXPERIMENT = "use_runner_v2";

  /**
   * Experiment that keeps the job on the legacy worker. Needed even though the legacy worker is
   * what a job without experiments is submitted as, because the service upgrades such a job to
   * Runner v2 on its own, see {@link #dataflowWorkerExperiment()}.
   */
  private static final String LEGACY_WORKER_EXPERIMENT = "disable_runner_v2";

  /** Experiment that routes GCS access through GcsUtilV2, i.e. the java-storage client. */
  private static final String GCS_UTIL_V2_EXPERIMENT = "use_gcsutil_v2";

  /** Pipeline option that pins the upload chunk size, see {@code GcsOptions}. */
  private static final String GCS_UPLOAD_BUFFER_SIZE_OPTION = "gcsUploadBufferSizeBytes";

  /**
   * Size of the worker pool every Dataflow run gets. Frozen, see {@link #launchConfig}: the runs
   * are only comparable if they all have the same amount of cpu, memory and network bandwidth.
   */
  private static final int DATAFLOW_NUM_WORKERS = 3;

  /** Machine type every Dataflow worker runs on. Frozen for the same reason. */
  private static final String DATAFLOW_MACHINE_TYPE = "e2-standard-2";

  /**
   * Dataset size every shape preset generates, so that the shapes are comparable. {@link
   * ByteSizeUtils#parseSizeToBytes} is binary, so this is 42,949,672,960 bytes.
   */
  private static final String MATRIX_TOTAL_BYTES = "40G";

  /**
   * Wall clock budget for a matrix run. Sized from measured runs: 10GB takes roughly 5 minutes of
   * worker time on the pinned pool, so 40GB needs about 20, and the read pipeline has to stay long
   * enough for Cloud Monitoring to have ingested more than just its last data point.
   */
  private static final int MATRIX_PIPELINE_TIMEOUT_MINUTES = 60;

  /**
   * Presets, kept as json so that a caller can name one as a base and override parts of it. The
   * {@code f<numFields>_s<maxFieldSize>} presets are the cells of the workload matrix.
   */
  private static final Map<String, String> TEST_CONFIGS_PRESET =
      ImmutableMap.<String, String>builder()
          // Small run against the Configuration defaults, for local development.
          .put("local", "{}")
          // Legacy size presets: a single field, the shape the test used to have.
          .put("medium", shape(1, "750", "7500M", 20))
          .put("large", shape(1, "750", "75G", 80))
          // Cells of the workload matrix.
          .put("f1_s1k", shape(1, "1K", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f10_s1k", shape(10, "1K", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f100_s16", shape(100, "16", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f1000_s16", shape(1000, "16", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f100_s1k", shape(100, "1K", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f1000_s1k", shape(1000, "1K", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          .put("f10_s64k", shape(10, "64K", MATRIX_TOTAL_BYTES, MATRIX_PIPELINE_TIMEOUT_MINUTES))
          // Blob column: a page size check every 100 rows would buffer 400 MB, so check every row.
          .put(
              "f1_s4m",
              "{\"numFields\":1,\"maxFieldSizeBytes\":\"4M\",\"totalBytes\":\""
                  + MATRIX_TOTAL_BYTES
                  + "\",\"minRowCountForPageSizeCheck\":1,\"numShards\":64,\"compressibility\":0.0,"
                  + "\"compressionCodec\":\"UNCOMPRESSED\",\"runner\":\"DataflowRunner\","
                  + "\"pipelineTimeout\":"
                  + MATRIX_PIPELINE_TIMEOUT_MINUTES
                  + "}")
          .build();

  private static GcsResourceManager resourceManager;

  /**
   * Prefix the write pipeline writes to, also used as read input when none is configured. Static so
   * that both tests share a single dataset.
   */
  private static String outputPrefix;

  private static Configuration configuration;

  /** Schema of the generated records, derived from {@code numFields}. */
  private static Schema schema;

  /** Whether {@code outputPrefix} already holds a dataset written by this class. */
  private static boolean datasetWritten;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  /** Returns the json of a shape preset, with all the frozen knobs pinned. */
  private static String shape(
      int numFields, String maxFieldSize, String totalSize, int pipelineTimeout) {
    return String.format(
        "{\"numFields\":%d,\"maxFieldSizeBytes\":\"%s\",\"totalBytes\":\"%s\",\"numShards\":64,"
            + "\"compressibility\":0.0,\"compressionCodec\":\"UNCOMPRESSED\","
            + "\"runner\":\"DataflowRunner\",\"pipelineTimeout\":%d}",
        numFields, maxFieldSize, totalSize, pipelineTimeout);
  }

  /**
   * Resolves the configuration and the dataset location once for the whole class, so that the write
   * test and the read test operate on the same files.
   */
  @BeforeClass
  public static void beforeClass() {
    resourceManager =
        GcsResourceManager.builder(TestProperties.artifactBucket(), "parquetiolt", CREDENTIALS)
            .build();

    String testConfig =
        TestProperties.getProperty("configuration", "local", TestProperties.Type.PROPERTY);
    configuration = resolveConfiguration(testConfig);
    validateAndDerive(configuration);
    schema = buildSchema(configuration.numFields);
    datasetWritten = false;

    if (!Strings.isNullOrEmpty(configuration.outputPrefix)) {
      outputPrefix = configuration.outputPrefix;
    } else {
      String tempDirName =
          "parquetiolt-"
              + DateTimeFormatter.ofPattern("MMddHHmmssSSS")
                  .withZone(ZoneOffset.UTC)
                  .format(java.time.Instant.now())
              + UUID.randomUUID().toString().substring(0, 10);
      resourceManager.registerTempDir(tempDirName);
      outputPrefix =
          String.format("gs://%s/%s/parquet", TestProperties.artifactBucket(), tempDirName);
    }
    printConfiguration();
  }

  @AfterClass
  public static void tearDownClass() {
    ResourceManagerUtils.cleanResources(resourceManager);
  }

  /** Writes the configured number of records under the configured output prefix. */
  @Test
  public void test1ParquetWrite() throws IOException {
    PipelineLauncher.LaunchInfo writeInfo = runWritePipeline(outputPrefix);

    printMetrics(
        writeInfo,
        MetricsConfiguration.builder()
            .setInputPCollection("Create avro records.out0")
            .setInputPCollectionV2("Create avro records/ParMultiDo(CreateAvroRecord).out0")
            .build());
  }

  /** Reads all the Parquet files matching the configured input file pattern. */
  @Test
  public void test2ParquetRead() throws IOException {
    String inputFilePattern = configuration.inputFilePattern;
    long expectedRecords = configuration.numRecords;
    if (Strings.isNullOrEmpty(inputFilePattern)) {
      if (!datasetWritten) {
        // No dataset given and the write test did not run: generate one so that the read test is
        // self contained. runWritePipeline already waits for the job and asserts it succeeded.
        runWritePipeline(outputPrefix);
      }
      inputFilePattern = outputPrefix + "*";
    }

    PCollection<FileIO.ReadableFile> files =
        readPipeline
            .apply("Create filepattern", Create.of(inputFilePattern))
            .apply("Match all files", FileIO.matchAll())
            .apply("Read matches", FileIO.readMatches());

    PCollection<GenericRecord> records;
    if (configuration.numFieldsToRead > 0
        && configuration.numFieldsToRead < configuration.numFields) {
      // Column projection: only the leading fields are fetched from the Parquet files, which is
      // what turns a sequential scan into many small ranged GETs.
      Schema projection = buildSchema(configuration.numFieldsToRead);
      records =
          files.apply(
              "Read parquet files",
              ParquetIO.readFiles(schema).withProjection(projection, projection));
    } else {
      records = files.apply("Read parquet files", ParquetIO.readFiles(schema));
    }
    records.apply("Counting element", ParDo.of(new CountingFn<>(READ_ELEMENT_METRIC_NAME)));

    PipelineLauncher.LaunchInfo readInfo =
        pipelineLauncher.launch(project, region, launchConfig("read-parquet", readPipeline));
    PipelineOperator.Result readResult =
        pipelineOperator.waitUntilDone(
            createConfig(readInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if the pipeline failed or timed out.
    assertThatResult(readResult).isLaunchFinished();

    // Only assert the record count when we know how many records the dataset holds.
    if (Strings.isNullOrEmpty(configuration.inputFilePattern)) {
      double numRecords =
          pipelineLauncher.getMetric(
              project,
              region,
              readInfo.jobId(),
              getBeamMetricsName(PipelineMetricsType.COUNTER, READ_ELEMENT_METRIC_NAME));
      assertEquals((double) expectedRecords, numRecords, 0.5);
    }

    printMetrics(
        readInfo,
        MetricsConfiguration.builder()
            .setOutputPCollection("Counting element.out0")
            .setOutputPCollectionV2("Counting element/ParMultiDo(Counting).out0")
            .build());
  }

  private PipelineLauncher.LaunchInfo runWritePipeline(String prefix) throws IOException {
    ParquetIO.Sink sink =
        ParquetIO.sink(schema)
            .withCompressionCodec(CompressionCodecName.fromConf(configuration.compressionCodec));
    if (configuration.rowGroupSize > 0) {
      sink = sink.withRowGroupSize(configuration.rowGroupSize);
    }
    if (configuration.minRowCountForPageSizeCheck > 0) {
      // With large values the default of a page size check every 100 rows buffers far too much.
      sink = sink.withMinRowCountForPageSizeCheck(configuration.minRowCountForPageSizeCheck);
    }

    // FileIO.write().to(...) expects a directory, so the prefix is split into the directory the
    // files are written to and the base name each file starts with. This way the written files are
    // "<prefix>-0000i-of-0000n.parquet" and can be matched back with "<prefix>*" by the read test.
    FileIO.Write<Void, GenericRecord> write =
        FileIO.<GenericRecord>write()
            .via(sink)
            .to(directoryOf(prefix))
            .withNaming(FileIO.Write.defaultNaming(baseNameOf(prefix), ".parquet"));
    if (configuration.numShards > 0) {
      write = write.withNumShards(configuration.numShards);
    }

    PCollection<GenericRecord> records =
        writePipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(configuration.numRecords))
            .apply(
                "Create avro records",
                ParDo.of(
                    new CreateAvroRecordFn(
                        schema.toString(),
                        configuration.numFields,
                        configuration.minFieldSizeBytes,
                        configuration.maxFieldSizeBytes,
                        configuration.compressibility)))
            .setCoder(AvroCoder.of(schema));
    records.apply("Write parquet files", write);

    PipelineLauncher.LaunchInfo writeInfo =
        pipelineLauncher.launch(project, region, launchConfig("write-parquet", writePipeline));
    PipelineOperator.Result writeResult =
        pipelineOperator.waitUntilDone(
            createConfig(writeInfo, Duration.ofMinutes(configuration.pipelineTimeout)));

    // Fail the test if the pipeline failed or timed out.
    assertThatResult(writeResult).isLaunchFinished();
    // The dataset now exists under `prefix`, so the read test can reuse it instead of writing a
    // second copy.
    datasetWritten = true;
    return writeInfo;
  }

  private PipelineLauncher.LaunchConfig launchConfig(String jobName, TestPipeline pipeline) {
    // The launcher only turns the parameters below into pipeline options for the DataflowRunner.
    // For the other runners it runs the pipeline with the options it already has, so the flag has
    // to be set explicitly here, otherwise no gcs_* metric is reported.
    pipeline.getOptions().as(GcsOptions.class).setGcsPerformanceMetrics(true);
    // Same reason: for a non-Dataflow runner the experiments parameter below never reaches the
    // pipeline, so the experiment has to be added to the options directly.
    if (configuration.useGcsUtilV2) {
      ExperimentalOptions.addExperiment(
          pipeline.getOptions().as(ExperimentalOptions.class), GCS_UTIL_V2_EXPERIMENT);
    }
    // Left unset the two clients pick different chunk sizes, which shows up as a difference in the
    // number of write requests.
    if (configuration.gcsUploadBufferSizeBytes > 0) {
      pipeline
          .getOptions()
          .as(GcsOptions.class)
          .setGcsUploadBufferSizeBytes(configuration.gcsUploadBufferSizeBytes);
    }

    PipelineLauncher.LaunchConfig.Builder builder =
        PipelineLauncher.LaunchConfig.builder(jobName)
            .setSdk(PipelineLauncher.Sdk.JAVA)
            .setPipeline(pipeline)
            .addParameter("runner", configuration.runner)
            // Required for GcsUtil to report the gcs_* client metrics.
            .addParameter(GCS_PERFORMANCE_METRICS_OPTION, "true");

    if (configuration.gcsUploadBufferSizeBytes > 0) {
      builder.addParameter(
          GCS_UPLOAD_BUFFER_SIZE_OPTION, String.valueOf(configuration.gcsUploadBufferSizeBytes));
    }

    if (DATAFLOW_RUNNER.equalsIgnoreCase(configuration.runner)) {
      // The worker pool is pinned so that the runs of the different workload shapes are
      // comparable: with autoscaling the service would give a shape that is cheap to process
      // fewer workers than an expensive one, and the throughput of the two could not be compared.
      // A fixed pool also keeps the number of parallel GCS connections constant, which is what
      // the gcs_* metrics measure.
      // maxNumWorkers is deliberately not set, it only bounds an autoscaling pool.
      builder
          // Picks the worker, see dataflowWorkerExperiment().
          .addParameter("experiments", dataflowWorkerExperiment())
          .addParameter("autoscalingAlgorithm", "NONE")
          .addParameter("numWorkers", String.valueOf(DATAFLOW_NUM_WORKERS))
          .addParameter("workerMachineType", DATAFLOW_MACHINE_TYPE);
    }

    return builder.build();
  }

  /**
   * Experiment that selects the Dataflow worker, {@code use_runner_v2} or {@code
   * disable_runner_v2}.
   *
   * <p>The worker is always selected explicitly, even though Runner v2 is what the service picks on
   * its own, because the container image is resolved on the client: {@code
   * DataflowRunner.getDefaultContainerImageUrl} takes the image name and the image tag from the
   * same branch of its {@code useUnifiedWorker()} check, and the two tags ({@code
   * dataflowFnapiContainerVersion} and {@code dataflowLegacyContainerVersion} in {@code
   * runners/google-cloud-dataflow-java/build.gradle}) are bumped independently.
   *
   * <p>A job submitted without an experiment therefore resolves the legacy pair {@code
   * beam-javaNN-batch:<legacy tag>}, which the service then upgrades to Runner v2 by renaming the
   * image to {@code beam_javaNN_sdk} while keeping the legacy tag. That image usually does not
   * exist, the workers fail with ImagePullBackOff and the job hangs until the test times out.
   *
   * <p>With the experiment set both paths resolve a container that exists: {@code use_runner_v2}
   * gives {@code beam_javaNN_sdk:<fnapi tag>}, {@code disable_runner_v2} keeps the service from
   * upgrading the job so {@code beam-javaNN-batch:<legacy tag>} stays correct.
   */
  private static String dataflowWorkerExperiment() {
    return configuration.useRunnerV2 ? RUNNER_V2_EXPERIMENT : LEGACY_WORKER_EXPERIMENT;
  }

  /**
   * Resolves {@code -Dconfiguration} into a {@link Configuration}.
   *
   * <p>The value is either the name of a preset, or a json object. A json object may select a base
   * preset with a {@code "preset"} property, in which case the remaining properties override the
   * ones of that preset. The merge is done on the json trees rather than on the deserialized
   * objects, because {@link SyntheticSourceOptions} has final properties that cannot be written
   * back.
   */
  private static Configuration resolveConfiguration(String spec) {
    String trimmed = spec.trim();
    try {
      ObjectNode overrides;
      if (trimmed.startsWith("{")) {
        JsonNode parsed = MAPPER.readTree(trimmed);
        if (!parsed.isObject()) {
          throw new IllegalArgumentException(
              String.format("Configuration json must be an object, but was: [%s]", trimmed));
        }
        overrides = (ObjectNode) parsed;
      } else {
        overrides = MAPPER.createObjectNode().put("preset", trimmed);
      }

      JsonNode preset = overrides.remove("preset");
      ObjectNode merged =
          preset == null
              ? MAPPER.createObjectNode()
              : (ObjectNode) MAPPER.readTree(presetJson(preset.asText()));
      merged.setAll(overrides);

      return Configuration.fromJsonString(merged.toString(), Configuration.class);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unable to parse test configuration: [%s]. Pass a valid configuration json, or one"
                  + " of the presets: %s",
              trimmed, TEST_CONFIGS_PRESET.keySet()),
          e);
    }
  }

  private static String presetJson(String name) {
    String preset = TEST_CONFIGS_PRESET.get(name);
    if (preset == null) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown preset: [%s]. Known presets: %s", name, TEST_CONFIGS_PRESET.keySet()));
    }
    return preset;
  }

  /** Checks the configuration and fills in the values that are derived from the others. */
  private static void validateAndDerive(Configuration configuration) {
    checkConfig(configuration.numFields > 0, "numFields must be positive");
    checkConfig(configuration.maxFieldSizeBytes > 0, "maxFieldSizeBytes must be positive");
    if (configuration.minFieldSizeBytes < 0) {
      configuration.minFieldSizeBytes = configuration.maxFieldSizeBytes;
    }
    checkConfig(
        configuration.minFieldSizeBytes <= configuration.maxFieldSizeBytes,
        "minFieldSizeBytes must not be greater than maxFieldSizeBytes");
    checkConfig(
        configuration.compressibility >= 0.0 && configuration.compressibility <= 1.0,
        "compressibility must be within [0.0, 1.0]");
    checkConfig(
        configuration.numFieldsToRead >= 0
            && configuration.numFieldsToRead <= configuration.numFields,
        "numFieldsToRead must be within [0, numFields]");

    if (configuration.totalBytes > 0) {
      configuration.numRecords =
          Math.max(1L, configuration.totalBytes / recordBytes(configuration));
    }
    checkConfig(
        configuration.numRecords > 0,
        "numRecords is 0. Set either numRecords or totalBytes, otherwise the write pipeline is a"
            + " no-op");
  }

  /** Average number of payload bytes of a record, ignoring the Parquet overhead. */
  private static long recordBytes(Configuration configuration) {
    // The field sizes are ints, so the arithmetic is widened to long: a wide record of large
    // fields overflows an int.
    long avgFieldSize =
        ((long) configuration.minFieldSizeBytes + configuration.maxFieldSizeBytes) / 2;
    return Math.max(1L, configuration.numFields * avgFieldSize);
  }

  private static void checkConfig(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  /** Prints the effective configuration, so that a run can be matched with its metrics. */
  private static void printConfiguration() {
    System.out.printf(
        "%n==========================================================%n"
            + "  TEST CONFIGURATION%n"
            + "==========================================================%n"
            + "  numFields:              %,d%n"
            + "  fieldSize:              %s .. %s%n"
            + "  recordSize:             %s%n"
            + "  numRecords:             %,d%n"
            + "  logicalSize:            %s%n"
            + "  compressibility:        %.2f%n"
            + "  compressionCodec:       %s%n"
            + "  rowGroupSize:           %s%n"
            + "  numShards:              %d%n"
            + "  numFieldsToRead:        %s%n"
            + "  runner:                 %s%n"
            + "  gcsUtil:                %s%n"
            + "  uploadChunkSize:        %s%n"
            + "  dataflowWorker:         %s%n"
            + "  workerPool:             %s%n"
            + "==========================================================%n%n",
        configuration.numFields,
        formatBytes(configuration.minFieldSizeBytes),
        formatBytes(configuration.maxFieldSizeBytes),
        formatBytes(recordBytes(configuration)),
        configuration.numRecords,
        formatBytes(configuration.numRecords * recordBytes(configuration)),
        configuration.compressibility,
        configuration.compressionCodec,
        configuration.rowGroupSize > 0 ? formatBytes(configuration.rowGroupSize) : "default",
        configuration.numShards,
        configuration.numFieldsToRead > 0 ? String.valueOf(configuration.numFieldsToRead) : "all",
        configuration.runner,
        configuration.useGcsUtilV2 ? "V2 (java-storage)" : "V1 (gcsio)",
        configuration.gcsUploadBufferSizeBytes > 0
            ? formatBytes(configuration.gcsUploadBufferSizeBytes)
            : "client default",
        DATAFLOW_RUNNER.equalsIgnoreCase(configuration.runner)
            ? String.format(
                "%s (--experiments=%s)",
                configuration.useRunnerV2 ? "Runner v2" : "legacy", dataflowWorkerExperiment())
            : "n/a",
        DATAFLOW_RUNNER.equalsIgnoreCase(configuration.runner)
            ? String.format("%d x %s, autoscaling off", DATAFLOW_NUM_WORKERS, DATAFLOW_MACHINE_TYPE)
            : "n/a");
  }

  /** Builds a record schema of {@code numFields} byte array fields named {@code f0..fN-1}. */
  private static Schema buildSchema(int numFields) {
    SchemaBuilder.FieldAssembler<Schema> fields =
        SchemaBuilder.record("TestAvroLine").namespace("ioitavro").fields();
    for (int i = 0; i < numFields; i++) {
      fields = fields.name(fieldName(i)).type().bytesType().noDefault();
    }
    return fields.endRecord();
  }

  private static String fieldName(int index) {
    return "f" + index;
  }

  /** Returns the directory part of a file prefix, e.g. {@code gs://bucket/dir/} for a prefix. */
  private static String directoryOf(String prefix) {
    int lastSlash = prefix.lastIndexOf('/');
    return lastSlash < 0 ? prefix : prefix.substring(0, lastSlash + 1);
  }

  /** Returns the file name part of a file prefix, e.g. {@code parquet} for {@code .../parquet}. */
  private static String baseNameOf(String prefix) {
    int lastSlash = prefix.lastIndexOf('/');
    String baseName = lastSlash < 0 ? prefix : prefix.substring(lastSlash + 1);
    return baseName.isEmpty() ? "output" : baseName;
  }

  /** Turns a sequence number into a record of the configured shape. */
  private static final class CreateAvroRecordFn extends DoFn<Long, GenericRecord> {
    // Schema is not serializable, so it is carried as json and parsed on the worker.
    private final String schemaJson;
    private final int numFields;
    private final int minFieldSizeBytes;
    private final int maxFieldSizeBytes;
    private final double compressibility;

    private transient Schema schema;

    CreateAvroRecordFn(
        String schemaJson,
        int numFields,
        int minFieldSizeBytes,
        int maxFieldSizeBytes,
        double compressibility) {
      this.schemaJson = schemaJson;
      this.numFields = numFields;
      this.minFieldSizeBytes = minFieldSizeBytes;
      this.maxFieldSizeBytes = maxFieldSizeBytes;
      this.compressibility = compressibility;
    }

    @Setup
    public void setup() {
      schema = new Schema.Parser().parse(schemaJson);
    }

    @ProcessElement
    public void processElement(@Element Long element, OutputReceiver<GenericRecord> receiver) {
      // Seeded with the element so that a record always holds the same content, whatever the
      // runner decides to retry.
      Random random = new Random(element);
      GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      for (int i = 0; i < numFields; i++) {
        int size =
            minFieldSizeBytes == maxFieldSizeBytes
                ? maxFieldSizeBytes
                : minFieldSizeBytes + random.nextInt(maxFieldSizeBytes - minFieldSizeBytes + 1);
        builder.set(fieldName(i), ByteBuffer.wrap(payload(random, size)));
      }
      receiver.output(builder.build());
    }

    /**
     * Returns {@code size} bytes of which a {@code 1 - compressibility} fraction is random. The
     * remaining bytes are left at zero, which is what the Parquet codec can collapse.
     */
    private byte[] payload(Random random, int size) {
      byte[] payload = new byte[size];
      int randomBytes = (int) Math.round(size * (1.0 - compressibility));
      if (randomBytes > 0) {
        byte[] randomPart = new byte[randomBytes];
        random.nextBytes(randomPart);
        System.arraycopy(randomPart, 0, payload, 0, randomBytes);
      }
      return payload;
    }
  }

  /**
   * Options of the ParquetIO load test.
   *
   * <p>Each option is tagged with its role in the workload matrix: {@code SWEPT} options describe
   * the shape of the data, {@code FROZEN} ones are held constant so that the shapes stay
   * comparable, and {@code DERIVED} ones are computed from the others.
   *
   * <p>{@link SyntheticSourceOptions} leaves {@code numRecords} at 0, which would silently turn the
   * write pipeline into a no-op. The defaults below describe a small local run instead, so that a
   * partial configuration json still runs a meaningful test.
   */
  static class Configuration extends SyntheticSourceOptions {
    Configuration() {
      // Inherited from SyntheticSourceOptions / SyntheticOptions. Jackson overwrites them when the
      // corresponding property is present in the configuration json. valueSizeBytes is unused, the
      // payload size is described by min/maxFieldSizeBytes.
      numRecords = 1000;
      valueSizeBytes = 750;
    }

    // --- Record shape --------------------------------------------------------------------------

    /** SWEPT. Number of fields per record. */
    @JsonProperty public int numFields = 1;

    /**
     * SWEPT. Upper bound of a single field's payload. Either a number of bytes or a suffixed size,
     * e.g. {@code 1024}, {@code "1K"} or {@code "4M"}. Declared as an {@code int} because a field
     * payload is a {@code byte[]}, so a size above {@link Integer#MAX_VALUE} is rejected when the
     * configuration is parsed.
     */
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.IntDeserializer.class)
    public int maxFieldSizeBytes = 750;

    /**
     * FROZEN at -1, meaning a fixed size of maxFieldSizeBytes. Set it for variable size fields.
     * Accepts a suffixed size as well.
     */
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.IntDeserializer.class)
    public int minFieldSizeBytes = -1;

    // --- Data content --------------------------------------------------------------------------

    /**
     * FROZEN at 0.0. 0.0 generates incompressible payloads, 1.0 generates zeros. Anything above 0
     * makes the bytes written to GCS smaller than the configured dataset, which turns the test into
     * a measure of the Parquet codec rather than of the GCS client.
     */
    @JsonProperty public double compressibility = 0.0;

    // --- Dataset scale -------------------------------------------------------------------------

    /**
     * Total size of the dataset. When positive, {@code numRecords} is DERIVED from it as {@code
     * totalBytes / recordBytes}. Either a number of bytes or a suffixed size, e.g. {@code "10G"}.
     * The shape presets pin it so that every cell of the matrix moves the same number of bytes.
     */
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.Deserializer.class)
    public long totalBytes = 0;

    // --- Object and file layout ----------------------------------------------------------------

    /** FROZEN. Number of output shards. 0 lets the runner decide and makes runs incomparable. */
    @JsonProperty public int numShards = 4;

    /**
     * FROZEN at 0, meaning the ParquetIO default of 128 MB. Accepts a suffixed size, e.g. "64M".
     */
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.IntDeserializer.class)
    public int rowGroupSize = 0;

    /** FROZEN at UNCOMPRESSED, so that the bytes on GCS are the bytes of the dataset. */
    @JsonProperty public String compressionCodec = "UNCOMPRESSED";

    /**
     * Number of rows the Parquet writer buffers between two page size checks. 0 keeps the ParquetIO
     * default of 100, which buffers too much when the records are large.
     */
    @JsonProperty public int minRowCountForPageSizeCheck = 0;

    // --- Read access pattern -------------------------------------------------------------------

    /**
     * Number of leading fields to project when reading. 0 reads all of them. Projecting a few
     * fields out of a wide record is what turns a sequential scan into many small ranged GETs.
     */
    @JsonProperty public int numFieldsToRead = 0;

    /**
     * Glob pattern of the Parquet files to read, e.g. {@code gs://<bucket>/parquet-files/sample*}.
     * Note that a single {@code *} never crosses a {@code /} boundary. If not set, the read test
     * generates its own dataset first and reads it back. The dataset must have been written with
     * the same {@code numFields}, otherwise the records cannot be decoded.
     */
    @JsonProperty public String inputFilePattern = "";

    /**
     * Prefix the Parquet files are written to, e.g. {@code gs://<bucket>/output}. If not set, a
     * temporary directory under the artifact bucket is used and cleaned up afterwards.
     */
    @JsonProperty public String outputPrefix = "";

    // --- Execution -----------------------------------------------------------------------------

    /** Runner specified to run the pipeline. */
    @JsonProperty public String runner = "DirectRunner";

    /**
     * Dataflow only. {@code true} runs the job on Runner v2, i.e. the unified worker, {@code false}
     * on the legacy worker. Either way the choice is sent to the service as an explicit experiment,
     * see {@link ParquetIOLT#dataflowWorkerExperiment()} for why it must not be left to the
     * service.
     */
    @JsonProperty public boolean useRunnerV2 = true;

    /**
     * {@code true} routes GCS access through GcsUtilV2, the java-storage client, instead of the
     * default GcsUtilV1. Applies to every runner: on Dataflow it is sent as the {@code
     * use_gcsutil_v2} experiment, elsewhere it is set on the pipeline options directly.
     *
     * <p>Which one a run actually used is logged by {@code GcsUtil} at INFO.
     */
    @JsonProperty public boolean useGcsUtilV2 = false;

    /**
     * Size of a single upload chunk, i.e. of one resumable upload request. 0 leaves the client
     * default, which both clients derive from the heap size. Accepts a suffixed size, e.g. "24M".
     * Keep it a multiple of 8M, gcsio requires that granularity.
     */
    @JsonProperty
    @JsonDeserialize(using = ByteSizeUtils.IntDeserializer.class)
    public int gcsUploadBufferSizeBytes = 0;

    /** Pipeline timeout in minutes. Must be a positive value. */
    @JsonProperty public int pipelineTimeout = 2;
  }
}
