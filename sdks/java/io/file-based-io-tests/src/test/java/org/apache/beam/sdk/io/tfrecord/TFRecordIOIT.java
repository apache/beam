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
package org.apache.beam.sdk.io.tfrecord;

import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.TFRecordIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/file-based-io-tests
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--datasetSize=12345",
 *  "--expectedHash=99f23ab",
 *  "--filenamePrefix=output_file_path",
 *  "--compressionType=GZIP"
 *  ]'
 *  --tests org.apache.beam.sdk.io.tfrecord.TFRecordIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class TFRecordIOIT {
  private static final String TFRECORD_NAMESPACE = TFRecordIOIT.class.getName();

  // Metric names
  private static final String WRITE_TIME = "write_time";
  private static final String READ_TIME = "read_time";
  private static final String DATASET_SIZE = "dataset_size";
  private static final String RUN_TIME = "run_time";

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Integer datasetSize;
  private static String expectedHash;
  private static Compression compressionType;
  private static InfluxDBSettings settings;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();
    datasetSize = options.getDatasetSize();
    expectedHash = options.getExpectedHash();
    numberOfTextLines = options.getNumberOfRecords();
    compressionType = Compression.valueOf(options.getCompressionType());
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  private static String createFilenamePattern() {
    return filenamePrefix + "*";
  }

  // TODO: There are two pipelines due to: https://issues.apache.org/jira/browse/BEAM-3267
  @Test
  public void writeThenReadAll() {
    final TFRecordIO.Write writeTransform =
        TFRecordIO.write()
            .to(filenamePrefix)
            .withCompression(compressionType)
            .withSuffix(".tfrecord");

    writePipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
        .apply(
            "Produce text lines",
            ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
        .apply("Transform strings to bytes", MapElements.via(new StringToByteArray()))
        .apply(
            "Record time before writing",
            ParDo.of(new TimeMonitor<>(TFRECORD_NAMESPACE, WRITE_TIME)))
        .apply("Write content to files", writeTransform);

    final PipelineResult writeResult = writePipeline.run();
    PipelineResult.State writeState = writeResult.waitUntilFinish();

    String filenamePattern = createFilenamePattern();
    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(TFRecordIO.read().from(filenamePattern).withCompression(AUTO))
            .apply(
                "Record time after reading",
                ParDo.of(new TimeMonitor<>(TFRECORD_NAMESPACE, READ_TIME)))
            .apply("Transform bytes to strings", MapElements.via(new ByteArrayToString()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()))
            .apply(Reshuffle.viaRandomKey());

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    readPipeline
        .apply(Create.of(filenamePattern))
        .apply(
            "Delete test files",
            ParDo.of(new DeleteFileFn())
                .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));
    final PipelineResult readResult = readPipeline.run();
    PipelineResult.State readState = readResult.waitUntilFinish();
    collectAndPublishMetrics(writeResult, readResult);
    // Fail the test if pipeline failed.
    assertNotEquals(writeState, PipelineResult.State.FAILED);
    assertNotEquals(readState, PipelineResult.State.FAILED);
  }

  private void collectAndPublishMetrics(
      final PipelineResult writeResults, final PipelineResult readResults) {
    final String uuid = UUID.randomUUID().toString();
    final String timestamp = Instant.now().toString();
    final Set<NamedTestResult> results = new HashSet<>();

    results.add(
        NamedTestResult.create(uuid, timestamp, RUN_TIME, getRunTime(writeResults, readResults)));
    results.addAll(
        MetricsReader.ofResults(writeResults, TFRECORD_NAMESPACE)
            .readAll(getWriteMetricSuppliers(uuid, timestamp)));
    results.addAll(
        MetricsReader.ofResults(readResults, TFRECORD_NAMESPACE)
            .readAll(getReadMetricSuppliers(uuid, timestamp)));

    IOITMetrics.publishToInflux(uuid, timestamp, results, settings);
  }

  private static Set<Function<MetricsReader, NamedTestResult>> getWriteMetricSuppliers(
      final String uuid, final String timestamp) {
    final Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, WRITE_TIME));
    suppliers.add(ignored -> NamedTestResult.create(uuid, timestamp, DATASET_SIZE, datasetSize));
    return suppliers;
  }

  private static Set<Function<MetricsReader, NamedTestResult>> getReadMetricSuppliers(
      final String uuid, final String timestamp) {
    final Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(getTimeMetric(uuid, timestamp, READ_TIME));
    return suppliers;
  }

  private static Function<MetricsReader, NamedTestResult> getTimeMetric(
      final String uuid, final String timestamp, final String metricName) {
    return reader -> {
      final long startTime = reader.getStartTimeMetric(metricName);
      final long endTime = reader.getEndTimeMetric(metricName);
      return NamedTestResult.create(uuid, timestamp, metricName, (endTime - startTime) / 1e3);
    };
  }

  private static double getRunTime(
      final PipelineResult writeResults, final PipelineResult readResult) {
    final long startTime =
        MetricsReader.ofResults(writeResults, TFRECORD_NAMESPACE).getStartTimeMetric(WRITE_TIME);
    final long endTime =
        MetricsReader.ofResults(readResult, TFRECORD_NAMESPACE).getEndTimeMetric(READ_TIME);
    return (endTime - startTime) / 1e3;
  }

  static class StringToByteArray extends SimpleFunction<String, byte[]> {
    @Override
    public byte[] apply(String input) {
      return input.getBytes(StandardCharsets.UTF_8);
    }
  }

  static class ByteArrayToString extends SimpleFunction<byte[], String> {
    @Override
    public String apply(byte[] input) {
      return new String(input, StandardCharsets.UTF_8);
    }
  }
}
