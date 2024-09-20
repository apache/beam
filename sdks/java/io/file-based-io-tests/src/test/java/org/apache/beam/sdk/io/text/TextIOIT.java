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
package org.apache.beam.sdk.io.text;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.IOITMetrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.InfluxDBSettings;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.TextIO}.
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
 *  --tests org.apache.beam.sdk.io.text.TextIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class TextIOIT {

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Integer datasetSize;
  private static String expectedHash;
  private static Compression compressionType;
  private static Integer numShards;
  private static boolean gatherGcsPerformanceMetrics;
  private static InfluxDBSettings settings;
  private static final String FILEIOIT_NAMESPACE = TextIOIT.class.getName();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();
    datasetSize = options.getDatasetSize();
    expectedHash = options.getExpectedHash();
    numberOfTextLines = options.getNumberOfRecords();
    compressionType = Compression.valueOf(options.getCompressionType());
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    numShards = options.getNumberOfShards();
    gatherGcsPerformanceMetrics = options.getReportGcsPerformanceMetrics();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Test
  public void writeThenReadAll() {
    TextIO.TypedWrite<String, Object> write =
        TextIO.write().to(filenamePrefix).withOutputFilenames().withCompression(compressionType);
    if (numShards != null) {
      write = write.withNumShards(numShards);
    }

    PCollection<String> testFilenames =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
            .apply(
                "Produce text lines",
                ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
            .apply(
                "Collect write start time",
                ParDo.of(new TimeMonitor<>(FILEIOIT_NAMESPACE, "startTime")))
            .apply("Write content to files", write)
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(
                "Collect write end time",
                ParDo.of(new TimeMonitor<>(FILEIOIT_NAMESPACE, "middleTime")));

    PCollection<String> consolidatedHashcode =
        testFilenames
            .apply("Match all files", FileIO.matchAll())
            .apply(
                "Read matches",
                FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
            .apply("Read files", TextIO.readFiles())
            .apply(
                "Collect read end time", ParDo.of(new TimeMonitor<>(FILEIOIT_NAMESPACE, "endTime")))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFilenames.apply(
        "Delete test files",
        ParDo.of(new DeleteFileFn())
            .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    PipelineResult result = pipeline.run();
    PipelineResult.State pipelineState = result.waitUntilFinish();
    assertEquals(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        Lineage.query(result.metrics(), Lineage.Type.SINK));

    collectAndPublishMetrics(result);
    // Fail the test if pipeline failed.
    assertNotEquals(pipelineState, PipelineResult.State.FAILED);
  }

  private void collectAndPublishMetrics(PipelineResult result) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Instant.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        fillMetricSuppliers(uuid, timestamp);

    final IOITMetrics metrics =
        new IOITMetrics(metricSuppliers, result, FILEIOIT_NAMESPACE, uuid, timestamp);
    metrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> fillMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers = new HashSet<>();

    metricSuppliers.add(
        (reader) -> {
          long writeStartTime = reader.getStartTimeMetric("startTime");
          long writeEndTime = reader.getEndTimeMetric("middleTime");
          double writeTime = (writeEndTime - writeStartTime) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "write_time", writeTime);
        });

    metricSuppliers.add(
        (reader) -> {
          long readStartTime = reader.getStartTimeMetric("middleTime");
          long readEndTime = reader.getEndTimeMetric("endTime");
          double readTime = (readEndTime - readStartTime) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "read_time", readTime);
        });

    metricSuppliers.add(
        (reader) -> {
          long writeStartTime = reader.getStartTimeMetric("startTime");
          long readEndTime = reader.getEndTimeMetric("endTime");
          double runTime = (readEndTime - writeStartTime) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "run_time", runTime);
        });
    if (datasetSize != null) {
      metricSuppliers.add(
          (ignored) -> NamedTestResult.create(uuid, timestamp, "dataset_size", datasetSize));
    }
    if (gatherGcsPerformanceMetrics) {
      metricSuppliers.add(
          reader -> {
            MetricsReader actualReader =
                reader.withNamespace("org.apache.beam.sdk.extensions.gcp.storage.GcsFileSystem");
            long numRenames = actualReader.getCounterMetric("num_renames");
            long renameTimeMsec = actualReader.getCounterMetric("rename_time_msec");
            double remamePerSec =
                (numRenames < 0 || renameTimeMsec < 0) ? -1 : numRenames / (renameTimeMsec / 1e3);
            return NamedTestResult.create(uuid, timestamp, "rename_per_sec", remamePerSec);
          });
    }
    return metricSuppliers;
  }
}
