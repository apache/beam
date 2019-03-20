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

import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.getExpectedHashForLineCount;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;

import com.google.cloud.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOITMetrics;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.TextIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/file-based-io-tests
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
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
  private static final Logger LOG = LoggerFactory.getLogger(TextIOIT.class);

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Compression compressionType;
  private static Integer numShards;
  private static String bigQueryDataset;
  private static String bigQueryTable;
  private static boolean gatherGcsPerformanceMetrics;
  private static final String FILEIOIT_NAMESPACE = TextIOIT.class.getName();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();

    numberOfTextLines = options.getNumberOfRecords();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    compressionType = Compression.valueOf(options.getCompressionType());
    numShards = options.getNumberOfShards();
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
    gatherGcsPerformanceMetrics = options.getReportGcsPerformanceMetrics();
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
            .apply("Read all files", TextIO.readAll().withCompression(AUTO))
            .apply(
                "Collect read end time", ParDo.of(new TimeMonitor<>(FILEIOIT_NAMESPACE, "endTime")))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    String expectedHash = getExpectedHashForLineCount(numberOfTextLines);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFilenames.apply(
        "Delete test files",
        ParDo.of(new DeleteFileFn())
            .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    collectAndPublishMetrics(result);
  }

  private void collectAndPublishMetrics(PipelineResult result) {
    String uuid = UUID.randomUUID().toString();
    Timestamp timestamp = Timestamp.now();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        fillMetricSuppliers(uuid, timestamp.toString());

    new IOITMetrics(metricSuppliers, result, FILEIOIT_NAMESPACE, uuid, timestamp.toString())
        .publish(bigQueryDataset, bigQueryTable);
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

    if (gatherGcsPerformanceMetrics) {
      metricSuppliers.add(
          reader -> {
            MetricsReader actualReader =
                reader.withNamespace("org.apache.beam.sdk.extensions.gcp.storage.GcsFileSystem");
            long numCopies = actualReader.getCounterMetric("num_copies");
            long copyTimeMsec = actualReader.getCounterMetric("copy_time_msec");
            double copiesPerSec =
                (numCopies < 0 || copyTimeMsec < 0) ? -1 : numCopies / (copyTimeMsec / 1e3);
            return NamedTestResult.create(uuid, timestamp, "copies_per_sec", copiesPerSec);
          });
    }
    return metricSuppliers;
  }
}
