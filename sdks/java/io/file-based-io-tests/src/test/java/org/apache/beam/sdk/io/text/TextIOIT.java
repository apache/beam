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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper.DeleteFileFn;
import org.apache.beam.sdk.io.common.FileBasedIOTestPipelineOptions;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.metrics.TimeMonitor;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.testutils.publishing.ConsoleResultPublisher;
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
    gatherAndPublishMetrics(result);
  }

  private void gatherAndPublishMetrics(PipelineResult result) {
    String uuid = UUID.randomUUID().toString();
    Timestamp timestamp = Timestamp.now();
    List<NamedTestResult> namedTestResults = readMetrics(result, uuid, timestamp);
    if (bigQueryDataset != null && bigQueryTable != null) {
      BigQueryResultsPublisher.create(bigQueryDataset, NamedTestResult.getSchema())
          .publish(namedTestResults, bigQueryTable);
    }
    ConsoleResultPublisher.publish(namedTestResults, uuid, timestamp.toString());
  }

  private List<NamedTestResult> readMetrics(
      PipelineResult result, String uuid, Timestamp timestamp) {
    List<NamedTestResult> results = new ArrayList<>();

    MetricsReader reader = new MetricsReader(result, FILEIOIT_NAMESPACE);
    long writeStartTime = reader.getStartTimeMetric("startTime");
    long writeEndTime = reader.getEndTimeMetric("middleTime");
    long readStartTime = reader.getStartTimeMetric("middleTime");
    long readEndTime = reader.getEndTimeMetric("endTime");
    double writeTime = (writeEndTime - writeStartTime) / 1e3;
    double readTime = (readEndTime - readStartTime) / 1e3;
    double runTime = (readEndTime - writeStartTime) / 1e3;
    if (gatherGcsPerformanceMetrics) {
      double copiesPerSec = calculateGcsCopies(result);

      if (copiesPerSec > 0) {
        results.add(
            NamedTestResult.create(uuid, timestamp.toString(), "copies_per_sec", copiesPerSec));
      }
    }
    results.add(NamedTestResult.create(uuid, timestamp.toString(), "read_time", readTime));
    results.add(NamedTestResult.create(uuid, timestamp.toString(), "write_time", writeTime));
    results.add(NamedTestResult.create(uuid, timestamp.toString(), "run_time", runTime));

    return results;
  }

  private double calculateGcsCopies(PipelineResult result) {
    MetricsReader metricsReader =
        new MetricsReader(result, "org.apache.beam.sdk.extensions.gcp.storage.GcsFileSystem");
    long numCopies = metricsReader.getCounterMetric("num_copies");
    long copyTimeMsec = metricsReader.getCounterMetric("copy_time_msec");
    if (numCopies < 0 || copyTimeMsec < 0) {
      return -1;
    }
    return numCopies / (copyTimeMsec / 1e3);
  }
}
