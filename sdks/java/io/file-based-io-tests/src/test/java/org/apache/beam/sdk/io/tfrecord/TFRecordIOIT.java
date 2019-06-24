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
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.getExpectedHashForLineCount;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;

import com.google.cloud.Timestamp;
import java.nio.charset.StandardCharsets;
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

  private static String filenamePrefix;
  private static Integer numberOfTextLines;
  private static Compression compressionType;
  private static String bigQueryDataset;
  private static String bigQueryTable;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();

    numberOfTextLines = options.getNumberOfRecords();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    compressionType = Compression.valueOf(options.getCompressionType());
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
  }

  private static String createFilenamePattern() {
    return filenamePrefix + "*";
  }

  // TODO: There are two pipelines due to: https://issues.apache.org/jira/browse/BEAM-3267
  @Test
  public void writeThenReadAll() {
    TFRecordIO.Write writeTransform =
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
            ParDo.of(new TimeMonitor<>(TFRECORD_NAMESPACE, "writeTime")))
        .apply("Write content to files", writeTransform);

    writePipeline.run().waitUntilFinish();

    String filenamePattern = createFilenamePattern();
    PCollection<String> consolidatedHashcode =
        readPipeline
            .apply(TFRecordIO.read().from(filenamePattern).withCompression(AUTO))
            .apply(
                "Record time after reading",
                ParDo.of(new TimeMonitor<>(TFRECORD_NAMESPACE, "readTime")))
            .apply("Transform bytes to strings", MapElements.via(new ByteArrayToString()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()))
            .apply(Reshuffle.viaRandomKey());

    String expectedHash = getExpectedHashForLineCount(numberOfTextLines);
    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    readPipeline
        .apply(Create.of(filenamePattern))
        .apply(
            "Delete test files",
            ParDo.of(new DeleteFileFn())
                .withSideInputs(consolidatedHashcode.apply(View.asSingleton())));
    PipelineResult result = readPipeline.run();
    result.waitUntilFinish();
    collectAndPublishMetrics(result);
  }

  private void collectAndPublishMetrics(PipelineResult result) {
    String uuid = UUID.randomUUID().toString();
    String timestamp = Timestamp.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        fillMetricSuppliers(uuid, timestamp);
    new IOITMetrics(metricSuppliers, result, TFRECORD_NAMESPACE, uuid, timestamp)
        .publish(bigQueryDataset, bigQueryTable);
  }

  private Set<Function<MetricsReader, NamedTestResult>> fillMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeTime");
          long writeEnd = reader.getEndTimeMetric("writeTime");
          double writeTime = (writeEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "write_time", writeTime);
        });

    suppliers.add(
        reader -> {
          long readStart = reader.getStartTimeMetric("readTime");
          long readEnd = reader.getEndTimeMetric("readTime");
          double readTime = (readEnd - readStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "read_time", readTime);
        });

    suppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeTime");
          long readEnd = reader.getEndTimeMetric("readTime");
          double runTime = (readEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "run_time", runTime);
        });

    return suppliers;
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
