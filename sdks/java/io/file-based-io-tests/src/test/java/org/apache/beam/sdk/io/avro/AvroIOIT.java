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
package org.apache.beam.sdk.io.avro;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;

import com.google.cloud.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
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
import org.apache.beam.sdk.transforms.DoFn;
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
 * An integration test for {@link AvroIO}.
 *
 * <p>Run this test using the command below. Pass in connection information via PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/file-based-io-tests
 *  -DintegrationTestPipelineOptions='[
 *  "--numberOfRecords=100000",
 *  "--datasetSize=12345",
 *  "--expectedHash=99f23ab",
 *  "--filenamePrefix=output_file_path"
 *  ]'
 *  --tests org.apache.beam.sdk.io.avro.AvroIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class AvroIOIT {

  private static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitavro\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvroLine\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"row\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private static String filenamePrefix;
  private static String bigQueryDataset;
  private static String bigQueryTable;
  private static final String AVRO_NAMESPACE = AvroIOIT.class.getName();
  private static Integer numberOfTextLines;
  private static Integer datasetSize;
  private static String expectedHash;
  private static InfluxDBSettings settings;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();

    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
    datasetSize = options.getDatasetSize();
    expectedHash = options.getExpectedHash();
    numberOfTextLines = options.getNumberOfRecords();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Test
  public void writeThenReadAll() {

    PCollection<String> testFilenames =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
            .apply(
                "Produce text lines",
                ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
            .apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
            .setCoder(AvroCoder.of(AVRO_SCHEMA))
            .apply("Collect start time", ParDo.of(new TimeMonitor<>(AVRO_NAMESPACE, "writeStart")))
            .apply(
                "Write Avro records to files",
                AvroIO.writeGenericRecords(AVRO_SCHEMA)
                    .to(filenamePrefix)
                    .withOutputFilenames()
                    .withSuffix(".avro"))
            .getPerDestinationOutputFilenames()
            .apply(
                "Collect middle time", ParDo.of(new TimeMonitor<>(AVRO_NAMESPACE, "middlePoint")))
            .apply(Values.create());

    PCollection<String> consolidatedHashcode =
        testFilenames
            .apply("Match all files", FileIO.matchAll())
            .apply(
                "Read matches",
                FileIO.readMatches().withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
            .apply("Read files", AvroIO.readFilesGenericRecords(AVRO_SCHEMA))
            .apply("Collect end time", ParDo.of(new TimeMonitor<>(AVRO_NAMESPACE, "endPoint")))
            .apply("Parse Avro records to Strings", ParDo.of(new ParseAvroRecordsFn()))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));
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
    String timestamp = Timestamp.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers =
        fillMetricSuppliers(uuid, timestamp);
    final IOITMetrics metrics =
        new IOITMetrics(metricSuppliers, result, AVRO_NAMESPACE, uuid, timestamp);
    metrics.publish(bigQueryDataset, bigQueryTable);
    metrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> fillMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();

    suppliers.add(
        (reader) -> {
          long writeStart = reader.getStartTimeMetric("writeStart");
          long writeEnd = reader.getEndTimeMetric("middlePoint");
          double writeTime = (writeEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "write_time", writeTime);
        });

    suppliers.add(
        (reader) -> {
          long readEnd = reader.getEndTimeMetric("endPoint");
          long readStart = reader.getStartTimeMetric("middlePoint");
          double readTime = (readEnd - readStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "read_time", readTime);
        });

    suppliers.add(
        (reader) -> {
          long readEnd = reader.getEndTimeMetric("endPoint");
          long writeStart = reader.getStartTimeMetric("writeStart");
          double runTime = (readEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "run_time", runTime);
        });
    if (datasetSize != null) {
      suppliers.add(
          (reader) -> NamedTestResult.create(uuid, timestamp, "dataset_size", datasetSize));
    }
    return suppliers;
  }

  private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new GenericRecordBuilder(AVRO_SCHEMA).set("row", c.element()).build());
    }
  }

  private static class ParseAvroRecordsFn extends DoFn<GenericRecord, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.valueOf(c.element().get("row")));
    }
  }
}
