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
package org.apache.beam.sdk.io.parquet;

import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.appendTimestampSuffix;
import static org.apache.beam.sdk.io.common.FileBasedIOITHelper.readFileBasedIOITPipelineOptions;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import com.google.cloud.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.FileBasedIOITHelper;
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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Integration tests for {@link org.apache.beam.sdk.io.parquet.ParquetIO}.
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
 *  ]'
 *  --tests org.apache.beam.sdk.io.parquet.ParquetIOIT
 *  -DintegrationTestRunner=direct
 * </pre>
 *
 * <p>Please see 'build_rules.gradle' file for instructions regarding running this test using Beam
 * performance testing framework.
 */
@RunWith(JUnit4.class)
public class ParquetIOIT {

  private static final Schema SCHEMA =
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
  private static Integer numberOfTextLines;
  private static Integer datasetSize;
  private static String expectedHash;
  private static InfluxDBSettings settings;

  @Rule public TestPipeline pipeline = TestPipeline.create();
  private static final String PARQUET_NAMESPACE = ParquetIOIT.class.getName();

  @BeforeClass
  public static void setup() {
    FileBasedIOTestPipelineOptions options = readFileBasedIOITPipelineOptions();
    numberOfTextLines = options.getNumberOfRecords();
    datasetSize = options.getDatasetSize();
    expectedHash = options.getExpectedHash();
    filenamePrefix = appendTimestampSuffix(options.getFilenamePrefix());
    bigQueryDataset = options.getBigQueryDataset();
    bigQueryTable = options.getBigQueryTable();
    settings =
        InfluxDBSettings.builder()
            .withHost(options.getInfluxHost())
            .withDatabase(options.getInfluxDatabase())
            .withMeasurement(options.getInfluxMeasurement())
            .get();
  }

  @Test
  public void writeThenReadAll() {
    PCollection<String> testFiles =
        pipeline
            .apply("Generate sequence", GenerateSequence.from(0).to(numberOfTextLines))
            .apply(
                "Produce text lines",
                ParDo.of(new FileBasedIOITHelper.DeterministicallyConstructTestTextLineFn()))
            .apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
            .setCoder(AvroCoder.of(SCHEMA))
            .apply(
                "Gather write start times",
                ParDo.of(new TimeMonitor<>(PARQUET_NAMESPACE, "writeStart")))
            .apply(
                "Write Parquet files",
                FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(filenamePrefix))
            .getPerDestinationOutputFilenames()
            .apply(
                "Gather write end times",
                ParDo.of(new TimeMonitor<>(PARQUET_NAMESPACE, "writeEnd")))
            .apply("Get file names", Values.create());

    PCollection<String> consolidatedHashcode =
        testFiles
            .apply("Find files", FileIO.matchAll())
            .apply("Read matched files", FileIO.readMatches())
            .apply(
                "Gather read start time",
                ParDo.of(new TimeMonitor<>(PARQUET_NAMESPACE, "readStart")))
            .apply("Read parquet files", ParquetIO.readFiles(SCHEMA))
            .apply(
                "Gather read end time", ParDo.of(new TimeMonitor<>(PARQUET_NAMESPACE, "readEnd")))
            .apply(
                "Map records to strings",
                MapElements.into(strings())
                    .via(
                        (SerializableFunction<GenericRecord, String>)
                            record -> String.valueOf(record.get("row"))))
            .apply("Calculate hashcode", Combine.globally(new HashingFn()));

    PAssert.thatSingleton(consolidatedHashcode).isEqualTo(expectedHash);

    testFiles.apply(
        "Delete test files",
        ParDo.of(new FileBasedIOITHelper.DeleteFileFn())
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
        new IOITMetrics(metricSuppliers, result, PARQUET_NAMESPACE, uuid, timestamp);
    metrics.publish(bigQueryDataset, bigQueryTable);
    metrics.publishToInflux(settings);
  }

  private Set<Function<MetricsReader, NamedTestResult>> fillMetricSuppliers(
      String uuid, String timestamp) {
    Set<Function<MetricsReader, NamedTestResult>> metricSuppliers = new HashSet<>();
    metricSuppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeStart");
          long writeEnd = reader.getEndTimeMetric("writeEnd");
          double writeTime = (writeEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "write_time", writeTime);
        });

    metricSuppliers.add(
        reader -> {
          long readStart = reader.getStartTimeMetric("readStart");
          long readEnd = reader.getEndTimeMetric("readEnd");
          double readTime = (readEnd - readStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "read_time", readTime);
        });

    metricSuppliers.add(
        reader -> {
          long writeStart = reader.getStartTimeMetric("writeStart");
          long readEnd = reader.getEndTimeMetric("readEnd");
          double runTime = (readEnd - writeStart) / 1e3;
          return NamedTestResult.create(uuid, timestamp, "run_time", runTime);
        });
    if (datasetSize != null) {
      metricSuppliers.add(
          (ignored) -> NamedTestResult.create(uuid, timestamp, "dataset_size", datasetSize));
    }
    return metricSuppliers;
  }

  private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new GenericRecordBuilder(SCHEMA).set("row", c.element()).build());
    }
  }
}
