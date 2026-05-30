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
package org.apache.beam.sdk.io.delta;

import static org.junit.Assert.assertNotEquals;

import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.IOITHelper;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MimeTypes;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.parquet.ParquetIO;
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
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Performance and Integration tests for {@link DeltaIO}.
 *
 * <p>Run this test using the command below:
 *
 * <pre>
 *  ./gradlew :sdks:java:io:delta:test --tests org.apache.beam.sdk.io.delta.DeltaIOIT
 * </pre>
 */
@RunWith(JUnit4.class)
public class DeltaIOIT implements Serializable {

  private static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitdelta\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestRowRecord\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                  + "     {\"name\": \"name\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private static final Schema PARTITIONED_AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitdelta\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestRowRecord\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"id\", \"type\": \"int\"},\n"
                  + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                  + "     {\"name\": \"part\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private static final String NAMESPACE = DeltaIOIT.class.getName();

  private static String tablePathPrefix;
  private static InfluxDBSettings settings;

  @Rule public transient TestPipeline pipelineWrite = TestPipeline.create();
  @Rule public transient TestPipeline pipelineRead = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    DeltaIOTestPipelineOptions options = null;
    try {
      options = IOITHelper.readIOTestPipelineOptions(DeltaIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      // In local environments, fall back to target directory if not provided
    }
    if (options != null) {
      tablePathPrefix = options.getTablePath();
      settings =
          InfluxDBSettings.builder()
              .withHost(options.getInfluxHost())
              .withDatabase(options.getInfluxDatabase())
              .withMeasurement(options.getInfluxMeasurement())
              .get();
    } else {
      tablePathPrefix = "target/temp-delta-table";
      settings = null;
    }
  }

  @Test
  public void testReadSmall() throws Exception {
    runIntegrationTest(1000, false, "small");
  }

  @Test
  public void testReadLarge() throws Exception {
    runIntegrationTest(100000, false, "large");
  }

  @Test
  public void testReadPartitioned() throws Exception {
    runIntegrationTest(1000, true, "partitioned");
  }

  private void runIntegrationTest(int numRecords, boolean isPartitioned, String scenarioName) throws Exception {
    String tablePath = appendTimestampSuffix(tablePathPrefix + "-" + scenarioName);
    try {
      // 1. Write Parquet files using Beam
      writeParquetFiles(tablePath, numRecords, isPartitioned);

      // 2. Generate Delta Log
      generateDeltaLog(tablePath, isPartitioned);

      // 3. Read Delta Table and assert
      readAndVerify(tablePath, numRecords, isPartitioned, scenarioName);

    } finally {
      cleanUp(tablePath);
    }
  }

  private static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, java.time.Instant.now().toEpochMilli());
  }

  private void writeParquetFiles(String tablePath, int numRecords, boolean isPartitioned) {
    if (isPartitioned) {
      pipelineWrite
          .apply("Generate sequence", GenerateSequence.from(0).to(numRecords))
          .apply("Construct TestRows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
          .apply("Convert to Partitioned Avro", ParDo.of(new DoFn<TestRow, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              TestRow row = c.element();
              String part = (row.id() % 2 == 0) ? "even" : "odd";
              c.output(new GenericRecordBuilder(PARTITIONED_AVRO_SCHEMA)
                  .set("id", row.id())
                  .set("name", row.name())
                  .set("part", part)
                  .build());
            }
          }))
          .setCoder(AvroCoder.of(PARTITIONED_AVRO_SCHEMA))
          .apply("Write Partitioned Parquet", FileIO.<String, GenericRecord>writeDynamic()
              .by(record -> "part=" + record.get("part"))
              .via(ParquetIO.sink(PARTITIONED_AVRO_SCHEMA))
              .to(tablePath)
              .withNaming(key -> DefaultFilenamePolicy.fromStandardNaming(
                  key + "/part", null, ".parquet", false)));
    } else {
      pipelineWrite
          .apply("Generate sequence", GenerateSequence.from(0).to(numRecords))
          .apply("Construct TestRows", ParDo.of(new TestRow.DeterministicallyConstructTestRowFn()))
          .apply("Convert to Avro", ParDo.of(new DoFn<TestRow, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              TestRow row = c.element();
              c.output(new GenericRecordBuilder(AVRO_SCHEMA)
                  .set("id", row.id())
                  .set("name", row.name())
                  .build());
            }
          }))
          .setCoder(AvroCoder.of(AVRO_SCHEMA))
          .apply("Write Parquet files", FileIO.<GenericRecord>write()
              .via(ParquetIO.sink(AVRO_SCHEMA))
              .to(tablePath)
              .withNumShards(2));
    }
    pipelineWrite.run().waitUntilFinish();
  }

  private void generateDeltaLog(String tablePath, boolean isPartitioned) throws Exception {
    List<MatchResult.Metadata> metadataList = FileSystems.match(tablePath + "/**/*.parquet").metadata();
    ResourceId logFileResourceId = FileSystems.matchNewResource(tablePath + "/_delta_log/00000000000000000000.json", false);

    try (PrintWriter writer = new PrintWriter(
        Channels.newWriter(FileSystems.create(logFileResourceId, MimeTypes.TEXT), "UTF-8"))) {
      writer.println("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":1}}");
      if (isPartitioned) {
        writer.println("{\"metaData\":{\"id\":\"test-uuid-partitioned\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"part\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionBy\":[\"part\"],\"configuration\":{},\"createdTime\":1717081200000}}");
      } else {
        writer.println("{\"metaData\":{\"id\":\"test-uuid-nonpartitioned\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\"{\\\"type\\\":\\\"struct\\\",\\\"fields\\\":[{\\\"name\\\":\\\"id\\\",\\\"type\\\":\\\"integer\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}},{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\",\\\"nullable\\\":true,\\\"metadata\\\":{}}]}\",\"partitionBy\":[],\"configuration\":{},\"createdTime\":1717081200000}}");
      }

      for (MatchResult.Metadata metadata : metadataList) {
        String fullPath = metadata.resourceId().toString();
        String tableRoot = FileSystems.matchNewResource(tablePath, true).toString();
        String relativePath = fullPath.substring(tableRoot.length());

        if (relativePath.startsWith("/")) {
          relativePath = relativePath.substring(1);
        }

        String addAction;
        if (isPartitioned) {
          String partValue = relativePath.contains("part=even") ? "even" : "odd";
          addAction = String.format(
              "{\"add\":{\"path\":\"%s\",\"partitionValues\":{\"part\":\"%s\"},\"size\":%d,\"modificationTime\":%d,\"dataChange\":true}}",
              relativePath, partValue, metadata.sizeBytes(), metadata.lastModifiedMillis());
        } else {
          addAction = String.format(
              "{\"add\":{\"path\":\"%s\",\"partitionValues\":{},\"size\":%d,\"modificationTime\":%d,\"dataChange\":true}}",
              relativePath, metadata.sizeBytes(), metadata.lastModifiedMillis());
        }
        writer.println(addAction);
      }
    }
  }

  private void readAndVerify(String tablePath, int numRecords, boolean isPartitioned, String scenarioName) {
    PCollection<org.apache.beam.sdk.values.Row> deltaRows =
        pipelineRead.apply("Read from Delta", DeltaIO.readRows().from(tablePath));

    PCollection<org.apache.beam.sdk.values.Row> monitoredRows =
        deltaRows.apply("TimeMonitor", ParDo.of(new TimeMonitor<>(NAMESPACE, scenarioName + "_read")));

    PCollection<TestRow> namesAndIds;
    if (isPartitioned) {
      namesAndIds = monitoredRows.apply("Convert to TestRow", ParDo.of(new DoFn<org.apache.beam.sdk.values.Row, TestRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          org.apache.beam.sdk.values.Row r = c.element();
          int id = r.getInt32("id");
          String name = r.getString("name");
          String part = r.getString("part");
          String expectedPart = (id % 2 == 0) ? "even" : "odd";
          org.junit.Assert.assertEquals(expectedPart, part);
          c.output(TestRow.create(id, name));
        }
      }));
    } else {
      namesAndIds = monitoredRows.apply("Convert to TestRow", ParDo.of(new DoFn<org.apache.beam.sdk.values.Row, TestRow>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          org.apache.beam.sdk.values.Row r = c.element();
          c.output(TestRow.create(r.getInt32("id"), r.getString("name")));
        }
      }));
    }

    PCollection<String> consolidatedHashcode =
        namesAndIds
            .apply(ParDo.of(new TestRow.SelectNameFn()))
            .apply("Hash row contents", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode)
        .containsInAnyOrder(TestRow.getExpectedHashForRowCount(numRecords));

    PipelineResult result = pipelineRead.run();
    PipelineResult.State pipelineState = result.waitUntilFinish();
    assertNotEquals(PipelineResult.State.FAILED, pipelineState);

    collectAndPublishMetrics(result, scenarioName + "_read", numRecords);
  }

  private void collectAndPublishMetrics(PipelineResult readResult, String metricName, int records) {
    if (settings == null) {
      return;
    }
    String uuid = UUID.randomUUID().toString();
    String timestamp = java.time.Instant.now().toString();

    Set<Function<MetricsReader, NamedTestResult>> suppliers = new HashSet<>();
    suppliers.add(
        reader -> {
          long start = reader.getStartTimeMetric(metricName);
          long end = reader.getEndTimeMetric(metricName);
          double duration = (end - start) / 1e3;
          return NamedTestResult.create(uuid, timestamp, metricName + "_duration_sec", duration);
        });
    suppliers.add(
        reader -> {
          long start = reader.getStartTimeMetric(metricName);
          long end = reader.getEndTimeMetric(metricName);
          double duration = (end - start) / 1e3;
          double throughput = duration > 0 ? records / duration : 0.0;
          return NamedTestResult.create(uuid, timestamp, metricName + "_throughput_ops_sec", throughput);
        });

    IOITMetrics metrics = new IOITMetrics(suppliers, readResult, NAMESPACE, uuid, timestamp);
    metrics.publishToInflux(settings);
  }

  private void cleanUp(String tablePath) {
    try {
      MatchResult matchResult = FileSystems.match(tablePath + "/**");
      List<ResourceId> resourceIds = new ArrayList<>();
      for (MatchResult.Metadata metadata : matchResult.metadata()) {
        resourceIds.add(metadata.resourceId());
      }
      if (!resourceIds.isEmpty()) {
        FileSystems.delete(resourceIds);
      }
      FileSystems.delete(Collections.singletonList(FileSystems.matchNewResource(tablePath, true)));
    } catch (Exception e) {
      // Ignore cleanup failures in test
    }
  }
}
