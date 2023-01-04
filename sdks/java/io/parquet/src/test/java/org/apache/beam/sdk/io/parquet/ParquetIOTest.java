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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO.GenericRecordPassthroughFn;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test on the {@link ParquetIO}. */
@RunWith(JUnit4.class)
public class ParquetIOTest implements Serializable {
  @Rule public transient TestPipeline mainPipeline = TestPipeline.create();

  @Rule public transient TestPipeline readPipeline = TestPipeline.create();

  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SCHEMA_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":\"string\"},"
          + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String REQUESTED_SCHEMA_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final String REQUESTED_SCHEMA_ENCODER_STRING =
      "{"
          + "\"type\":\"record\", "
          + "\"name\":\"testrecord\","
          + "\"fields\":["
          + "    {\"name\":\"name\",\"type\":[\"string\",\"null\"]},"
          + "    {\"name\":\"id\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final Schema REQUESTED_ENCODER_SCHEMA =
      new Schema.Parser().parse(REQUESTED_SCHEMA_ENCODER_STRING);
  private static final Schema REQUESTED_SCHEMA = new Schema.Parser().parse(REQUESTED_SCHEMA_STRING);
  private static final String[] SCIENTISTS =
      new String[] {
        "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
        "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
      };

  @Test
  public void testWriteAndReadWithProjection() {
    List<GenericRecord> requestRecords = generateRequestedRecords(1000);
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(SCHEMA)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                .withProjection(REQUESTED_SCHEMA, REQUESTED_ENCODER_SCHEMA));
    PAssert.that(readBack).containsInAnyOrder(requestRecords);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testBlockTracker() {
    OffsetRange range = new OffsetRange(0, 1);
    ParquetIO.ReadFiles.BlockTracker tracker = new ParquetIO.ReadFiles.BlockTracker(range, 7, 3);
    assertEquals(tracker.getProgress().getWorkRemaining(), 1.0, 0.01);
    assertEquals(tracker.getProgress().getWorkCompleted(), 0.0, 0.01);
    tracker.tryClaim(0L);
    tracker.tryClaim(1L);
    assertEquals(tracker.getProgress().getWorkRemaining(), 0.0, 0.01);
    assertEquals(tracker.getProgress().getWorkCompleted(), 1.0, 0.01);
  }

  @Test
  public void testSplitBlockWithLimit() {
    ParquetIO.ReadFiles.SplitReadFn<GenericRecord> testFn =
        new ParquetIO.ReadFiles.SplitReadFn<>(
            null, null, ParquetIO.GenericRecordPassthroughFn.create(), null);
    ArrayList<BlockMetaData> blockList = new ArrayList<>();
    ArrayList<OffsetRange> rangeList;
    BlockMetaData testBlock = mock(BlockMetaData.class);
    when(testBlock.getTotalByteSize()).thenReturn((long) 60);
    rangeList = testFn.splitBlockWithLimit(0, blockList.size(), blockList, 200);
    assertTrue(rangeList.isEmpty());
    for (int i = 0; i < 6; i++) {
      blockList.add(testBlock);
    }
    rangeList = testFn.splitBlockWithLimit(1, blockList.size(), blockList, 200);
    assertEquals(1L, rangeList.get(0).getFrom());
    assertEquals(5L, rangeList.get(0).getTo());
    assertEquals(5L, rangeList.get(1).getFrom());
    assertEquals(6L, rangeList.get(1).getTo());
    assertEquals(2L, rangeList.size());
  }

  @Test
  public void testWriteAndRead() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    ParquetIO.Read read = ParquetIO.read(SCHEMA);

    PCollection<GenericRecord> readBack =
        readPipeline.apply(read.from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));
    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteWithRowGroupSizeAndRead() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA).withRowGroupSize(1500))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();
    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(SCHEMA).from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));
    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithBeamSchema() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));

    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBackRecords =
        readPipeline.apply(
            ParquetIO.read(SCHEMA)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                .withBeamSchemas(true));

    PAssert.that(readBackRecords).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFilesAsJsonForUnknownSchema() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<String> readBackAsJson =
        readPipeline.apply(
            ParquetIO.parseGenericRecords(ParseGenericRecordAsJsonFn.create())
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBackAsJson).containsInAnyOrder(convertRecordsToJson(records));
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);

    ParquetIO.ReadFiles readFiles = ParquetIO.readFiles(SCHEMA);

    PCollection<GenericRecord> writeThenRead =
        mainPipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(SCHEMA))
                    .to(temporaryFolder.getRoot().getAbsolutePath()))
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(readFiles);

    PAssert.that(writeThenRead).containsInAnyOrder(records);

    mainPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadFilesAsJsonForUnknownSchemaFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);
    List<String> expectedJsonRecords = convertRecordsToJson(records);

    ParquetIO.ParseFiles<String> parseFiles =
        ParquetIO.parseFilesGenericRecords(ParseGenericRecordAsJsonFn.create());

    PCollection<String> writeThenRead =
        mainPipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(SCHEMA))
                    .to(temporaryFolder.getRoot().getAbsolutePath()))
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(parseFiles);

    assertEquals(1000, expectedJsonRecords.size());
    PAssert.that(writeThenRead).containsInAnyOrder(expectedJsonRecords);

    mainPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadFilesAsRowForUnknownSchemaFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);
    List<Row> expectedRows =
        records.stream().map(record -> AvroUtils.toBeamRowStrict(record, null)).collect(toList());

    PCollection<Row> writeThenRead =
        mainPipeline
            .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
            .apply(
                FileIO.<GenericRecord>write()
                    .via(ParquetIO.sink(SCHEMA))
                    .to(temporaryFolder.getRoot().getAbsolutePath()))
            .getPerDestinationOutputFilenames()
            .apply(Values.create())
            .apply(FileIO.matchAll())
            .apply(FileIO.readMatches())
            .apply(
                ParquetIO.parseFilesGenericRecords(
                        (SerializableFunction<GenericRecord, Row>)
                            record -> AvroUtils.toBeamRowStrict(record, null))
                    .withCoder(SchemaCoder.of(AvroUtils.toBeamSchema(SCHEMA))));

    PAssert.that(writeThenRead).containsInAnyOrder(expectedRows);

    mainPipeline.run().waitUntilFinish();
  }

  @Test
  @SuppressWarnings({"nullable", "ConstantConditions"} /* forced check. */)
  public void testReadFilesUnknownSchemaFilesForGenericRecordThrowException() {
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ParquetIO.parseFilesGenericRecords(GenericRecordPassthroughFn.create())
                    .expand(null));

    assertEquals(
        "Parse can't be used for reading as GenericRecord.", illegalArgumentException.getMessage());
  }

  private List<GenericRecord> generateGenericRecords(long count) {
    List<GenericRecord> data = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(SCHEMA);
    for (int i = 0; i < count; i++) {
      int index = i % SCIENTISTS.length;
      GenericRecord record =
          builder.set("name", SCIENTISTS[index]).set("id", Integer.toString(i)).build();
      data.add(record);
    }
    return data;
  }

  private List<GenericRecord> generateRequestedRecords(long count) {
    ArrayList<GenericRecord> data = new ArrayList<>();
    GenericRecordBuilder builder = new GenericRecordBuilder(REQUESTED_ENCODER_SCHEMA);
    for (int i = 0; i < count; i++) {
      GenericRecord record = builder.set("id", Integer.toString(i)).set("name", null).build();
      data.add(record);
    }
    return data;
  }

  @Test
  public void testReadDisplayData() {
    Configuration configuration = new Configuration();
    configuration.set("parquet.foo", "foo");
    DisplayData displayData =
        DisplayData.from(
            ParquetIO.read(SCHEMA)
                .from("foo.parquet")
                .withProjection(REQUESTED_SCHEMA, SCHEMA)
                .withAvroDataModel(GenericData.get())
                .withConfiguration(configuration));

    assertThat(displayData, hasDisplayItem("filePattern", "foo.parquet"));
    assertThat(displayData, hasDisplayItem("schema", SCHEMA.toString()));
    assertThat(displayData, hasDisplayItem("inferBeamSchema", false));
    assertThat(displayData, hasDisplayItem("projectionSchema", REQUESTED_SCHEMA.toString()));
    assertThat(displayData, hasDisplayItem("avroDataModel", GenericData.get().toString()));
    assertThat(displayData, hasDisplayItem("parquet.foo", "foo"));
  }

  public static class TestRecord {
    String name;

    public TestRecord(String name) {
      this.name = name;
    }
  }

  @Test(expected = org.apache.beam.sdk.Pipeline.PipelineExecutionException.class)
  public void testWriteAndReadUsingReflectDataSchemaWithoutDataModelThrowsException() {
    Schema testRecordSchema = ReflectData.get().getSchema(TestRecord.class);

    List<GenericRecord> records = generateGenericRecords(1000);
    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(testRecordSchema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(testRecordSchema))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(testRecordSchema)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadUsingReflectDataSchemaWithDataModel() {
    Schema testRecordSchema = ReflectData.get().getSchema(TestRecord.class);

    List<GenericRecord> records = generateGenericRecords(1000);
    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(testRecordSchema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(testRecordSchema))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(testRecordSchema)
                .withAvroDataModel(GenericData.get())
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadUsingGenericDataSchemaWithDataModel() {
    Schema schema = new Schema.Parser().parse(SCHEMA_STRING);

    List<GenericRecord> records = generateGenericRecords(1000);
    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(schema)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(schema).withAvroDataModel(GenericData.get()))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(schema)
                .withAvroDataModel(GenericData.get())
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithConfiguration() {
    List<GenericRecord> records = generateGenericRecords(10);
    List<GenericRecord> expectedRecords = generateGenericRecords(1);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    Configuration configuration = new Configuration();
    FilterPredicate filterPredicate =
        FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromString("0"));
    ParquetInputFormat.setFilterPredicate(configuration, filterPredicate);
    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(SCHEMA)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                .withConfiguration(configuration));
    PAssert.that(readBack).containsInAnyOrder(expectedRecords);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFilesAsJsonForUnknownSchemaWithConfiguration() {
    List<GenericRecord> records = generateGenericRecords(10);
    List<GenericRecord> expectedRecords = generateGenericRecords(1);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    Configuration configuration = new Configuration();
    FilterPredicate filterPredicate =
        FilterApi.eq(FilterApi.binaryColumn("id"), Binary.fromString("0"));
    ParquetInputFormat.setFilterPredicate(configuration, filterPredicate);

    PCollection<String> readBackAsJson =
        readPipeline.apply(
            ParquetIO.parseGenericRecords(ParseGenericRecordAsJsonFn.create())
                .withConfiguration(configuration)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBackAsJson).containsInAnyOrder(convertRecordsToJson(expectedRecords));
    readPipeline.run().waitUntilFinish();
  }

  /** Returns list of JSON representation of GenericRecords. */
  private static List<String> convertRecordsToJson(List<GenericRecord> records) {
    return records.stream().map(ParseGenericRecordAsJsonFn.create()::apply).collect(toList());
  }

  /** Sample Parse function that converts GenericRecord as JSON. for testing. */
  private static class ParseGenericRecordAsJsonFn
      implements SerializableFunction<GenericRecord, String> {

    public static ParseGenericRecordAsJsonFn create() {
      return new ParseGenericRecordAsJsonFn();
    }

    @Override
    public String apply(GenericRecord input) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      try {
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(input.getSchema(), baos, true);
        new GenericDatumWriter<GenericRecord>(input.getSchema()).write(input, jsonEncoder);
        jsonEncoder.flush();
      } catch (IOException ioException) {
        throw new RuntimeException("error converting record to JSON", ioException);
      }
      try {
        return baos.toString("UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
