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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.Assert;
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
  public void testBlockTracker() throws Exception {
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
    ParquetIO.ReadFiles.SplitReadFn testFn = new ParquetIO.ReadFiles.SplitReadFn(null, null);
    ArrayList<BlockMetaData> blockList = new ArrayList<BlockMetaData>();
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

    PCollection<GenericRecord> readBack =
        readPipeline.apply(
            ParquetIO.read(SCHEMA).from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));
    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadWithSplit() {
    List<GenericRecord> records = generateGenericRecords(1000);

    mainPipeline
        .apply(Create.of(records).withCoder(AvroCoder.of(SCHEMA)))
        .apply(
            FileIO.<GenericRecord>write()
                .via(ParquetIO.sink(SCHEMA))
                .to(temporaryFolder.getRoot().getAbsolutePath()));
    mainPipeline.run().waitUntilFinish();

    PCollection<GenericRecord> readBackWithSplit =
        readPipeline.apply(
            ParquetIO.read(SCHEMA)
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*")
                .withSplit());
    PAssert.that(readBackWithSplit).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteAndReadFiles() {
    List<GenericRecord> records = generateGenericRecords(1000);

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
            .apply(ParquetIO.readFiles(SCHEMA));

    PAssert.that(writeThenRead).containsInAnyOrder(records);

    mainPipeline.run().waitUntilFinish();
  }

  private List<GenericRecord> generateGenericRecords(long count) {
    ArrayList<GenericRecord> data = new ArrayList<>();
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
      int index = i % SCIENTISTS.length;
      GenericRecord record = builder.set("id", Integer.toString(i)).set("name", null).build();
      data.add(record);
    }
    return data;
  }

  @Test
  public void testReadDisplayData() {
    DisplayData displayData = DisplayData.from(ParquetIO.read(SCHEMA).from("foo.parquet"));

    Assert.assertThat(displayData, hasDisplayItem("filePattern", "foo.parquet"));
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

  @Test(expected = org.apache.beam.sdk.Pipeline.PipelineExecutionException.class)
  public void testWriteAndReadWithSplitUsingReflectDataSchemaWithoutDataModelThrowsException() {
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
                .withSplit()
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
  public void testWriteAndReadwithSplitUsingReflectDataSchemaWithDataModel() {
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
                .withSplit()
                .withAvroDataModel(GenericData.get())
                .from(temporaryFolder.getRoot().getAbsolutePath() + "/*"));

    PAssert.that(readBack).containsInAnyOrder(records);
    readPipeline.run().waitUntilFinish();
  }
}
