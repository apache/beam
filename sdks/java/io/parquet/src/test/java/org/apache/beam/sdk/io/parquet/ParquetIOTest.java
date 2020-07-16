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
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
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
          + "    {\"name\":\"name\",\"type\":\"string\"}"
          + "  ]"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  private static final String[] SCIENTISTS =
      new String[] {
        "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
        "Faraday", "Newton", "Bohr", "Galilei", "Maxwell"
      };

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
      GenericRecord record = builder.set("name", SCIENTISTS[index]).build();
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
}
