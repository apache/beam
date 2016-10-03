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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Test on the {@link ParquetIO}.
 */
public class ParquetIOTest implements Serializable {

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    File file = temporaryFolder.newFile("testread.parquet");
    file.delete();
    Path path = new Path(file.toString());

    Schema schema = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"testrecord\","
        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
        .withSchema(schema)
        .build()) {

      String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
          "Newton", "Bohr", "Galilei", "Maxwell"};

      GenericRecordBuilder builder = new GenericRecordBuilder(schema);

      for (int i = 0; i < 1000; i++) {
        int index = i % scientists.length;
        GenericRecord record = builder.set("name", scientists[index]).build();
        writer.write(record);
      }
    }

    PCollection<GenericRecord> output = pipeline.apply(ParquetIO.read().withPath(file.toString())
        .withSchema(schema));

    PAssert.thatSingleton(output.apply("Count All", Count.<GenericRecord>globally()))
        .isEqualTo(1000L);

    PCollection<KV<String, GenericRecord>> mapped =
        output.apply(MapElements.via(
            new SimpleFunction<GenericRecord, KV<String, GenericRecord>>() {
              public KV<String, GenericRecord> apply(GenericRecord record) {
                String name = record.get("name").toString();
                return KV.of(name, record);
              }
            }));

    PAssert.that(mapped.apply("Count Scientist", Count.<String, GenericRecord>perKey()))
        .satisfies(new SerializableFunction<Iterable<KV<String, Long>>, Void>() {
      @Override
      public Void apply(Iterable<KV<String, Long>> input) {
        for (KV<String, Long> element : input) {
          assertEquals(element.getKey(), 100L, element.getValue().longValue());
        }
        return null;
      }
    });

    pipeline.run();
  }

  @Ignore("Direct runner uses com.google.common.cache.LocalCache causing file already exists. "
      + "Investigating.")
  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    File file = temporaryFolder.newFile("testwrite.parquet");
    file.delete();

    Schema schema = new Schema.Parser().parse("{\"type\":\"record\", \"name\":\"testrecord\","
        + "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}");

    ArrayList<GenericRecord> data = new ArrayList<>();
    String[] scientists = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday",
        "Newton", "Bohr", "Galilei", "Maxwell"};

    GenericRecordBuilder builder = new GenericRecordBuilder(schema);

    for (int i = 0; i < 1000; i++) {
      int index = i % scientists.length;
      GenericRecord record = builder.set("name", scientists[index]).build();
      data.add(record);
    }

    pipeline.apply(Create.of(data).withCoder(AvroCoder.of(schema)))
        .apply(ParquetIO.write().withPath(file.toString()).withSchema(schema.toString()));

    pipeline.run();

    Path path = new Path(file.toString());

    int count = 0;
    ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path).build();
    GenericRecord record;
    while ((record = reader.read()) != null) {
      System.out.println(record.get("name"));
      count++;
    }

    assertEquals(1000, count);
  }

}
