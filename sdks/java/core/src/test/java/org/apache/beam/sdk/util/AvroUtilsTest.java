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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.util.AvroUtils.AvroMetadata;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.Nullable;
import org.apache.avro.reflect.ReflectData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 * Tests for AvroUtils.
 */
@RunWith(JUnit4.class)
public class AvroUtilsTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final int DEFAULT_RECORD_COUNT = 10000;

  /**
   * Generates an input Avro file containing the given records in the temporary directory and
   * returns the full path of the file.
   */
  @SuppressWarnings("deprecation")  // test of internal functionality
  private <T> String generateTestFile(String filename, List<T> elems, AvroCoder<T> coder,
      String codec) throws IOException {
    File tmpFile = tmpFolder.newFile(filename);
    String path = tmpFile.toString();

    FileOutputStream os = new FileOutputStream(tmpFile);
    DatumWriter<T> datumWriter = coder.createDatumWriter();
    try (DataFileWriter<T> writer = new DataFileWriter<>(datumWriter)) {
      writer.setCodec(CodecFactory.fromString(codec));
      writer.create(coder.getSchema(), os);
      for (T elem : elems) {
        writer.append(elem);
      }
    }
    return path;
  }

  @Test
  public void testReadMetadataWithCodecs() throws Exception {
    // Test reading files generated using all codecs.
    String codecs[] = {DataFileConstants.NULL_CODEC, DataFileConstants.BZIP2_CODEC,
        DataFileConstants.DEFLATE_CODEC, DataFileConstants.SNAPPY_CODEC,
        DataFileConstants.XZ_CODEC};
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);

    for (String codec : codecs) {
      String filename = generateTestFile(
          codec, expected, AvroCoder.of(Bird.class), codec);
      AvroMetadata metadata = AvroUtils.readMetadataFromFile(filename);
      assertEquals(codec, metadata.getCodec());
    }
  }

  @Test
  public void testReadSchemaString() throws Exception {
    List<Bird> expected = createRandomRecords(DEFAULT_RECORD_COUNT);
    String codec = DataFileConstants.NULL_CODEC;
    String filename = generateTestFile(
        codec, expected, AvroCoder.of(Bird.class), codec);
    AvroMetadata metadata = AvroUtils.readMetadataFromFile(filename);
    // By default, parse validates the schema, which is what we want.
    Schema schema = new Schema.Parser().parse(metadata.getSchemaString());
    assertEquals(8, schema.getFields().size());
  }

  @Test
  public void testConvertGenericRecordToTableRow() throws Exception {
    TableSchema tableSchema = new TableSchema();
    List<TableFieldSchema> subFields = Lists.<TableFieldSchema>newArrayList(
        new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"));
    /*
     * Note that the quality and quantity fields do not have their mode set, so they should default
     * to NULLABLE. This is an important test of BigQuery semantics.
     *
     * All the other fields we set in this function are required on the Schema response.
     *
     * See https://cloud.google.com/bigquery/docs/reference/v2/tables#schema
     */
    List<TableFieldSchema> fields =
        Lists.<TableFieldSchema>newArrayList(
            new TableFieldSchema().setName("number").setType("INTEGER").setMode("REQUIRED"),
            new TableFieldSchema().setName("species").setType("STRING").setMode("NULLABLE"),
            new TableFieldSchema().setName("quality").setType("FLOAT") /* default to NULLABLE */,
            new TableFieldSchema().setName("quantity").setType("INTEGER") /* default to NULLABLE */,
            new TableFieldSchema().setName("birthday").setType("TIMESTAMP").setMode("NULLABLE"),
            new TableFieldSchema().setName("flighted").setType("BOOLEAN").setMode("NULLABLE"),
            new TableFieldSchema().setName("scion").setType("RECORD").setMode("NULLABLE")
                .setFields(subFields),
            new TableFieldSchema().setName("associates").setType("RECORD").setMode("REPEATED")
                .setFields(subFields));
    tableSchema.setFields(fields);
    Schema avroSchema = AvroCoder.of(Bird.class).getSchema();

    {
      // Test nullable fields.
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      TableRow convertedRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("number", "5")
          .set("associates", new ArrayList<TableRow>());
      assertEquals(row, convertedRow);
    }
    {
      // Test type conversion for TIMESTAMP, INTEGER, BOOLEAN, and FLOAT.
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      record.put("quality", 5.0);
      record.put("birthday", 5L);
      record.put("flighted", Boolean.TRUE);
      TableRow convertedRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("number", "5")
          .set("birthday", "1970-01-01 00:00:00.000005 UTC")
          .set("quality", 5.0)
          .set("associates", new ArrayList<TableRow>())
          .set("flighted", Boolean.TRUE);
      assertEquals(row, convertedRow);
    }
    {
      // Test repeated fields.
      Schema subBirdSchema = AvroCoder.of(Bird.SubBird.class).getSchema();
      GenericRecord nestedRecord = new GenericData.Record(subBirdSchema);
      nestedRecord.put("species", "other");
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("number", 5L);
      record.put("associates", Lists.<GenericRecord>newArrayList(nestedRecord));
      TableRow convertedRow = AvroUtils.convertGenericRecordToTableRow(record, tableSchema);
      TableRow row = new TableRow()
          .set("associates", Lists.<TableRow>newArrayList(
              new TableRow().set("species", "other")))
          .set("number", "5");
      assertEquals(row, convertedRow);
    }
  }

  /**
   * Pojo class used as the record type in tests.
   */
  @DefaultCoder(AvroCoder.class)
  static class Bird {
    long number;
    @Nullable String species;
    @Nullable Double quality;
    @Nullable Long quantity;
    @Nullable Long birthday;  // Exercises TIMESTAMP.
    @Nullable Boolean flighted;
    @Nullable SubBird scion;
    SubBird[] associates;

    static class SubBird {
      @Nullable String species;

      public SubBird() {}
    }

    public Bird() {
      associates = new SubBird[1];
      associates[0] = new SubBird();
    }
  }

  /**
   * Create a list of n random records.
   */
  private static List<Bird> createRandomRecords(long n) {
    String[] species = {"pigeons", "owls", "gulls", "hawks", "robins", "jays"};
    Random random = new Random(0);

    List<Bird> records = new ArrayList<>();
    for (long i = 0; i < n; i++) {
      Bird bird = new Bird();
      bird.quality = random.nextDouble();
      bird.species = species[random.nextInt(species.length)];
      bird.number = i;
      bird.quantity = random.nextLong();
      records.add(bird);
    }
    return records;
  }

  @DefaultCoder(AvroCoder.class)
  public static class MyClass {
    public MyClass() {
      str = "abadsdsa";
      bird = new Bird();
      birds = new LinkedList<Bird>();
      map = new HashMap<>();
      map2 = new HashMap<>();
      map2.put("aninnermap", new HashMap<String, MyClass>());
    }
    @Nullable String str;
    @Nullable Bird bird;
    @Nullable List<Bird> birds;
    @Nullable Map<String, Set<MyClass>> map;
    @Nullable Map<String, Map<String, MyClass>> map2;
  }

  @DefaultCoder(AvroCoder.class)
  public static class MyClass2 {
    public MyClass2() {}
    long value;
    Bird bird;
  }

  @Test
  public void getMyClassSchema()
      throws ClassNotFoundException, ExecutionException, InterruptedException {
    fail(AvroCoder.of(MyClass.class).getSchema().toString());
  }


  @Test
  public void reproThreadAvro607Issue()
      throws ClassNotFoundException, ExecutionException, InterruptedException {
    final String schemaStr = "{\"type\":\"record\",\"name\":\"MyClass\",\"namespace\":\"org.apache.beam.sdk.util.AvroUtilsTest$\",\"fields\":[{\"name\":\"str\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"bird\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Bird\",\"fields\":[{\"name\":\"number\",\"type\":\"long\"},{\"name\":\"species\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"quality\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"quantity\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"birthday\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"flighted\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"scion\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SubBird\",\"namespace\":\"org.apache.beam.sdk.util.AvroUtilsTest$Bird$\",\"fields\":[{\"name\":\"species\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},{\"name\":\"associates\",\"type\":{\"type\":\"array\",\"items\":\"org.apache.beam.sdk.util.AvroUtilsTest$Bird$.SubBird\",\"java-class\":\"[Lorg.apache.beam.sdk.util.AvroUtilsTest$Bird$SubBird;\"}}]}],\"default\":null},{\"name\":\"birds\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"Bird\",\"java-class\":\"java.util.List\"}],\"default\":null},{\"name\":\"map\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"array\",\"items\":\"MyClass\",\"java-class\":\"java.util.Set\"}}],\"default\":null},{\"name\":\"map2\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"map\",\"values\":\"MyClass\"}}],\"default\":null}]}";
    final Schema schema = new Schema.Parser().parse(schemaStr);
    final int numThreads = 1024;
    final CountDownLatch latch = new CountDownLatch(numThreads);
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));
    List<ListenableFuture<byte[]>> ret = new LinkedList<>();
    for (int i = 0; i < numThreads; ++i) {
      final int current = i;
      ret.add(executorService.submit(new Callable<byte[]>() {
        @Override
        public byte[] call() throws Exception {
          latch.countDown();
          latch.await();
          if (current % 3 == 0) {
            ReflectData.get().getSchema(MyClass.class);
          } else if (current % 3 == 1) {
            ReflectData.get().getSchema(MyClass2.class);
          } else {
            ReflectData.get().getSchema(Bird.class);
          }
          return null;
        }
      }));
    }
    Futures.allAsList(ret).get();
    executorService.shutdown();
  }
}
