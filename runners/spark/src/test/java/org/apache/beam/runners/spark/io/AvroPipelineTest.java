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
package org.apache.beam.runners.spark.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Avro pipeline test. */
public class AvroPipelineTest {

  private File inputFile;
  private File outputFile;

  @Rule public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() throws IOException {
    inputFile = tmpDir.newFile("test.avro");
    outputFile = new File(tmpDir.getRoot(), "out.avro");
  }

  @Test
  public void testGeneric() throws Exception {
    Schema schema = new Schema.Parser().parse(Resources.getResource("person.avsc").openStream());
    GenericRecord savedRecord = new GenericData.Record(schema);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), schema);

    PCollection<GenericRecord> input =
        pipeline.apply(AvroIO.readGenericRecords(schema).from(inputFile.getAbsolutePath()));
    input.apply(AvroIO.writeGenericRecords(schema).to(outputFile.getAbsolutePath()));
    pipeline.run();

    List<GenericRecord> records = readGenericFile();
    assertEquals(Lists.newArrayList(savedRecord), records);
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema)
      throws IOException {
    FileOutputStream outputStream = new FileOutputStream(this.inputFile);
    GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(schema);

    try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(genericDatumWriter)) {
      dataFileWriter.create(schema, outputStream);
      for (GenericRecord record : genericRecords) {
        dataFileWriter.append(record);
      }
    }
    outputStream.close();
  }

  private List<GenericRecord> readGenericFile() throws IOException {
    List<GenericRecord> records = Lists.newArrayList();
    GenericDatumReader<GenericRecord> genericDatumReader = new GenericDatumReader<>();
    try (DataFileReader<GenericRecord> dataFileReader =
        new DataFileReader<>(new File(outputFile + "-00000-of-00001"), genericDatumReader)) {
      for (GenericRecord record : dataFileReader) {
        records.add(record);
      }
    }
    return records;
  }
}
