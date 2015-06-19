/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class AvroPipelineTest {

  private File inputFile;
  private File outputDir;

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    inputFile = tmpDir.newFile("test.avro");
    outputDir = tmpDir.newFolder("out");
    outputDir.delete();
  }

  @Test
  public void testGeneric() throws Exception {
    Schema schema = new Schema.Parser().parse(Resources.getResource("person.avsc").openStream());
    GenericRecord savedRecord = new GenericData.Record(schema);
    savedRecord.put("name", "John Doe");
    savedRecord.put("age", 42);
    savedRecord.put("siblingnames", Lists.newArrayList("Jimmy", "Jane"));
    populateGenericFile(Lists.newArrayList(savedRecord), schema);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    PCollection<GenericRecord> input = p.apply(
        AvroIO.Read.from(inputFile.getAbsolutePath()).withSchema(schema));
    input.apply(AvroIO.Write.to(outputDir.getAbsolutePath()).withSchema(schema));
    EvaluationResult res = SparkPipelineRunner.create().run(p);
    res.close();

    List<GenericRecord> records = readGenericFile();
    assertEquals(Lists.newArrayList(savedRecord), records);
  }

  private void populateGenericFile(List<GenericRecord> genericRecords, Schema schema) throws IOException {
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
    try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>
        (new File(outputDir, "part-r-00000.avro"), genericDatumReader)) {
      for (GenericRecord record : dataFileReader) {
        records.add(record);
      }
    }
    return records;
  }


}
