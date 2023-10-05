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
package org.apache.beam.it.gcp.artifacts.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

/**
 * The {@link AvroTestUtil} class provides common utilities used for executing tests that involve
 * Avro.
 */
public class AvroTestUtil {

  /**
   * Create Avro file for the given schema and records list.
   *
   * @param schema Schema to use.
   * @param records Records to write on the sink.
   * @return Byte array that represents the content.
   */
  public static byte[] createAvroFile(Schema schema, List<GenericRecord> records)
      throws IOException {
    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<>(writer).create(schema, baos);
    for (GenericRecord record : records) {
      fileWriter.append(record);
    }
    fileWriter.close();

    return baos.toByteArray();
  }

  /**
   * Read Avro records to a list of {@link GenericRecord}.
   *
   * @param schema Schema to use for reading.
   * @param contents Byte array with contents to read.
   * @return A list with all records.
   */
  public static List<GenericRecord> readRecords(Schema schema, byte[] contents) throws IOException {
    List<GenericRecord> records = new ArrayList<>();

    GenericDatumReader<GenericRecord> datum = new GenericDatumReader<>(schema);
    DataFileReader<GenericRecord> reader =
        new DataFileReader<>(new SeekableByteArrayInput(contents), datum);

    while (reader.hasNext()) {
      records.add(reader.next());
    }

    return records;
  }
}
