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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * The {@link ParquetTestUtil} class provides common utilities used for executing tests that involve
 * Parquet.
 */
public class ParquetTestUtil {

  /**
   * Create Parquet file for the given schema and records list.
   *
   * @param schema Schema to use.
   * @param records Records to write to the sink.
   * @return Byte array that represents the content.
   */
  public static byte[] createParquetFile(Schema schema, List<GenericRecord> records)
      throws IOException {

    File tempFile = createTempFile();
    Path file = new Path(tempFile.getPath());

    AvroParquetWriter.Builder<GenericRecord> builder = AvroParquetWriter.builder(file);
    ParquetWriter<GenericRecord> parquetWriter = builder.withSchema(schema).build();
    for (GenericRecord record : records) {
      parquetWriter.write(record);
    }
    parquetWriter.close();

    return Files.readAllBytes(tempFile.toPath());
  }

  /**
   * Read Parquet records to a list of {@link GenericRecord}.
   *
   * @param contents Byte array with contents to read.
   * @return A list with all records.
   */
  public static List<GenericRecord> readRecords(byte[] contents) throws IOException {
    List<GenericRecord> records = new ArrayList<>();

    try (ParquetReader<GenericRecord> reader =
        AvroParquetReader.<GenericRecord>builder(new ParquetByteArrayInput(contents)).build()) {
      GenericRecord record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }

  // @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  private static File createTempFile() throws IOException {
    File tempFile = File.createTempFile(ParquetTestUtil.class.getSimpleName(), ".tmp");
    tempFile.deleteOnExit();
    tempFile.delete();
    return tempFile;
  }

  static class ParquetByteArrayInput implements InputFile {
    private final byte[] data;

    private static class SeekByteArray extends ByteArrayInputStream {
      public SeekByteArray(byte[] buf) {
        super(buf);
      }

      public void setPos(int pos) {
        this.pos = pos;
      }

      public int getPos() {
        return this.pos;
      }
    }

    public ParquetByteArrayInput(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return this.data.length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new DelegatingSeekableInputStream(new SeekByteArray(this.data)) {
        @Override
        public void seek(long newPos) {
          ((SeekByteArray) this.getStream()).setPos((int) newPos);
        }

        @Override
        public long getPos() {
          return ((SeekByteArray) this.getStream()).getPos();
        }
      };
    }
  }
}
