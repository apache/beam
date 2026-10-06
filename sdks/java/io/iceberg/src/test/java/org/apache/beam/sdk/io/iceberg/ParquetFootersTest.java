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
package org.apache.beam.sdk.io.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ParquetFootersTest {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()));

  @Test
  public void testReadsSchemaAndRowCount() throws IOException {
    String path = writeParquet("data.parquet", 3);

    ParquetMetadata footer = ParquetFooters.read(path);

    MessageType schema = footer.getFileMetaData().getSchema();
    assertEquals(2, schema.getFieldCount());
    assertEquals("id", schema.getFieldName(0));
    assertEquals("name", schema.getFieldName(1));
    long rows = 0;
    for (org.apache.parquet.hadoop.metadata.BlockMetaData block : footer.getBlocks()) {
      rows += block.getRowCount();
    }
    assertEquals(3, rows);
  }

  @Test
  public void testMissingFileThrowsFileNotFound() {
    String path = new File(temp.getRoot(), "missing.parquet").getAbsolutePath();
    assertThrows(FileNotFoundException.class, () -> ParquetFooters.read(path));
  }

  @Test
  public void testEmptyFileThrows() throws IOException {
    File file = temp.newFile("empty.parquet");
    assertThrows(RuntimeException.class, () -> ParquetFooters.read(file.getAbsolutePath()));
  }

  @Test
  public void testNonParquetFileThrows() throws IOException {
    File file = temp.newFile("garbage.parquet");
    Files.write(file.toPath(), "this is not a parquet file".getBytes(StandardCharsets.UTF_8));
    assertThrows(RuntimeException.class, () -> ParquetFooters.read(file.getAbsolutePath()));
  }

  @Test
  public void testTruncatedFileThrows() throws IOException {
    String path = writeParquet("full.parquet", 3);
    byte[] bytes = Files.readAllBytes(new File(path).toPath());
    File truncated = temp.newFile("truncated.parquet");
    // Drop the trailing footer-length + magic bytes so the footer cannot be located.
    Files.write(truncated.toPath(), java.util.Arrays.copyOf(bytes, bytes.length - 8));
    assertThrows(RuntimeException.class, () -> ParquetFooters.read(truncated.getAbsolutePath()));
  }

  private String writeParquet(String name, int rows) throws IOException {
    String path = new File(temp.getRoot(), name).getAbsolutePath();
    DataWriter<Record> writer =
        Parquet.writeData(org.apache.iceberg.Files.localOutput(path))
            .schema(SCHEMA)
            .withSpec(PartitionSpec.unpartitioned())
            .createWriterFunc(GenericParquetWriter::create)
            .build();
    for (int i = 0; i < rows; i++) {
      writer.write(GenericRecord.create(SCHEMA).copy("id", i, "name", "n" + i));
    }
    writer.close();
    return path;
  }
}
