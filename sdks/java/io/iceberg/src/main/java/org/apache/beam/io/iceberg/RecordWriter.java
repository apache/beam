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
package org.apache.beam.io.iceberg;

import java.io.IOException;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
class RecordWriter<ElementT> {

  final Table table;

  final DataWriter<Record> writer;
  final GenericRecord baseRecord;
  final SerializableBiFunction<Record, ElementT, Record> toRecord;

  final String location;

  RecordWriter(
      Table table,
      String location,
      Schema schema,
      PartitionSpec partitionSpec,
      FileFormat format,
      SerializableBiFunction<Record, ElementT, Record> toRecord)
      throws IOException {
    this.table = table;
    this.baseRecord = GenericRecord.create(schema);
    this.toRecord = toRecord;
    this.location = table.locationProvider().newDataLocation(partitionSpec, baseRecord, location);

    OutputFile outputFile = table.io().newOutputFile(this.location);
    switch (format) {
      case AVRO:
        writer =
            Avro.writeData(outputFile).schema(schema).withSpec(partitionSpec).overwrite().build();
        break;
      case PARQUET:
        writer =
            Parquet.writeData(outputFile)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(schema)
                .withSpec(partitionSpec)
                .overwrite()
                .build();
        break;
      case ORC:
        writer =
            ORC.writeData(outputFile)
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .schema(schema)
                .withSpec(partitionSpec)
                .overwrite()
                .build();
        break;
      default:
        throw new RuntimeException("Unrecognized File Format. This should be impossible.");
    }
  }

  public void write(ElementT element) throws IOException {
    Record record = toRecord.apply(baseRecord, element);
    writer.write(record);
  }

  public void close() throws IOException {
    writer.close();
  }

  public long bytesWritten() {
    return writer.length();
  }

  public Table table() {
    return table;
  }

  public String location() {
    return location;
  }

  public DataFile dataFile() {
    return writer.toDataFile();
  }
}
