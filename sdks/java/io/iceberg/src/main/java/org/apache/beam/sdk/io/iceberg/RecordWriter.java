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

import java.io.IOException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);
  private final Counter activeWriters = Metrics.counter(RecordWriterManager.class, "activeWriters");
  private final DataWriter<Record> icebergDataWriter;
  private final Table table;
  private final String absoluteFilename;
  private final FileFormat fileFormat;

  RecordWriter(
      Catalog catalog, IcebergDestination destination, String filename, PartitionKey partitionKey)
      throws IOException {
    this(
        catalog.loadTable(destination.getTableIdentifier()),
        destination.getFileFormat(),
        filename,
        partitionKey);
  }

  RecordWriter(Table table, FileFormat fileFormat, String filename, PartitionKey partitionKey)
      throws IOException {
    this.table = table;
    this.fileFormat = fileFormat;
    if (table.spec().isUnpartitioned()) {
      absoluteFilename =
          fileFormat.addExtension(table.locationProvider().newDataLocation(filename));
    } else {
      absoluteFilename =
          fileFormat.addExtension(
              table.locationProvider().newDataLocation(table.spec(), partitionKey, filename));
    }
    OutputFile outputFile = table.io().newOutputFile(absoluteFilename);

    switch (fileFormat) {
      case AVRO:
        icebergDataWriter =
            Avro.writeData(outputFile)
                .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                .schema(table.schema())
                .withSpec(table.spec())
                .withPartition(partitionKey)
                .overwrite()
                .build();
        break;
      case PARQUET:
        icebergDataWriter =
            Parquet.writeData(outputFile)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(table.schema())
                .withSpec(table.spec())
                .withPartition(partitionKey)
                .overwrite()
                .build();
        break;
      case ORC:
        throw new UnsupportedOperationException("ORC file format not currently supported.");
      default:
        throw new RuntimeException("Unknown File Format: " + fileFormat);
    }
    activeWriters.inc();
    LOG.info(
        "Opened {} writer for table {}, partition {}. Writing to path: {}",
        fileFormat,
        table.name(),
        partitionKey,
        absoluteFilename);
  }

  public void write(Record record) {
    icebergDataWriter.write(record);
  }

  public void close() throws IOException {
    try {
      icebergDataWriter.close();
    } catch (IOException e) {
      throw new IOException(
          String.format(
              "Failed to close %s writer for table %s, path: %s",
              fileFormat, table.name(), absoluteFilename),
          e);
    }
    activeWriters.dec();
    LOG.info("Closed {} writer for table {}, path: {}", fileFormat, table.name(), absoluteFilename);
  }

  public long bytesWritten() {
    return icebergDataWriter.length();
  }

  public DataFile getDataFile() {
    return icebergDataWriter.toDataFile();
  }
}
