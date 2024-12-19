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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScanTaskReader extends BoundedSource.BoundedReader<Row> {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTaskReader.class);

  private final ScanTaskSource source;
  private final org.apache.iceberg.Schema project;

  transient @Nullable FileIO io;
  transient @Nullable InputFilesDecryptor decryptor;
  transient @Nullable Queue<FileScanTask> fileScanTasks;
  transient @Nullable CloseableIterator<Record> currentIterator;
  transient @Nullable Record current;

  public ScanTaskReader(ScanTaskSource source) {
    this.source = source;
    this.project = IcebergUtils.beamSchemaToIcebergSchema(source.getSchema());
  }

  @Override
  public boolean start() throws IOException {
    Table table = source.getTable();
    EncryptionManager encryptionManager = table.encryption();

    current = null;
    io = table.io();
    decryptor = new InputFilesDecryptor(source.getTask(), io, encryptionManager);
    fileScanTasks = new ArrayDeque<>();
    fileScanTasks.addAll(source.getTask().files());

    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    Queue<FileScanTask> fileScanTasks =
        checkStateNotNull(this.fileScanTasks, "files null in advance() - did you call start()?");
    InputFilesDecryptor decryptor =
        checkStateNotNull(this.decryptor, "decryptor null in adance() - did you call start()?");

    // This nullness annotation is incorrect, but the most expedient way to work with Iceberg's APIs
    // which are not null-safe.
    @SuppressWarnings("nullness")
    org.apache.iceberg.@NonNull Schema project = this.project;

    do {
      // If our current iterator is working... do that.
      if (currentIterator != null && currentIterator.hasNext()) {
        current = currentIterator.next();
        return true;
      }

      // Close out the current iterator and try to open a new one
      if (currentIterator != null) {
        currentIterator.close();
        currentIterator = null;
      }

      LOG.info("Trying to open new file.");
      if (fileScanTasks.isEmpty()) {
        LOG.info("We have exhausted all available files in this CombinedScanTask");
        break;
      }

      // We have a new file to start reading
      FileScanTask fileTask = fileScanTasks.remove();
      DataFile file = fileTask.file();
      InputFile input = decryptor.getInputFile(fileTask);
      Map<Integer, ?> idToConstants =
          constantsMap(fileTask, IdentityPartitionConverters::convertConstant, project);

      CloseableIterable<Record> iterable;
      switch (file.format()) {
        case ORC:
          LOG.info("Preparing ORC input");
          iterable =
              ORC.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(project)
                  .createReaderFunc(
                      fileSchema ->
                          GenericOrcReader.buildReader(project, fileSchema, idToConstants))
                  .filter(fileTask.residual())
                  .build();
          break;
        case PARQUET:
          LOG.info("Preparing Parquet input.");
          iterable =
              Parquet.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(project)
                  .createReaderFunc(
                      fileSchema ->
                          GenericParquetReaders.buildReader(project, fileSchema, idToConstants))
                  .filter(fileTask.residual())
                  .build();
          break;
        case AVRO:
          LOG.info("Preparing Avro input.");
          iterable =
              Avro.read(input)
                  .split(fileTask.start(), fileTask.length())
                  .project(project)
                  .createReaderFunc(
                      fileSchema -> DataReader.create(project, fileSchema, idToConstants))
                  .build();
          break;
        default:
          throw new UnsupportedOperationException("Cannot read format: " + file.format());
      }
      currentIterator = iterable.iterator();

    } while (true);

    return false;
  }

  private Map<Integer, ?> constantsMap(
      FileScanTask task, BiFunction<Type, Object, Object> converter, Schema schema) {
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    Schema partitionSchema = TypeUtil.select(schema, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();

    if (projectsIdentityPartitionColumns) {
      return PartitionUtil.constantsMap(task, converter);
    } else {
      return Collections.emptyMap();
    }
  }

  @Override
  public Row getCurrent() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }
    return IcebergUtils.icebergRecordToBeamRow(source.getSchema(), current);
  }

  @Override
  public void close() throws IOException {
    if (currentIterator != null) {
      currentIterator.close();
      currentIterator = null;
    }
    if (fileScanTasks != null) {
      fileScanTasks.clear();
      fileScanTasks = null;
    }
    if (io != null) {
      io.close();
      io = null;
    }
  }

  @Override
  public BoundedSource<Row> getCurrentSource() {
    return source;
  }
}
