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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import java.io.IOException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion.RecordBatchRowIterator;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigQueryStorageArrowReader<T> implements BigQueryStorageReader<T> {

  private final org.apache.arrow.vector.types.pojo.Schema arrowSchema;
  private final Schema schema;
  private final SerializableFunction<Row, T> fromRow;
  private final Coder<Row> badRecordCoder;
  private @Nullable RecordBatchRowIterator recordBatchIterator;
  private long rowCount;
  private @Nullable RootAllocator alloc;

  private transient @Nullable Row badRecord = null;

  BigQueryStorageArrowReader(
      org.apache.arrow.vector.types.pojo.Schema writerSchema,
      Schema readerSchema,
      SerializableFunction<Row, T> fromRow) {
    this.arrowSchema = writerSchema;
    this.schema = readerSchema;
    this.fromRow = fromRow;
    this.badRecordCoder = RowCoder.of(readerSchema);
    this.rowCount = 0;
    this.alloc = null;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException {
    com.google.cloud.bigquery.storage.v1.ArrowRecordBatch recordBatch =
        readRowsResponse.getArrowRecordBatch();
    rowCount = recordBatch.getRowCount();
    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    this.alloc = alloc;
    this.recordBatchIterator =
        ArrowConversion.rowsFromSerializedRecordBatch(
            arrowSchema, schema, recordBatch.getSerializedRecordBatch().newInput(), alloc);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public T readSingleRecord() throws IOException {
    if (recordBatchIterator == null) {
      throw new IOException("Not Initialized");
    }
    Row row = recordBatchIterator.next();
    try {
      return fromRow.apply(row);
    } catch (Exception e) {
      badRecord = row;
      throw new ReadException(e);
    }
  }

  @Override
  public @Nullable Row getLastBadRecord() {
    return badRecord;
  }

  @Override
  public Coder<Row> getBadRecordCoder() {
    return badRecordCoder;
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return recordBatchIterator == null || !recordBatchIterator.hasNext();
  }

  @Override
  public void resetBuffer() {
    cleanUp();
  }

  private void cleanUp() {
    if (recordBatchIterator != null) {
      recordBatchIterator.close();
      recordBatchIterator = null;
    }
    if (alloc != null) {
      alloc.close();
      alloc = null;
    }
  }

  @Override
  public void close() {
    this.cleanUp();
  }
}
