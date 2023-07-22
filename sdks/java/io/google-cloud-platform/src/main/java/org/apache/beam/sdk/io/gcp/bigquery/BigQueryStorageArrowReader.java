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

import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion.RecordBatchRowIterator;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.values.Row;

class BigQueryStorageArrowReader implements BigQueryStorageReader {

  private @Nullable RecordBatchRowIterator recordBatchIterator;
  private long rowCount;
  private ArrowSchema protoSchema;
  private @Nullable RootAllocator alloc;

  BigQueryStorageArrowReader(ReadSession readSession) throws IOException {
    protoSchema = readSession.getArrowSchema();
    this.rowCount = 0;
    this.alloc = null;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException {
    com.google.cloud.bigquery.storage.v1.ArrowRecordBatch recordBatch =
        readRowsResponse.getArrowRecordBatch();
    rowCount = recordBatch.getRowCount();
    InputStream input = protoSchema.getSerializedSchema().newInput();
    Schema arrowSchema = ArrowConversion.arrowSchemaFromInput(input);
    RootAllocator alloc = new RootAllocator(Long.MAX_VALUE);
    this.alloc = alloc;
    this.recordBatchIterator =
        ArrowConversion.rowsFromSerializedRecordBatch(
            arrowSchema, recordBatch.getSerializedRecordBatch().newInput(), alloc);
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public GenericRecord readSingleRecord() throws IOException {
    if (recordBatchIterator == null) {
      throw new IOException("Not Initialized");
    }
    Row row = recordBatchIterator.next();
    // TODO(https://github.com/apache/beam/issues/21076): Update this interface to expect a Row, and
    // avoid converting Arrow data to
    // GenericRecord.
    return AvroUtils.toGenericRecord(row, null);
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
