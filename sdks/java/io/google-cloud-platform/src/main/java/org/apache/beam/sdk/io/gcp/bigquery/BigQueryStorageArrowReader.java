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
import java.nio.channels.Channels;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.values.Row;

class BigQueryStorageArrowReader implements BigQueryStorageReader {

  private org.apache.beam.sdk.schemas.Schema arrowBeamSchema;
  private @Nullable Iterator<Row> recordBatchIterable;
  private long rowCount;
  private ArrowSchema protoSchema;
  private RootAllocator alloc;
  private ReadChannel read;

  BigQueryStorageArrowReader(ReadSession readSession) throws IOException {
    protoSchema = readSession.getArrowSchema();
    InputStream input = protoSchema.getSerializedSchema().newInput();
    this.arrowBeamSchema =
        ArrowConversion.ArrowSchemaTranslator.toBeamSchema(
            ArrowConversion.arrowSchemaFromInput(input));
    this.rowCount = 0;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException {
    com.google.cloud.bigquery.storage.v1.ArrowRecordBatch recordBatch =
        readRowsResponse.getArrowRecordBatch();
    rowCount = recordBatch.getRowCount();
    this.read = new ReadChannel(Channels.newChannel(protoSchema.getSerializedSchema().newInput()));
    this.alloc = new RootAllocator(Long.MAX_VALUE);
    recordBatchIterable =
        ArrowConversion.rowsFromRecordBatch(
                arrowBeamSchema,
                ArrowConversion.rowFromSerializedRecordBatch(this.alloc, this.read))
            .iterator();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Object readSingleRecord() throws IOException {
    if (recordBatchIterable == null) {
      throw new IOException("Not Initialized");
    }
    return recordBatchIterable.next();
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return recordBatchIterable == null || !recordBatchIterable.hasNext();
  }

  @Override
  public void resetBuffer() {
    recordBatchIterable = null;
    alloc.close();
    try {
      read.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void close() {
    alloc.close();
    try {
      read.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
