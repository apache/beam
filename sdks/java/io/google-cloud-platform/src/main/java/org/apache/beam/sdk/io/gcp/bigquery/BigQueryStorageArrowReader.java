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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.values.Row;

class BigQueryStorageArrowReader implements BigQueryStorageReader {

  private final RootAllocator alloc;
  private final Schema arrowSchema;
  private final org.apache.beam.sdk.schemas.Schema arrowBeamSchema;
  private final VectorSchemaRoot vectorRoot;
  private final VectorLoader vectorLoader;
  private Iterator<Row> recordBatchIterable;
  private long rowCount;

  BigQueryStorageArrowReader(ReadSession readSession) throws IOException {
    ArrowSchema protoSchema = readSession.getArrowSchema();
    ReadChannel readChannel =
        new ReadChannel(Channels.newChannel(protoSchema.getSerializedSchema().newInput()));

    this.alloc = new RootAllocator(Long.MAX_VALUE);
    this.arrowSchema = MessageSerializer.deserializeSchema(readChannel);
    this.arrowBeamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(arrowSchema);
    this.vectorRoot = VectorSchemaRoot.create(arrowSchema, alloc);
    this.vectorLoader = new VectorLoader(vectorRoot);
    this.rowCount = 0;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) throws IOException {
    com.google.cloud.bigquery.storage.v1.ArrowRecordBatch recordBatch =
        readRowsResponse.getArrowRecordBatch();
    InputStream buffer = recordBatch.getSerializedRecordBatch().newInput();
    rowCount = recordBatch.getRowCount();

    vectorRoot.clear();
    ReadChannel readChannel = new ReadChannel(Channels.newChannel(buffer));
    ArrowRecordBatch arrowMessage = MessageSerializer.deserializeRecordBatch(readChannel, alloc);
    vectorLoader.load(arrowMessage);
    recordBatchIterable =
        ArrowConversion.rowsFromRecordBatch(arrowBeamSchema, vectorRoot).iterator();
    arrowMessage.close();
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public Object readSingleRecord() throws IOException {
    return recordBatchIterable.next();
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return recordBatchIterable == null || !recordBatchIterable.hasNext();
  }

  @Override
  public void resetBuffer() {
    recordBatchIterable = null;
  }

  @Override
  public void close() {
    vectorRoot.close();
    alloc.close();
  }
}
