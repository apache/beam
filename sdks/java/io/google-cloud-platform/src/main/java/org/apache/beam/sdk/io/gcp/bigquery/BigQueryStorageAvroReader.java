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

import com.google.cloud.bigquery.storage.v1.AvroRows;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigQueryStorageAvroReader implements BigQueryStorageReader {

  private final Schema avroSchema;
  private final DatumReader<GenericRecord> datumReader;
  private @Nullable BinaryDecoder decoder;
  private long rowCount;

  BigQueryStorageAvroReader(ReadSession readSession) {
    this.avroSchema = new Schema.Parser().parse(readSession.getAvroSchema().getSchema());
    this.datumReader = new GenericDatumReader<>(avroSchema);
    this.rowCount = 0;
    decoder = null;
  }

  @Override
  public void processReadRowsResponse(ReadRowsResponse readRowsResponse) {
    AvroRows avroRows = readRowsResponse.getAvroRows();
    rowCount = avroRows.getRowCount();
    @SuppressWarnings({
      "nullness" // reused decoder can be null but avro not annotated
    })
    BinaryDecoder newDecoder =
        DecoderFactory.get()
            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);
    decoder = newDecoder;
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public GenericRecord readSingleRecord() throws IOException {
    Preconditions.checkStateNotNull(decoder);
    @SuppressWarnings({
      "nullness" // reused record is null but avro not annotated
    })
    // record should not be reused, mutating outputted values is unsafe
    GenericRecord newRecord = datumReader.read(/*reuse=*/ null, decoder);
    return newRecord;
  }

  @Override
  public boolean readyForNextReadResponse() throws IOException {
    return decoder == null || decoder.isEnd();
  }

  @Override
  public void resetBuffer() {
    decoder = null;
  }

  @Override
  public void close() {}
}
