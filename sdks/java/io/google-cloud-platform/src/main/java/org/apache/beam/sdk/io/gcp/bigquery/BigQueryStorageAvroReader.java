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
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

class BigQueryStorageAvroReader<AvroT, T> implements BigQueryStorageReader<T> {

  private final DatumReader<AvroT> datumReader;
  private final SerializableFunction<AvroT, T> fromAvroRecord;
  private final @Nullable AvroCoder<AvroT> badRecordCoder;
  private @Nullable BinaryDecoder decoder;
  private long rowCount;

  private transient @Nullable AvroT badRecord = null;

  BigQueryStorageAvroReader(
      Schema writerSchema,
      Schema readerSchema,
      AvroSource.DatumReaderFactory<AvroT> readerFactory,
      SerializableFunction<AvroT, T> fromAvroRecord) {
    this.datumReader = readerFactory.apply(writerSchema, readerSchema);
    this.fromAvroRecord = fromAvroRecord;
    this.rowCount = 0;
    this.decoder = null;
    if (readerFactory instanceof AvroDatumFactory) {
      this.badRecordCoder = AvroCoder.of((AvroDatumFactory<AvroT>) readerFactory, readerSchema);
    } else {
      this.badRecordCoder = null;
    }
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
  public T readSingleRecord() throws IOException {
    Preconditions.checkStateNotNull(decoder);
    @SuppressWarnings({
      "nullness" // reused record is null but avro not annotated
    })
    // record should not be reused, mutating outputted values is unsafe
    AvroT avroRecord = datumReader.read(/*reuse=*/ null, decoder);
    try {
      return fromAvroRecord.apply(avroRecord);
    } catch (Exception e) {
      badRecord = avroRecord;
      throw new ReadException(e);
    }
  }

  @Override
  public @Nullable Object getLastBadRecord() {
    return badRecord;
  }

  @Override
  public @Nullable Coder<AvroT> getBadRecordCoder() {
    return badRecordCoder;
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
