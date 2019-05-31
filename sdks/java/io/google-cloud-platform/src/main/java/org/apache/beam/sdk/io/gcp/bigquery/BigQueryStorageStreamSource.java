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

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.fromJsonString;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.toJsonString;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** A {@link org.apache.beam.sdk.io.Source} representing a single stream in a read session. */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  public static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      Stream stream,
      TableSchema tableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession,
        stream,
        toJsonString(checkNotNull(tableSchema, "tableSchema")),
        parseFn,
        outputCoder,
        bqServices);
  }

  private final ReadSession readSession;
  private final Stream stream;
  private final String jsonTableSchema;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamSource(
      ReadSession readSession,
      Stream stream,
      String jsonTableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = checkNotNull(readSession, "readSession");
    this.stream = checkNotNull(stream, "stream");
    this.jsonTableSchema = checkNotNull(jsonTableSchema, "jsonTableSchema");
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .addIfNotNull(
            DisplayData.item("table", BigQueryHelpers.toTableSpec(readSession.getTableReference()))
                .withLabel("Table"))
        .add(DisplayData.item("readSession", readSession.getName()).withLabel("Read session"))
        .add(DisplayData.item("stream", stream.getName()).withLabel("Stream"));
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    // The size of stream source can't be estimated due to server-side liquid sharding.
    // TODO: Implement progress reporting.
    return 0L;
  }

  @Override
  public List<? extends BoundedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    // A stream source can't be split without reading from it due to server-side liquid sharding.
    // TODO: Implement dynamic work rebalancing.
    return ImmutableList.of(this);
  }

  @Override
  public BigQueryStorageStreamReader<T> createReader(PipelineOptions options) throws IOException {
    return new BigQueryStorageStreamReader<>(this, options.as(BigQueryOptions.class));
  }

  /** A {@link org.apache.beam.sdk.io.Source.Reader} which reads records from a stream. */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public static class BigQueryStorageStreamReader<T> extends BoundedSource.BoundedReader<T> {

    private final DatumReader<GenericRecord> datumReader;
    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final StorageClient storageClient;
    private final TableSchema tableSchema;

    private BigQueryStorageStreamSource<T> source;
    private Iterator<ReadRowsResponse> responseIterator;
    private BinaryDecoder decoder;
    private GenericRecord record;
    private T current;
    private long currentOffset;

    private BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, BigQueryOptions options) throws IOException {
      this.source = source;
      this.datumReader =
          new GenericDatumReader<>(
              new Schema.Parser().parse(source.readSession.getAvroSchema().getSchema()));
      this.parseFn = source.parseFn;
      this.storageClient = source.bqServices.getStorageClient(options);
      this.tableSchema = fromJsonString(source.jsonTableSchema, TableSchema.class);
    }

    @Override
    public boolean start() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadPosition(
                  StreamPosition.newBuilder().setStream(source.stream).setOffset(currentOffset))
              .build();

      responseIterator = storageClient.readRows(request).iterator();
      return readNextRecord();
    }

    @Override
    public boolean advance() throws IOException {
      currentOffset++;
      return readNextRecord();
    }

    private boolean readNextRecord() throws IOException {
      while (decoder == null || decoder.isEnd()) {
        if (!responseIterator.hasNext()) {
          return false;
        }

        ReadRowsResponse nextResponse = responseIterator.next();

        decoder =
            DecoderFactory.get()
                .binaryDecoder(
                    nextResponse.getAvroRows().getSerializedBinaryRows().toByteArray(), decoder);
      }

      record = datumReader.read(record, decoder);
      current = parseFn.apply(new SchemaAndRecord(record, tableSchema));
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public void close() {
      storageClient.close();
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public BoundedSource<T> splitAtFraction(double fraction) {
      return null;
    }
  }
}
