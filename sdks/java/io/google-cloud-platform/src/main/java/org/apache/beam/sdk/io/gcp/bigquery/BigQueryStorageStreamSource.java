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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamRequest;
import com.google.cloud.bigquery.storage.v1.SplitReadStreamResponse;
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
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.BigQueryServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing a single stream in a read session. */
@Experimental(Kind.SOURCE_SINK)
public class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamSource.class);

  public static <T> BigQueryStorageStreamSource<T> create(
      ReadSession readSession,
      ReadStream readStream,
      TableSchema tableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSource<>(
        readSession,
        readStream,
        toJsonString(checkNotNull(tableSchema, "tableSchema")),
        parseFn,
        outputCoder,
        bqServices);
  }

  /**
   * Creates a new source with the same properties as this one, except with a different {@link
   * ReadStream}.
   */
  public BigQueryStorageStreamSource<T> fromExisting(ReadStream newReadStream) {
    return new BigQueryStorageStreamSource<>(
        readSession, newReadStream, jsonTableSchema, parseFn, outputCoder, bqServices);
  }

  private final ReadSession readSession;
  private final ReadStream readStream;
  private final String jsonTableSchema;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamSource(
      ReadSession readSession,
      ReadStream readStream,
      String jsonTableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = checkNotNull(readSession, "readSession");
    this.readStream = checkNotNull(readStream, "stream");
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
        .add(DisplayData.item("table", readSession.getTable()).withLabel("Table"))
        .add(DisplayData.item("readSession", readSession.getName()).withLabel("Read session"))
        .add(DisplayData.item("stream", readStream.getName()).withLabel("Stream"));
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

  @Override
  public String toString() {
    return readStream.toString();
  }

  /** A {@link org.apache.beam.sdk.io.Source.Reader} which reads records from a stream. */
  @Experimental(Kind.SOURCE_SINK)
  public static class BigQueryStorageStreamReader<T> extends BoundedSource.BoundedReader<T> {

    private final DatumReader<GenericRecord> datumReader;
    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final StorageClient storageClient;
    private final TableSchema tableSchema;

    private BigQueryStorageStreamSource<T> source;
    private BigQueryServerStream<ReadRowsResponse> responseStream;
    private Iterator<ReadRowsResponse> responseIterator;
    private BinaryDecoder decoder;
    private GenericRecord record;
    private T current;
    private long currentOffset;

    // Values used for progress reporting.
    private double fractionConsumed;
    private double fractionConsumedFromPreviousResponse;
    private double fractionConsumedFromCurrentResponse;
    private long rowsReadFromCurrentResponse;
    private long totalRowCountFromCurrentResponse;

    private BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, BigQueryOptions options) throws IOException {
      this.source = source;
      this.datumReader =
          new GenericDatumReader<>(
              new Schema.Parser().parse(source.readSession.getAvroSchema().getSchema()));
      this.parseFn = source.parseFn;
      this.storageClient = source.bqServices.getStorageClient(options);
      this.tableSchema = fromJsonString(source.jsonTableSchema, TableSchema.class);
      this.fractionConsumed = 0d;
      this.fractionConsumedFromPreviousResponse = 0d;
      this.fractionConsumedFromCurrentResponse = 0d;
      this.rowsReadFromCurrentResponse = 0L;
      this.totalRowCountFromCurrentResponse = 0L;
    }

    @Override
    public synchronized boolean start() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadStream(source.readStream.getName())
              .setOffset(currentOffset)
              .build();

      responseStream = storageClient.readRows(request);
      responseIterator = responseStream.iterator();
      LOG.info("Started BigQuery Storage API read from stream {}.", source.readStream.getName());
      return readNextRecord();
    }

    @Override
    public synchronized boolean advance() throws IOException {
      currentOffset++;
      return readNextRecord();
    }

    private synchronized boolean readNextRecord() throws IOException {
      while (decoder == null || decoder.isEnd()) {
        if (!responseIterator.hasNext()) {
          fractionConsumed = 1d;
          return false;
        }

        fractionConsumedFromPreviousResponse = fractionConsumedFromCurrentResponse;
        ReadRowsResponse currentResponse = responseIterator.next();
        decoder =
            DecoderFactory.get()
                .binaryDecoder(
                    currentResponse.getAvroRows().getSerializedBinaryRows().toByteArray(), decoder);

        // Since we now have a new response, reset the row counter for the current response.
        rowsReadFromCurrentResponse = 0L;

        totalRowCountFromCurrentResponse = currentResponse.getAvroRows().getRowCount();
        fractionConsumedFromCurrentResponse = getFractionConsumed(currentResponse);

        Preconditions.checkArgument(
            totalRowCountFromCurrentResponse >= 0L,
            "Row count from current response (%s) must be greater than or equal to zero.",
            totalRowCountFromCurrentResponse);
        Preconditions.checkArgument(
            0f <= fractionConsumedFromCurrentResponse && fractionConsumedFromCurrentResponse <= 1f,
            "Fraction consumed from current response (%s) is not in the range [0.0, 1.0].",
            fractionConsumedFromCurrentResponse);
        Preconditions.checkArgument(
            fractionConsumedFromPreviousResponse <= fractionConsumedFromCurrentResponse,
            "Fraction consumed from the current response (%s) has to be larger than or equal to "
                + "the fraction consumed from the previous response (%s).",
            fractionConsumedFromCurrentResponse,
            fractionConsumedFromPreviousResponse);
      }

      record = datumReader.read(record, decoder);
      current = parseFn.apply(new SchemaAndRecord(record, tableSchema));

      // Updates the fraction consumed value. This value is calculated by interpolating between
      // the fraction consumed value from the previous server response (or zero if we're consuming
      // the first response) and the fractional value in the current response based on how many of
      // the rows in the current response have been consumed.
      rowsReadFromCurrentResponse++;
      fractionConsumed =
          fractionConsumedFromPreviousResponse
              + (fractionConsumedFromCurrentResponse - fractionConsumedFromPreviousResponse)
                  * rowsReadFromCurrentResponse
                  * 1.0
                  / totalRowCountFromCurrentResponse;

      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public synchronized void close() {
      storageClient.close();
    }

    @Override
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public BoundedSource<T> splitAtFraction(double fraction) {
      Metrics.counter(BigQueryStorageStreamReader.class, "split-at-fraction-calls").inc();
      LOG.debug(
          "Received BigQuery Storage API split request for stream {} at fraction {}.",
          source.readStream.getName(),
          fraction);

      if (fraction <= 0.0 || fraction >= 1.0) {
        LOG.info("BigQuery Storage API does not support splitting at fraction {}", fraction);
        return null;
      }

      SplitReadStreamRequest splitRequest =
          SplitReadStreamRequest.newBuilder()
              .setName(source.readStream.getName())
              .setFraction((float) fraction)
              .build();

      SplitReadStreamResponse splitResponse = storageClient.splitReadStream(splitRequest);
      if (!splitResponse.hasPrimaryStream() || !splitResponse.hasRemainderStream()) {
        // No more splits are possible!
        Metrics.counter(
                BigQueryStorageStreamReader.class,
                "split-at-fraction-calls-failed-due-to-impossible-split-point")
            .inc();
        LOG.info(
            "BigQuery Storage API stream {} cannot be split at {}.",
            source.readStream.getName(),
            fraction);
        return null;
      }

      // We may be able to split this source. Before continuing, we pause the reader thread and
      // replace its current source with the primary stream iff the reader has not moved past
      // the split point.
      synchronized (this) {
        BigQueryServerStream<ReadRowsResponse> newResponseStream;
        Iterator<ReadRowsResponse> newResponseIterator;
        try {
          newResponseStream =
              storageClient.readRows(
                  ReadRowsRequest.newBuilder()
                      .setReadStream(splitResponse.getPrimaryStream().getName())
                      .setOffset(currentOffset + 1)
                      .build());
          newResponseIterator = newResponseStream.iterator();
          newResponseIterator.hasNext();
        } catch (FailedPreconditionException e) {
          // The current source has already moved past the split point, so this split attempt
          // is unsuccessful.
          Metrics.counter(
                  BigQueryStorageStreamReader.class,
                  "split-at-fraction-calls-failed-due-to-bad-split-point")
              .inc();
          LOG.info(
              "BigQuery Storage API split of stream {} abandoned because the primary stream is to "
                  + "the left of the split fraction {}.",
              source.readStream.getName(),
              fraction);
          return null;
        } catch (Exception e) {
          Metrics.counter(
                  BigQueryStorageStreamReader.class,
                  "split-at-fraction-calls-failed-due-to-other-reasons")
              .inc();
          LOG.error("BigQuery Storage API stream split failed.", e);
          return null;
        }

        // Cancels the parent stream before replacing it with the primary stream.
        responseStream.cancel();
        source = source.fromExisting(splitResponse.getPrimaryStream());
        responseStream = newResponseStream;
        responseIterator = newResponseIterator;

        // N.B.: Once #readNextRecord is called, this line has the effect of using the fraction
        // consumed value at split time as the fraction consumed value of the previous response,
        // leading to a better interpolation window start. Unfortunately, this is not the best value
        // as it will lead to a significant speed up in the fraction consumed values while the first
        // post-split response is being processed. In the future, if the server returns the start
        // and end fraction consumed values in each response, then these interpolations will be
        // easier to perform as state from the previous response will not need to be maintained.
        fractionConsumedFromCurrentResponse = fractionConsumed;

        decoder = null;
      }

      Metrics.counter(BigQueryStorageStreamReader.class, "split-at-fraction-calls-successful")
          .inc();
      LOG.info(
          "Successfully split BigQuery Storage API stream at {}. Split response: {}",
          fraction,
          splitResponse);
      return source.fromExisting(splitResponse.getRemainderStream());
    }

    @Override
    public synchronized Double getFractionConsumed() {
      return fractionConsumed;
    }

    private static float getFractionConsumed(ReadRowsResponse response) {
      // TODO(kmj): Make this work.
      // return response.getStatus().getFractionConsumed();
      return 0;
    }
  }
}
