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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.rpc.ApiException;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.BigQueryServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BigQueryStorageStreamSourceV2<T> extends BoundedSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageStreamSourceV2.class);

  public static <T> BigQueryStorageStreamSourceV2<T> create(
      ReadSession readSession,
      List<ReadStream> streamBundle,
      TableSchema tableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageStreamSourceV2<>(
        readSession,
        streamBundle,
        toJsonString(Preconditions.checkArgumentNotNull(tableSchema, "tableSchema")),
        parseFn,
        outputCoder,
        bqServices);
  }

  /**
   * Creates a new source with the same properties as this one, except with a different {@link
   * List<ReadStream>}.
   */
  public BigQueryStorageStreamSourceV2<T> fromExisting(List<ReadStream> newStreamBundle) {
    return new BigQueryStorageStreamSourceV2<>(
        readSession, newStreamBundle, jsonTableSchema, parseFn, outputCoder, bqServices);
  }

  private final ReadSession readSession;
  private final List<ReadStream> streamBundle;
  private final String jsonTableSchema;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamSourceV2(
      ReadSession readSession,
      List<ReadStream> streamBundle,
      String jsonTableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.readSession = Preconditions.checkArgumentNotNull(readSession, "readSession");
    this.streamBundle = Preconditions.checkArgumentNotNull(streamBundle, "streams");
    this.jsonTableSchema = Preconditions.checkArgumentNotNull(jsonTableSchema, "jsonTableSchema");
    this.parseFn = Preconditions.checkArgumentNotNull(parseFn, "parseFn");
    this.outputCoder = Preconditions.checkArgumentNotNull(outputCoder, "outputCoder");
    this.bqServices = Preconditions.checkArgumentNotNull(bqServices, "bqServices");
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
        .add(DisplayData.item("readSession", readSession.getName()).withLabel("Read session"));
    for (ReadStream readStream : streamBundle) {
      builder.add(DisplayData.item("stream", readStream.getName()).withLabel("Stream"));
    }
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
  public BigQueryStorageStreamBundleReader<T> createReader(PipelineOptions options)
      throws IOException {
    return new BigQueryStorageStreamBundleReader<>(this, options.as(BigQueryOptions.class));
  }

  // @Override
  // public String toString() {
  //   return readStream.toString();
  // }

  public static class BigQueryStorageStreamBundleReader<T> extends BoundedSource.BoundedReader<T> {
    private final BigQueryStorageReader reader;
    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final StorageClient storageClient;
    private final TableSchema tableSchema;

    private BigQueryStorageStreamSourceV2<T> source;
    private @Nullable BigQueryServerStream<ReadRowsResponse> responseStream = null;
    private @Nullable Iterator<ReadRowsResponse> responseIterator = null;
    private @Nullable T current = null;
    private int currentStreamIndex;
    private long currentOffset;

    // Values used for progress reporting.
    private double fractionConsumed;
    private double progressAtResponseStart;
    private double progressAtResponseEnd;
    private long rowsConsumedFromCurrentResponse;
    private long totalRowsInCurrentResponse;

    private @Nullable TableReference tableReference;
    private @Nullable ServiceCallMetric serviceCallMetric;

    private BigQueryStorageStreamBundleReader(
        BigQueryStorageStreamSourceV2<T> source, BigQueryOptions options) throws IOException {
      this.source = source;
      this.reader = BigQueryStorageReaderFactory.getReader(source.readSession);
      this.parseFn = source.parseFn;
      this.storageClient = source.bqServices.getStorageClient(options);
      this.tableSchema = fromJsonString(source.jsonTableSchema, TableSchema.class);
      this.currentStreamIndex = 0;
      this.fractionConsumed = 0d;
      this.progressAtResponseStart = 0d;
      this.progressAtResponseEnd = 0d;
      this.rowsConsumedFromCurrentResponse = 0L;
      this.totalRowsInCurrentResponse = 0L;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public synchronized boolean start() throws IOException {
      return readNextStream();
    }

    @Override
    public synchronized boolean advance() throws IOException {
      Preconditions.checkStateNotNull(responseIterator);
      currentOffset++;
      return readNextRecord();
    }

    private synchronized boolean readNextStream() throws IOException {
      BigQueryStorageStreamSourceV2<T> source = getCurrentSource();
      if (currentStreamIndex == source.streamBundle.size() - 1) {
        fractionConsumed = 1d;
        return false;
      }
      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadStream(source.streamBundle.get(currentStreamIndex).getName())
              .setOffset(currentOffset)
              .build();

      tableReference = BigQueryUtils.toTableReference(source.readSession.getTable());
      serviceCallMetric = BigQueryUtils.readCallMetric(tableReference);
      LOG.info(
          "Started BigQuery Storage API read from stream {}.",
          source.streamBundle.get(0).getName());
      responseStream = storageClient.readRows(request, source.readSession.getTable());
      responseIterator = responseStream.iterator();
      return readNextRecord();
    }

    @RequiresNonNull("responseIterator")
    private synchronized boolean readNextRecord() throws IOException {
      Iterator<ReadRowsResponse> responseIterator = this.responseIterator;
      while (reader.readyForNextReadResponse()) {
        if (!responseIterator.hasNext()) {
          currentStreamIndex++;
          return readNextStream();
        }

        ReadRowsResponse response;
        try {
          response = responseIterator.next();
          // Since we don't have a direct hook to the underlying
          // API call, record success every time we read a record successfully.
          if (serviceCallMetric != null) {
            serviceCallMetric.call("ok");
          }
        } catch (ApiException e) {
          // Occasionally the iterator will fail and raise an exception.
          // Capture it here and record the error in the metric.
          if (serviceCallMetric != null) {
            serviceCallMetric.call(e.getStatusCode().getCode().name());
          }
          throw e;
        }

        progressAtResponseStart = response.getStats().getProgress().getAtResponseStart();
        progressAtResponseEnd = response.getStats().getProgress().getAtResponseEnd();
        totalRowsInCurrentResponse = response.getRowCount();
        rowsConsumedFromCurrentResponse = 0L;

        checkArgument(
            totalRowsInCurrentResponse >= 0,
            "Row count from current response (%s) must be non-negative.",
            totalRowsInCurrentResponse);

        checkArgument(
            0f <= progressAtResponseStart && progressAtResponseStart <= 1f,
            "Progress at response start (%s) is not in the range [0.0, 1.0].",
            progressAtResponseStart);

        checkArgument(
            0f <= progressAtResponseEnd && progressAtResponseEnd <= 1f,
            "Progress at response end (%s) is not in the range [0.0, 1.0].",
            progressAtResponseEnd);

        reader.processReadRowsResponse(response);
      }

      SchemaAndRecord schemaAndRecord = new SchemaAndRecord(reader.readSingleRecord(), tableSchema);

      current = parseFn.apply(schemaAndRecord);

      // Updates the fraction consumed value. This value is calculated by interpolating between
      // the fraction consumed value from the previous server response (or zero if we're consuming
      // the first response) and the fractional value in the current response based on how many of
      // the rows in the current response have been consumed.
      rowsConsumedFromCurrentResponse++;

      fractionConsumed =
          progressAtResponseStart
              + (progressAtResponseEnd - progressAtResponseStart)
                  * rowsConsumedFromCurrentResponse
                  * 1.0
                  / totalRowsInCurrentResponse;

      // Assuming that each stream in the StreamBundle has approximately the same amount of data.
      fractionConsumed = fractionConsumed / source.streamBundle.size();

      return true;
    }

    @Override
    public synchronized void close() {
      // Because superclass cannot have preconditions around these variables, cannot use
      // @RequiresNonNull
      Preconditions.checkStateNotNull(storageClient);
      Preconditions.checkStateNotNull(reader);
      storageClient.close();
      reader.close();
    }

    @Override
    public synchronized BigQueryStorageStreamSourceV2<T> getCurrentSource() {
      return source;
    }

    @Override
    @SuppressWarnings("ReturnValueIgnored")
    public @Nullable BoundedSource<T> splitAtFraction(double fraction) {
      int streamCountInBundle = source.streamBundle.size();
      double splitIndex = streamCountInBundle * fraction;
      if (currentStreamIndex > splitIndex) {
        // The reader has moved past the requested split point.
        Metrics.counter(
                BigQueryStorageStreamBundleReader.class,
                "split-at-fraction-calls-failed-due-to-impossible-split-point")
            .inc();
        LOG.info(
            "BigQuery Storage API Session {} cannot be split at {}.",
            source.readSession.getName(),
            fraction);
        return null;
      }
      // Splitting the remainder Streams into a new StreamBundle.
      List<ReadStream> remainderStreamBundle =
          new ArrayList<>(
              source.streamBundle.subList((int) Math.ceil(splitIndex), streamCountInBundle));
      // Updating the primary StreamBundle.
      source.streamBundle.subList((int) Math.ceil(splitIndex), streamCountInBundle).clear();
      Metrics.counter(BigQueryStorageStreamBundleReader.class, "split-at-fraction-calls-successful")
          .inc();
      LOG.info("Successfully split BigQuery Storage API StreamBundle at {}.", fraction);
      return source.fromExisting(remainderStreamBundle);
    }

    @Override
    public synchronized Double getFractionConsumed() {
      return fractionConsumed;
    }
  }
}
