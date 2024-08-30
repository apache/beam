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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.services.bigquery.model.TableReference;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.BigQueryServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing a single stream in a read session. */
class BigQueryStorageStreamSource<T> extends BoundedSource<T> {

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
        toJsonString(Preconditions.checkArgumentNotNull(tableSchema, "tableSchema")),
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

  public BigQueryStorageStreamSource<T> fromExisting(
      SerializableFunction<SchemaAndRecord, T> parseFn) {
    return new BigQueryStorageStreamSource<>(
        readSession, readStream, jsonTableSchema, parseFn, outputCoder, bqServices);
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
    this.readSession = Preconditions.checkArgumentNotNull(readSession, "readSession");
    this.readStream = Preconditions.checkArgumentNotNull(readStream, "stream");
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
  public static class BigQueryStorageStreamReader<T> extends BoundedSource.BoundedReader<T> {

    private final BigQueryStorageReader reader;
    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final StorageClient storageClient;
    private final TableSchema tableSchema;

    private BigQueryStorageStreamSource<T> source;
    private @Nullable BigQueryServerStream<ReadRowsResponse> responseStream = null;
    private @Nullable Iterator<ReadRowsResponse> responseIterator = null;
    private @Nullable T current = null;
    private long currentOffset;

    // Values used for progress reporting.
    private boolean splitPossible = true;
    private boolean splitAllowed = true;
    private double fractionConsumed;
    private double progressAtResponseStart;
    private double progressAtResponseEnd;
    private long rowsConsumedFromCurrentResponse;
    private long totalRowsInCurrentResponse;

    private @Nullable TableReference tableReference;
    private @Nullable ServiceCallMetric serviceCallMetric;

    // Initialize metrics.
    private final Counter totalSplitCalls =
        Metrics.counter(BigQueryStorageStreamReader.class, "split-at-fraction-calls");
    private final Counter impossibleSplitPointCalls =
        Metrics.counter(
            BigQueryStorageStreamReader.class,
            "split-at-fraction-calls-failed-due-to-impossible-split-point");
    private final Counter badSplitPointCalls =
        Metrics.counter(
            BigQueryStorageStreamReader.class,
            "split-at-fraction-calls-failed-due-to-bad-split-point");
    private final Counter otherFailedSplitCalls =
        Metrics.counter(
            BigQueryStorageStreamReader.class,
            "split-at-fraction-calls-failed-due-to-other-reasons");
    private final Counter successfulSplitCalls =
        Metrics.counter(BigQueryStorageStreamReader.class, "split-at-fraction-calls-successful");
    private static final ExecutorService executor = Executors.newCachedThreadPool();

    private BigQueryStorageStreamReader(
        BigQueryStorageStreamSource<T> source, BigQueryOptions options) throws IOException {
      this.source = source;
      this.reader = BigQueryStorageReaderFactory.getReader(source.readSession);
      this.parseFn = source.parseFn;
      this.storageClient = source.bqServices.getStorageClient(options);
      this.tableSchema = fromJsonString(source.jsonTableSchema, TableSchema.class);
      // number of stream determined from server side for storage read api v2
      this.splitAllowed = !options.getEnableStorageReadApiV2();
      this.fractionConsumed = 0d;
      this.progressAtResponseStart = 0d;
      this.progressAtResponseEnd = 0d;
      this.rowsConsumedFromCurrentResponse = 0L;
      this.totalRowsInCurrentResponse = 0L;
    }

    @Override
    public synchronized boolean start() throws IOException {
      BigQueryStorageStreamSource<T> source = getCurrentSource();

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadStream(source.readStream.getName())
              .setOffset(currentOffset)
              .build();

      tableReference = BigQueryUtils.toTableReference(source.readSession.getTable());
      serviceCallMetric = BigQueryUtils.readCallMetric(tableReference);
      LOG.info("Started BigQuery Storage API read from stream {}.", source.readStream.getName());
      responseStream = storageClient.readRows(request, source.readSession.getTable());
      responseIterator = responseStream.iterator();
      return readNextRecord();
    }

    @Override
    public synchronized boolean advance() throws IOException {
      Preconditions.checkStateNotNull(responseIterator);
      currentOffset++;
      return readNextRecord();
    }

    @RequiresNonNull("responseIterator")
    private synchronized boolean readNextRecord() throws IOException {
      Iterator<ReadRowsResponse> responseIterator = this.responseIterator;
      while (reader.readyForNextReadResponse()) {
        boolean previous = splitAllowed;
        // disallow splitAtFraction (where it also calls hasNext) when iterator busy
        splitAllowed = false;
        boolean hasNext = responseIterator.hasNext();
        splitAllowed = previous;
        // hasNext call has internal retry. Record throttling metrics after called
        storageClient.reportPendingMetrics();

        if (!hasNext) {
          fractionConsumed = 1d;
          return false;
        }

        ReadRowsResponse response;
        try {
          response = responseIterator.next();
          // Since we don't have a direct hook to the underlying
          // API call, record success ever time we read a record successfully.
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

      SchemaAndRecord schemaAndRecord = new SchemaAndRecord(reader.readSingleRecord(), tableSchema);

      current = parseFn.apply(schemaAndRecord);

      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
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
    public synchronized BigQueryStorageStreamSource<T> getCurrentSource() {
      return source;
    }

    @Override
    @SuppressWarnings("ReturnValueIgnored")
    public @Nullable BoundedSource<T> splitAtFraction(double fraction) {
      // Because superclass cannot have preconditions around these variables, cannot use
      // @RequiresNonNull
      Preconditions.checkStateNotNull(responseStream);
      final BigQueryServerStream<ReadRowsResponse> responseStream = this.responseStream;
      totalSplitCalls.inc();
      LOG.debug(
          "Received BigQuery Storage API split request for stream {} at fraction {}.",
          source.readStream.getName(),
          fraction);

      if (fraction <= 0.0 || fraction >= 1.0) {
        LOG.info("BigQuery Storage API does not support splitting at fraction {}", fraction);
        return null;
      }

      if (!splitPossible || !splitAllowed) {
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
        impossibleSplitPointCalls.inc();
        LOG.info(
            "BigQuery Storage API stream {} cannot be split at {}.",
            source.readStream.getName(),
            fraction);
        splitPossible = false;
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
                      .build(),
                  source.readSession.getTable());
          newResponseIterator = newResponseStream.iterator();
          // The following line is required to trigger the `FailedPreconditionException` on which
          // the SplitReadStream validation logic depends. Removing it will cause incorrect
          // split operations to succeed.
          Future<Boolean> future = executor.submit(newResponseIterator::hasNext);
          try {
            // The intended wait time is in sync with splitReadStreamSettings.setRetrySettings in
            // StorageClientImpl.
            future.get(30, TimeUnit.SECONDS);
          } catch (TimeoutException | InterruptedException | ExecutionException e) {
            badSplitPointCalls.inc();
            LOG.info(
                "Split of stream {} abandoned because current position check failed with {}.",
                source.readStream.getName(),
                e.getClass().getName());

            return null;
          } finally {
            future.cancel(true);
          }
        } catch (FailedPreconditionException e) {
          // The current source has already moved past the split point, so this split attempt
          // is unsuccessful.
          badSplitPointCalls.inc();
          LOG.info(
              "BigQuery Storage API split of stream {} abandoned because the primary stream is to "
                  + "the left of the split fraction {}.",
              source.readStream.getName(),
              fraction);
          return null;
        } catch (Exception e) {
          otherFailedSplitCalls.inc();
          LOG.error("BigQuery Storage API stream split failed.", e);
          return null;
        }

        // Cancels the parent stream before replacing it with the primary stream.
        responseStream.cancel();
        this.source = source.fromExisting(splitResponse.getPrimaryStream());
        this.responseStream = newResponseStream;
        this.responseIterator = newResponseIterator;
        reader.resetBuffer();
      }

      successfulSplitCalls.inc();
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
  }
}
