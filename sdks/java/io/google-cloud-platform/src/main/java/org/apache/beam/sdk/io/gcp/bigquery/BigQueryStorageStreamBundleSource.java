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
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.BigQueryServerStream;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.beam.sdk.io.Source} representing a bundle of Streams in a BigQuery ReadAPI
 * Session. This Source ONLY supports splitting at the StreamBundle level.
 *
 * <p>{@link BigQueryStorageStreamBundleSource} defines a split-point as the starting offset of each
 * Stream. As a result, the number of valid split points in the Source is equal to the number of
 * Streams in the StreamBundle and this Source does NOT support sub-Stream splitting.
 *
 * <p>Additionally, the underlying {@link org.apache.beam.sdk.io.range.OffsetRangeTracker} and
 * {@link OffsetBasedSource} operate in the split point space and do NOT directly interact with the
 * Streams constituting the StreamBundle. Consequently, fractional values used in
 * `splitAtFraction()` are translated into StreamBundleIndices and the underlying RangeTracker
 * handles the split operation by checking the validity of the split point. This has the following
 * implications for the `splitAtFraction()` operation:
 *
 * <p>1. Fraction values that point to the "middle" of a Stream will be translated to the
 * appropriate Stream boundary by the RangeTracker.
 *
 * <p>2. Once a Stream is being read from, the RangeTracker will only accept `splitAtFraction()`
 * calls that point to StreamBundleIndices that are greater than the StreamBundleIndex of the
 * current Stream
 *
 * @param <T> Type of records represented by the source.
 * @see OffsetBasedSource
 * @see org.apache.beam.sdk.io.range.OffsetRangeTracker
 * @see org.apache.beam.sdk.io.BlockBasedSource (semantically similar to {@link
 *     BigQueryStorageStreamBundleSource})
 */
class BigQueryStorageStreamBundleSource<T> extends OffsetBasedSource<T> {

  public static <T> BigQueryStorageStreamBundleSource<T> create(
      ReadSession readSession,
      List<ReadStream> streamBundle,
      TableSchema tableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices,
      long minBundleSize) {
    return new BigQueryStorageStreamBundleSource<>(
        readSession,
        streamBundle,
        toJsonString(Preconditions.checkArgumentNotNull(tableSchema, "tableSchema")),
        parseFn,
        outputCoder,
        bqServices,
        minBundleSize);
  }

  /**
   * Creates a new source with the same properties as this one, except with a different {@link
   * List<ReadStream>}.
   */
  public BigQueryStorageStreamBundleSource<T> fromExisting(List<ReadStream> newStreamBundle) {
    return new BigQueryStorageStreamBundleSource<>(
        readSession,
        newStreamBundle,
        jsonTableSchema,
        parseFn,
        outputCoder,
        bqServices,
        getMinBundleSize());
  }

  private final ReadSession readSession;
  private final List<ReadStream> streamBundle;
  private final String jsonTableSchema;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;

  private BigQueryStorageStreamBundleSource(
      ReadSession readSession,
      List<ReadStream> streamBundle,
      String jsonTableSchema,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices,
      long minBundleSize) {
    super(0, streamBundle.size(), minBundleSize);
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
  public List<? extends OffsetBasedSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    // This method is only called for initial splits. Since this class will always be a child source
    // of BigQueryStorageSourceBase, all splits here will be handled by `splitAtFraction()`. As a
    // result, this is a no-op.
    return ImmutableList.of(this);
  }

  @Override
  public long getMaxEndOffset(PipelineOptions options) throws Exception {
    return this.streamBundle.size();
  }

  @Override
  public OffsetBasedSource<T> createSourceForSubrange(long start, long end) {
    List<ReadStream> newStreamBundle = streamBundle.subList((int) start, (int) end);
    return fromExisting(newStreamBundle);
  }

  @Override
  public BigQueryStorageStreamBundleReader<T> createReader(PipelineOptions options)
      throws IOException {
    return new BigQueryStorageStreamBundleReader<>(this, options.as(BigQueryOptions.class));
  }

  public static class BigQueryStorageStreamBundleReader<T> extends OffsetBasedReader<T> {
    private static final Logger LOG =
        LoggerFactory.getLogger(BigQueryStorageStreamBundleReader.class);

    private final BigQueryStorageReader reader;
    private final SerializableFunction<SchemaAndRecord, T> parseFn;
    private final StorageClient storageClient;
    private final TableSchema tableSchema;

    private BigQueryStorageStreamBundleSource<T> source;
    private @Nullable BigQueryServerStream<ReadRowsResponse> responseStream = null;
    private @Nullable Iterator<ReadRowsResponse> responseIterator = null;
    private @Nullable T current = null;
    private int currentStreamBundleIndex;
    private long currentStreamOffset;

    // Values used for progress reporting.
    private double fractionOfStreamBundleConsumed;

    private double progressAtResponseStart;
    private double progressAtResponseEnd;
    private long rowsConsumedFromCurrentResponse;
    private long totalRowsInCurrentResponse;

    private @Nullable TableReference tableReference;
    private @Nullable ServiceCallMetric serviceCallMetric;

    private BigQueryStorageStreamBundleReader(
        BigQueryStorageStreamBundleSource<T> source, BigQueryOptions options) throws IOException {
      super(source);
      this.source = source;
      this.reader = BigQueryStorageReaderFactory.getReader(source.readSession);
      this.parseFn = source.parseFn;
      this.storageClient = source.bqServices.getStorageClient(options);
      this.tableSchema = fromJsonString(source.jsonTableSchema, TableSchema.class);
      this.currentStreamBundleIndex = 0;
      this.fractionOfStreamBundleConsumed = 0d;
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
    protected long getCurrentOffset() throws NoSuchElementException {
      return currentStreamBundleIndex;
    }

    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      if (currentStreamOffset == 0) {
        return true;
      }
      return false;
    }

    @Override
    public boolean startImpl() throws IOException {
      return readNextStream();
    }

    @Override
    public boolean advanceImpl() throws IOException {
      Preconditions.checkStateNotNull(responseIterator);
      currentStreamOffset += totalRowsInCurrentResponse;
      return readNextRecord();
    }

    private boolean readNextStream() throws IOException {
      BigQueryStorageStreamBundleSource<T> source = getCurrentSource();
      if (currentStreamBundleIndex == source.streamBundle.size()) {
        fractionOfStreamBundleConsumed = 1d;
        return false;
      }
      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadStream(source.streamBundle.get(currentStreamBundleIndex).getName())
              .build();
      tableReference = BigQueryUtils.toTableReference(source.readSession.getTable());
      serviceCallMetric = BigQueryUtils.readCallMetric(tableReference);
      LOG.info(
          "Started BigQuery Storage API read from stream {}.",
          source.streamBundle.get(currentStreamBundleIndex).getName());
      responseStream = storageClient.readRows(request, source.readSession.getTable());
      responseIterator = responseStream.iterator();
      return readNextRecord();
    }

    @RequiresNonNull("responseIterator")
    private boolean readNextRecord() throws IOException {
      Iterator<ReadRowsResponse> responseIterator = this.responseIterator;
      if (responseIterator == null) {
        LOG.info("Received null responseIterator for stream {}", currentStreamBundleIndex);
        return false;
      }
      while (reader.readyForNextReadResponse()) {
        if (!responseIterator.hasNext()) {
          synchronized (this) {
            currentStreamOffset = 0;
            currentStreamBundleIndex++;
          }
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

      // Calculates the fraction of the current stream that has been consumed. This value is
      // calculated by interpolating between the fraction consumed value from the previous server
      // response (or zero if we're consuming the first response) and the fractional value in the
      // current response based on how many of the rows in the current response have been consumed.
      rowsConsumedFromCurrentResponse++;

      double fractionOfCurrentStreamConsumed =
          progressAtResponseStart
              + ((progressAtResponseEnd - progressAtResponseStart)
                  * (rowsConsumedFromCurrentResponse * 1.0 / totalRowsInCurrentResponse));

      // We now calculate the progress made over the entire StreamBundle by assuming that each
      // Stream in the StreamBundle has approximately the same amount of data. Given this, merely
      // counting the number of Streams that have been read and linearly interpolating with the
      // progress made in the current Stream gives us the overall StreamBundle progress.
      fractionOfStreamBundleConsumed =
          (currentStreamBundleIndex + fractionOfCurrentStreamConsumed) / source.streamBundle.size();
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
    public synchronized BigQueryStorageStreamBundleSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public synchronized Double getFractionConsumed() {
      return fractionOfStreamBundleConsumed;
    }
  }
}
