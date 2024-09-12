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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.StreamController;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.ByteStringComparator;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.SettableFuture;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link BigtableService} that actually communicates with the Cloud Bigtable
 * service.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableServiceImpl implements BigtableService {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableServiceImpl.class);

  // Default byte limit is a percentage of the JVM's available memory
  private static final double DEFAULT_BYTE_LIMIT_PERCENTAGE = .1;
  // Percentage of max number of rows allowed in the buffer
  private static final double WATERMARK_PERCENTAGE = .1;
  private static final long MIN_BYTE_BUFFER_SIZE = 100 * 1024 * 1024; // 100MB

  BigtableServiceImpl(BigtableDataSettings settings) throws IOException {
    this.projectId = settings.getProjectId();
    this.instanceId = settings.getInstanceId();
    this.client = BigtableDataClient.create(settings);
    LOG.info("Started Bigtable service with settings {}", settings);
  }

  private final BigtableDataClient client;
  private final String projectId;
  private final String instanceId;

  @Override
  public BigtableWriterImpl openForWriting(BigtableWriteOptions writeOptions) {
    return new BigtableWriterImpl(
        client,
        projectId,
        instanceId,
        writeOptions.getTableId().get(),
        writeOptions.getCloseWaitTimeout());
  }

  @VisibleForTesting
  static class BigtableReaderImpl implements Reader {
    private final BigtableDataClient client;

    private final String projectId;
    private final String instanceId;
    private final String tableId;

    private final List<ByteKeyRange> ranges;
    private final RowFilter rowFilter;
    private Iterator<Row> results;

    private Row currentRow;

    private ServerStream<Row> stream;

    private boolean exhausted;

    @VisibleForTesting
    BigtableReaderImpl(
        BigtableDataClient client,
        String projectId,
        String instanceId,
        String tableId,
        List<ByteKeyRange> ranges,
        @Nullable RowFilter rowFilter) {
      this.client = client;
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
      this.ranges = ranges;
      this.rowFilter = rowFilter;
    }

    @Override
    public boolean start() throws IOException {
      ServiceCallMetric serviceCallMetric = createCallMetric(projectId, instanceId, tableId);

      Query query = Query.create(tableId);
      for (ByteKeyRange sourceRange : ranges) {
        query.range(
            ByteString.copyFrom(sourceRange.getStartKey().getValue()),
            ByteString.copyFrom(sourceRange.getEndKey().getValue()));
      }

      if (rowFilter != null) {
        query.filter(Filters.FILTERS.fromProto(rowFilter));
      }
      try {
        stream =
            client
                .readRowsCallable(new BigtableRowProtoAdapter())
                .call(query, GrpcCallContext.createDefault());
        results = stream.iterator();
        serviceCallMetric.call("ok");
      } catch (StatusRuntimeException e) {
        serviceCallMetric.call(e.getStatus().getCode().toString());
        throw e;
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (results.hasNext()) {
        currentRow = results.next();
        return true;
      }
      exhausted = true;
      return false;
    }

    @Override
    public Row getCurrentRow() throws NoSuchElementException {
      if (currentRow == null) {
        throw new NoSuchElementException();
      }
      return currentRow;
    }

    @Override
    public void close() {
      if (!exhausted) {
        stream.cancel();
        exhausted = true;
      }
    }

    @Override
    public void reportLineage() {
      Lineage.getSources().add("bigtable", ImmutableList.of(projectId, instanceId, tableId));
    }
  }

  @VisibleForTesting
  static class BigtableSegmentReaderImpl implements Reader {
    private final BigtableDataClient client;

    private @Nullable ReadRowsRequest nextRequest;
    private @Nullable Row currentRow;
    private @Nullable Future<UpstreamResults> future;
    private final Queue<Row> buffer;
    private final int refillSegmentWaterMark;
    private final long maxSegmentByteSize;
    private ServiceCallMetric serviceCallMetric;
    private final String projectId;
    private final String instanceId;
    private final String tableId;

    private static class UpstreamResults {
      private final List<Row> rows;
      private final @Nullable ReadRowsRequest nextRequest;

      private UpstreamResults(List<Row> rows, @Nullable ReadRowsRequest nextRequest) {
        this.rows = rows;
        this.nextRequest = nextRequest;
      }
    }

    static BigtableSegmentReaderImpl create(
        BigtableDataClient client,
        String projectId,
        String instanceId,
        String tableId,
        List<ByteKeyRange> ranges,
        @Nullable RowFilter rowFilter,
        int maxBufferedElementCount) {

      RowSet.Builder rowSetBuilder = RowSet.newBuilder();
      if (ranges.isEmpty()) {
        rowSetBuilder = RowSet.newBuilder().addRowRanges(RowRange.getDefaultInstance());
      } else {
        // BigtableSource only contains ranges with a closed start key and open end key
        for (ByteKeyRange beamRange : ranges) {
          RowRange.Builder rangeBuilder = rowSetBuilder.addRowRangesBuilder();
          rangeBuilder
              .setStartKeyClosed(ByteString.copyFrom(beamRange.getStartKey().getValue()))
              .setEndKeyOpen(ByteString.copyFrom(beamRange.getEndKey().getValue()));
        }
      }
      RowSet rowSet = rowSetBuilder.build();
      RowFilter filter = MoreObjects.firstNonNull(rowFilter, RowFilter.getDefaultInstance());

      long maxSegmentByteSize =
          (long)
              Math.max(
                  MIN_BYTE_BUFFER_SIZE,
                  (Runtime.getRuntime().totalMemory() * DEFAULT_BYTE_LIMIT_PERCENTAGE));

      return new BigtableSegmentReaderImpl(
          client,
          projectId,
          instanceId,
          tableId,
          rowSet,
          filter,
          maxBufferedElementCount,
          maxSegmentByteSize,
          createCallMetric(projectId, instanceId, tableId));
    }

    @VisibleForTesting
    BigtableSegmentReaderImpl(
        BigtableDataClient client,
        String projectId,
        String instanceId,
        String tableId,
        RowSet rowSet,
        @Nullable RowFilter filter,
        int maxRowsInBuffer,
        long maxSegmentByteSize,
        ServiceCallMetric serviceCallMetric) {
      if (rowSet.equals(rowSet.getDefaultInstanceForType())) {
        rowSet = RowSet.newBuilder().addRowRanges(RowRange.getDefaultInstance()).build();
      }
      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setTableName(NameUtil.formatTableName(projectId, instanceId, tableId))
              .setRows(rowSet)
              .setFilter(filter)
              .setRowsLimit(maxRowsInBuffer)
              .build();

      this.client = client;
      this.nextRequest = request;
      this.maxSegmentByteSize = maxSegmentByteSize;
      this.serviceCallMetric = serviceCallMetric;
      this.buffer = new ArrayDeque<>();
      // Asynchronously refill buffer when there is 10% of the elements are left
      this.refillSegmentWaterMark =
          Math.max(1, (int) (request.getRowsLimit() * WATERMARK_PERCENTAGE));
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
    }

    @Override
    public void close() {}

    @Override
    public void reportLineage() {
      Lineage.getSources().add("bigtable", ImmutableList.of(projectId, instanceId, tableId));
    }

    @Override
    public boolean start() throws IOException {
      future = fetchNextSegment();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (future != null && future.isDone()) {
        // Add rows from the future to the buffer and reset the future
        // so we can do prefetching
        consumeReadRowsFuture();
      }
      if (buffer.size() < refillSegmentWaterMark && future == null) {
        future = fetchNextSegment();
      }
      if (buffer.isEmpty() && future != null) {
        consumeReadRowsFuture();
      }
      currentRow = buffer.poll();
      return currentRow != null;
    }

    private Future<UpstreamResults> fetchNextSegment() {
      SettableFuture<UpstreamResults> future = SettableFuture.create();
      // When the nextRequest is null, the last fill completed and the buffer contains the last rows
      if (nextRequest == null) {
        future.set(new UpstreamResults(ImmutableList.of(), null));
        return future;
      }

      client
          .readRowsCallable(new BigtableRowProtoAdapter())
          .call(
              Query.fromProto(nextRequest),
              new ResponseObserver<Row>() {
                private StreamController controller;

                List<Row> rows = new ArrayList<>();

                long currentByteSize = 0;
                boolean byteLimitReached = false;

                @Override
                public void onStart(StreamController controller) {
                  this.controller = controller;
                }

                @Override
                public void onResponse(Row response) {
                  // calculate size of the response
                  currentByteSize += response.getSerializedSize();
                  rows.add(response);
                  if (currentByteSize > maxSegmentByteSize) {
                    LOG.debug(
                        "Reached maxSegmentByteSize, cancelling the stream. currentByteSize is {}, maxSegmentByteSize is {}, read rows {}",
                        currentByteSize,
                        maxSegmentByteSize,
                        rows.size());
                    byteLimitReached = true;
                    controller.cancel();
                    return;
                  }
                }

                @Override
                public void onError(Throwable t) {
                  if (byteLimitReached) {
                    // When the byte limit is reached we cancel the stream in onResponse.
                    // In this case we don't want to fail the request with cancellation
                    // exception. Instead, we construct the next request.
                    onComplete();
                  } else {
                    future.setException(t);
                  }
                }

                @Override
                public void onComplete() {
                  ReadRowsRequest nextNextRequest = null;

                  // Only schedule the next segment fetch when there's a possibility of more
                  // data to read. We know there might be more data when the current segment
                  // ended with the artificial byte limit or the row limit.
                  // If the RPC ended without hitting the byte limit or row limit, we know
                  // there's no more data to read and nextNextRequest would be null.
                  if (byteLimitReached || rows.size() == nextRequest.getRowsLimit()) {
                    nextNextRequest =
                        truncateRequest(nextRequest, rows.get(rows.size() - 1).getKey());
                  }
                  future.set(new UpstreamResults(rows, nextNextRequest));
                }
              },
              GrpcCallContext.createDefault());
      return future;
    }

    private void consumeReadRowsFuture() throws IOException {
      try {
        UpstreamResults r = future.get();
        buffer.addAll(r.rows);
        nextRequest = r.nextRequest;
        future = null;
        serviceCallMetric.call("ok");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof StatusRuntimeException) {
          serviceCallMetric.call(((StatusRuntimeException) cause).getStatus().getCode().toString());
        }
        throw new IOException(cause);
      }
    }

    private ReadRowsRequest truncateRequest(ReadRowsRequest request, ByteString lastKey) {
      RowSet.Builder segment = RowSet.newBuilder();

      for (RowRange rowRange : request.getRows().getRowRangesList()) {
        int startCmp = StartPoint.extract(rowRange).compareTo(new StartPoint(lastKey, true));
        int endCmp = EndPoint.extract(rowRange).compareTo(new EndPoint(lastKey, true));

        if (endCmp <= 0) {
          // range end is on or left of the split: skip
          continue;
        }

        RowRange.Builder newRange = rowRange.toBuilder();
        if (startCmp > 0) {
          // If the startKey is passed the split point than add the whole range
          segment.addRowRanges(newRange.build());
        } else {
          // Row is split, remove all read rowKeys and split RowSet at last buffered Row
          segment.addRowRanges(newRange.setStartKeyOpen(lastKey).build());
        }
      }
      if (segment.getRowRangesCount() == 0) {
        return null;
      }

      ReadRowsRequest.Builder requestBuilder = request.toBuilder();
      requestBuilder.clearRows();
      return requestBuilder.setRows(segment).build();
    }

    @Override
    public Row getCurrentRow() throws NoSuchElementException {
      if (currentRow == null) {
        throw new NoSuchElementException();
      }
      return currentRow;
    }
  }

  @VisibleForTesting
  static class BigtableWriterImpl implements Writer {
    private Batcher<RowMutationEntry, Void> bulkMutation;

    private BigtableDataClient client;
    private Integer outstandingMutations = 0;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();
    private String projectId;
    private String instanceId;
    private String tableId;
    private Duration closeWaitTimeout;

    private Distribution bulkSize = Metrics.distribution("BigTable-" + tableId, "batchSize");
    private Distribution latency = Metrics.distribution("BigTable-" + tableId, "batchLatencyMs");

    BigtableWriterImpl(
        BigtableDataClient client,
        String projectId,
        String instanceId,
        String tableId,
        Duration closeWaitTimeout) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
      this.closeWaitTimeout = closeWaitTimeout;
      this.bulkMutation = client.newBulkMutationBatcher(tableId);
      this.client = client;
    }

    @Override
    public void close() throws IOException {
      if (bulkMutation != null) {
        try {
          stopwatch.start();
          // closeAsync will send any remaining elements in the batch.
          // If the experimental close wait timeout flag is set,
          // set a timeout waiting for the future.
          ApiFuture<Void> future = bulkMutation.closeAsync();
          if (Duration.ZERO.isShorterThan(closeWaitTimeout)) {
            future.get(closeWaitTimeout.getMillis(), TimeUnit.MILLISECONDS);
          } else {
            future.get();
          }
          bulkSize.update(outstandingMutations);
          outstandingMutations = 0;
          stopwatch.stop();
          latency.update(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (BatchingException e) {
          // Ignore batching failures because element failures are tracked as is in
          // BigtableIOWriteFn.
          // TODO: Bigtable client already tracks BatchingExceptions, use BatchingExceptions
          // instead of tracking them separately in BigtableIOWriteFn.
        } catch (TimeoutException e) {
          // We fail because future.get() timed out
          throw new IOException("BulkMutation took too long to close", e);
        } catch (ExecutionException e) {
          throw new IOException("Failed to close batch", e.getCause());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // We fail since close() operation was interrupted.
          throw new IOException(e);
        }
        bulkMutation = null;
      }
    }

    @Override
    public CompletionStage<MutateRowResponse> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException {

      com.google.cloud.bigtable.data.v2.models.Mutation mutation =
          com.google.cloud.bigtable.data.v2.models.Mutation.fromProtoUnsafe(record.getValue());

      RowMutationEntry entry = RowMutationEntry.createFromMutationUnsafe(record.getKey(), mutation);

      ServiceCallMetric serviceCallMetric = createServiceCallMetric();

      CompletableFuture<MutateRowResponse> result = new CompletableFuture<>();

      outstandingMutations += 1;
      Futures.addCallback(
          new VendoredListenableFutureAdapter<>(bulkMutation.add(entry)),
          new WriteMutationCallback(result, serviceCallMetric),
          directExecutor());
      return result;
    }

    @Override
    public void writeSingleRecord(KV<ByteString, Iterable<Mutation>> record) throws ApiException {
      com.google.cloud.bigtable.data.v2.models.Mutation mutation =
          com.google.cloud.bigtable.data.v2.models.Mutation.fromProtoUnsafe(record.getValue());

      RowMutation rowMutation = RowMutation.create(tableId, record.getKey(), mutation);

      ServiceCallMetric serviceCallMetric = createServiceCallMetric();

      try {
        client.mutateRow(rowMutation);
        serviceCallMetric.call("ok");
      } catch (ApiException e) {
        if (e.getCause() instanceof StatusRuntimeException) {
          serviceCallMetric.call(
              ((StatusRuntimeException) e.getCause()).getStatus().getCode().value());
        } else {
          serviceCallMetric.call("unknown");
        }
        throw e;
      }
    }

    @Override
    public void reportLineage() {
      Lineage.getSinks().add("bigtable", ImmutableList.of(projectId, instanceId, tableId));
    }

    private ServiceCallMetric createServiceCallMetric() {
      // Populate metrics
      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.MutateRows");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.bigtableResource(projectId, instanceId, tableId));
      baseLabels.put(MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, projectId);
      baseLabels.put(MonitoringInfoConstants.Labels.INSTANCE_ID, instanceId);
      baseLabels.put(
          MonitoringInfoConstants.Labels.TABLE_ID,
          GcpResourceIdentifiers.bigtableTableID(projectId, instanceId, tableId));
      return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    }

    private static class WriteMutationCallback implements FutureCallback<MutateRowResponse> {
      private final CompletableFuture<MutateRowResponse> result;

      private final ServiceCallMetric serviceCallMetric;

      public WriteMutationCallback(
          CompletableFuture<MutateRowResponse> result, ServiceCallMetric serviceCallMetric) {
        this.result = result;
        this.serviceCallMetric = serviceCallMetric;
      }

      @Override
      public void onSuccess(MutateRowResponse mutateRowResponse) {
        result.complete(mutateRowResponse);
        serviceCallMetric.call("ok");
      }

      @Override
      public void onFailure(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
          serviceCallMetric.call(
              ((StatusRuntimeException) throwable).getStatus().getCode().value());
        } else if (throwable instanceof DeadlineExceededException) {
          // incoming throwable can be a StatusRuntimeException or a specific grpc ApiException
          serviceCallMetric.call(504);
        } else {
          serviceCallMetric.call("unknown");
        }
        result.completeExceptionally(throwable);
      }
    }
  }

  @Override
  public Reader createReader(BigtableSource source) throws IOException {
    if (source.getMaxBufferElementCount() != null) {
      return BigtableSegmentReaderImpl.create(
          client,
          projectId,
          instanceId,
          source.getTableId().get(),
          source.getRanges(),
          source.getRowFilter(),
          source.getMaxBufferElementCount());
    } else {
      return new BigtableReaderImpl(
          client,
          projectId,
          instanceId,
          source.getTableId().get(),
          source.getRanges(),
          source.getRowFilter());
    }
  }

  @Override
  public List<KeyOffset> getSampleRowKeys(BigtableSource source) {
    return client.sampleRowKeys(source.getTableId().get());
  }

  public static ServiceCallMetric createCallMetric(
      String projectId, String instanceId, String tableId) {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.ReadRows");
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.bigtableResource(projectId, instanceId, tableId));
    baseLabels.put(MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, projectId);
    baseLabels.put(MonitoringInfoConstants.Labels.INSTANCE_ID, instanceId);
    baseLabels.put(
        MonitoringInfoConstants.Labels.TABLE_ID,
        GcpResourceIdentifiers.bigtableTableID(projectId, instanceId, tableId));
    return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
  }

  @Override
  public void close() {
    client.close();
  }

  /** Helper class to ease comparison of RowRange start points. */
  private static final class StartPoint implements Comparable<StartPoint> {
    private final ByteString value;
    private final boolean isClosed;

    @NonNull
    static StartPoint extract(@NonNull RowRange rowRange) {
      switch (rowRange.getStartKeyCase()) {
        case STARTKEY_NOT_SET:
          return new StartPoint(ByteString.EMPTY, true);
        case START_KEY_CLOSED:
          return new StartPoint(rowRange.getStartKeyClosed(), true);
        case START_KEY_OPEN:
          if (rowRange.getStartKeyOpen().isEmpty()) {
            // Take care to normalize an open empty start key to be closed.
            return new StartPoint(ByteString.EMPTY, true);
          } else {
            return new StartPoint(rowRange.getStartKeyOpen(), false);
          }
        default:
          throw new IllegalArgumentException("Unknown startKeyCase: " + rowRange.getStartKeyCase());
      }
    }

    private StartPoint(@NonNull ByteString value, boolean isClosed) {
      this.value = value;
      this.isClosed = isClosed;
    }

    @Override
    public int compareTo(@NonNull StartPoint o) {
      return ComparisonChain.start()
          // Empty string comes first
          .compareTrueFirst(value.isEmpty(), o.value.isEmpty())
          .compare(value, o.value, ByteStringComparator.INSTANCE)
          // Closed start point comes before an open start point: [x,y] starts before (x,y].
          .compareTrueFirst(isClosed, o.isClosed)
          .result();
    }
  }

  /** Helper class to ease comparison of RowRange endpoints. */
  private static final class EndPoint implements Comparable<EndPoint> {
    private final ByteString value;
    private final boolean isClosed;

    @NonNull
    static EndPoint extract(@NonNull RowRange rowRange) {
      switch (rowRange.getEndKeyCase()) {
        case ENDKEY_NOT_SET:
          return new EndPoint(ByteString.EMPTY, true);
        case END_KEY_CLOSED:
          return new EndPoint(rowRange.getEndKeyClosed(), true);
        case END_KEY_OPEN:
          if (rowRange.getEndKeyOpen().isEmpty()) {
            // Take care to normalize an open empty end key to be closed.
            return new EndPoint(ByteString.EMPTY, true);
          } else {
            return new EndPoint(rowRange.getEndKeyOpen(), false);
          }
        default:
          throw new IllegalArgumentException("Unknown endKeyCase: " + rowRange.getEndKeyCase());
      }
    }

    private EndPoint(@NonNull ByteString value, boolean isClosed) {
      this.value = value;
      this.isClosed = isClosed;
    }

    @Override
    public int compareTo(@NonNull EndPoint o) {
      return ComparisonChain.start()
          // Empty string comes last
          .compareFalseFirst(value.isEmpty(), o.value.isEmpty())
          .compare(value, o.value, ByteStringComparator.INSTANCE)
          // Open end point comes before a closed end point: [x,y) ends before [x,y].
          .compareFalseFirst(isClosed, o.isClosed)
          .result();
    }
  }

  static class BigtableRowProtoAdapter implements RowAdapter<com.google.bigtable.v2.Row> {
    @Override
    public RowBuilder<com.google.bigtable.v2.Row> createRowBuilder() {
      return new DefaultRowBuilder();
    }

    @Override
    public boolean isScanMarkerRow(com.google.bigtable.v2.Row row) {
      return Objects.equals(row, com.google.bigtable.v2.Row.getDefaultInstance());
    }

    @Override
    public ByteString getKey(com.google.bigtable.v2.Row row) {
      return row.getKey();
    }

    private static class DefaultRowBuilder
        implements RowAdapter.RowBuilder<com.google.bigtable.v2.Row> {
      private com.google.bigtable.v2.Row.Builder protoBuilder =
          com.google.bigtable.v2.Row.newBuilder();

      private @Nullable ByteString currentValue;
      private Family.@Nullable Builder lastFamily;
      private @Nullable String lastFamilyName;
      private Column.@Nullable Builder lastColumn;
      private @Nullable ByteString lastColumnName;
      private Cell.@Nullable Builder lastCell;

      @Override
      public void startRow(ByteString key) {
        protoBuilder.setKey(key);

        lastFamilyName = null;
        lastFamily = null;
        lastColumnName = null;
        lastColumn = null;
      }

      @Override
      public void startCell(
          String family, ByteString qualifier, long timestamp, List<String> labels, long size) {
        boolean familyChanged = false;

        if (!family.equals(lastFamilyName)) {
          familyChanged = true;
          lastFamily = protoBuilder.addFamiliesBuilder().setName(family);
          lastFamilyName = family;
        }
        if (!qualifier.equals(lastColumnName) || familyChanged) {
          lastColumn = lastFamily.addColumnsBuilder().setQualifier(qualifier);
          lastColumnName = qualifier;
        }
        lastCell = lastColumn.addCellsBuilder().setTimestampMicros(timestamp).addAllLabels(labels);
        currentValue = null;
      }

      @Override
      public void cellValue(ByteString value) {
        if (currentValue == null) {
          currentValue = value;
        } else {
          currentValue = currentValue.concat(value);
        }
      }

      @Override
      public void finishCell() {
        lastCell.setValue(currentValue);
      }

      @Override
      public com.google.bigtable.v2.Row finishRow() {
        return protoBuilder.build();
      }

      @Override
      public void reset() {
        lastFamilyName = null;
        lastFamily = null;
        lastColumnName = null;
        lastColumn = null;
        currentValue = null;

        protoBuilder = com.google.bigtable.v2.Row.newBuilder();
      }

      @Override
      public com.google.bigtable.v2.Row createScanMarkerRow(ByteString key) {
        return com.google.bigtable.v2.Row.newBuilder().getDefaultInstanceForType();
      }
    }
  }
}
