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

import static org.apache.beam.sdk.io.gcp.bigtable.BigtableHBaseVeneeringSettings.BigtableIOOperationTimeouts;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.api.gax.batching.Batcher;
import com.google.api.gax.grpc.GrpcCallContext;
import com.google.api.gax.rpc.ResponseObserver;
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
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.internal.ByteStringComparator;
import com.google.cloud.bigtable.data.v2.internal.NameUtil;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Deadline;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.SettableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;
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

  public BigtableServiceImpl(BigtableOptions options) {
    try {
      this.options = options;
      this.veneeringSettings = BigtableHBaseVeneeringSettings.create(options);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final BigtableOptions options;
  private final BigtableHBaseVeneeringSettings veneeringSettings;

  @Override
  public BigtableOptions getBigtableOptions() {
    return options;
  }

  @Override
  public BigtableWriterImpl openForWriting(String tableId) throws IOException {
    LOG.info("Opening for writing with settings " + veneeringSettings.getDataSettings().toString());
    return new BigtableWriterImpl(
        BigtableDataClient.create(veneeringSettings.getDataSettings()),
        veneeringSettings.getDataSettings(),
        tableId);
  }

  @Override
  public boolean tableExists(String tableId) throws IOException {
    try (BigtableTableAdminClient adminClient =
        BigtableTableAdminClient.create(veneeringSettings.getTableAdminSettings())) {
      adminClient.getTable(tableId);
      return true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Code.NOT_FOUND) {
        return false;
      }
      String message =
          String.format(
              "Error checking whether table %s (BigtableOptions %s) exists", tableId, options);
      LOG.error(message, e);
      throw new IOException(message, e);
    }
  }

  @VisibleForTesting
  static class BigtableReaderImpl implements Reader {
    private BigtableDataSettings settings;
    private BigtableDataClient client;
    private final BigtableSource source;
    private Iterator<Row> results;
    private BigtableIOOperationTimeouts operationTimeouts;

    private Row currentRow;

    @VisibleForTesting
    BigtableReaderImpl(
        BigtableDataClient client,
        BigtableDataSettings settings,
        BigtableSource source,
        BigtableIOOperationTimeouts operationTimeouts) {
      this.settings = settings;
      this.client = client;
      this.source = source;
      this.operationTimeouts = operationTimeouts;
    }

    @Override
    public boolean start() throws IOException {
      ServiceCallMetric serviceCallMetric =
          createCallMetric(
              settings.getProjectId(), settings.getInstanceId(), source.getTableId().get());

      Query query = Query.create(source.getTableId().get());
      for (ByteKeyRange sourceRange : source.getRanges()) {
        query.range(
            ByteString.copyFrom(sourceRange.getStartKey().getValue()),
            ByteString.copyFrom(sourceRange.getEndKey().getValue()));
      }

      if (source.getRowFilter() != null) {
        query.filter(Filters.FILTERS.fromProto(source.getRowFilter()));
      }
      try {
        results =
            client
                .readRowsCallable(new BeamRowAdapter())
                .call(query, BigtableServiceImpl.createScanCallContext(operationTimeouts))
                .iterator();
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
      return false;
    }

    @Override
    public void close() throws IOException {
      // Goal: by the end of this function, both results and session are null and closed,
      // independent of what errors they throw or prior state.

      if (client == null) {
        // Only possible when previously closed, so we know that results is also null.
        return;
      }

      client.close();
      client = null;
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
  static class BigtableSegmentReaderImpl implements Reader {
    private BigtableDataClient client;
    private @Nullable ReadRowsRequest nextRequest;
    private @Nullable Row currentRow;
    private @Nullable Future<UpstreamResults> future;
    private final Queue<Row> buffer;
    private final int refillSegmentWaterMark;
    private final long maxSegmentByteSize;
    private ServiceCallMetric serviceCallMetric;
    private BigtableIOOperationTimeouts operationTimeouts;

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
        BigtableDataSettings settings,
        BigtableSource source,
        BigtableIOOperationTimeouts operationTimeouts) {

      RowSet.Builder rowSetBuilder = RowSet.newBuilder();
      if (source.getRanges().isEmpty()) {
        rowSetBuilder = RowSet.newBuilder().addRowRanges(RowRange.getDefaultInstance());
      } else {
        // BigtableSource only contains ranges with a closed start key and open end key
        for (ByteKeyRange beamRange : source.getRanges()) {
          RowRange.Builder rangeBuilder = rowSetBuilder.addRowRangesBuilder();
          rangeBuilder
              .setStartKeyClosed(ByteString.copyFrom(beamRange.getStartKey().getValue()))
              .setEndKeyOpen(ByteString.copyFrom(beamRange.getEndKey().getValue()));
        }
      }
      RowSet rowSet = rowSetBuilder.build();
      RowFilter filter =
          MoreObjects.firstNonNull(source.getRowFilter(), RowFilter.getDefaultInstance());

      long maxSegmentByteSize =
          (long)
              Math.max(
                  MIN_BYTE_BUFFER_SIZE,
                  (Runtime.getRuntime().totalMemory() * DEFAULT_BYTE_LIMIT_PERCENTAGE));

      return new BigtableSegmentReaderImpl(
          client,
          settings,
          source.getTableId().get(),
          rowSet,
          source.getMaxBufferElementCount(),
          maxSegmentByteSize,
          filter,
          createCallMetric(
              settings.getProjectId(), settings.getInstanceId(), source.getTableId().get()),
          operationTimeouts);
    }

    @VisibleForTesting
    BigtableSegmentReaderImpl(
        BigtableDataClient client,
        BigtableDataSettings settings,
        String tableId,
        RowSet rowSet,
        int maxRowsInBuffer,
        long maxSegmentByteSize,
        RowFilter filter,
        ServiceCallMetric serviceCallMetric,
        BigtableIOOperationTimeouts operationTimeouts) {
      if (rowSet.equals(rowSet.getDefaultInstanceForType())) {
        rowSet = RowSet.newBuilder().addRowRanges(RowRange.getDefaultInstance()).build();
      }
      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setTableName(
                  NameUtil.formatTableName(
                      settings.getProjectId(), settings.getInstanceId(), tableId))
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
      this.refillSegmentWaterMark = (int) (request.getRowsLimit() * WATERMARK_PERCENTAGE);
      this.operationTimeouts = operationTimeouts;
    }

    @Override
    public boolean start() throws IOException {
      future = fetchNextSegment();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (buffer.size() < refillSegmentWaterMark && future == null) {
        future = fetchNextSegment();
      }
      if (buffer.isEmpty() && future != null) {
        waitReadRowsFuture();
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
          .readRowsCallable(new BeamRowAdapter())
          .call(
              Query.fromProto(nextRequest),
              new ResponseObserver<com.google.bigtable.v2.Row>() {
                private StreamController controller;

                List<com.google.bigtable.v2.Row> rows = new ArrayList<>();

                long currentByteSize = 0;
                boolean byteLimitReached = false;

                @Override
                public void onStart(StreamController controller) {
                  this.controller = controller;
                }

                @Override
                public void onResponse(com.google.bigtable.v2.Row response) {
                  // calculate size of the response
                  currentByteSize += response.getSerializedSize();
                  rows.add(response);
                  if (currentByteSize > maxSegmentByteSize) {
                    byteLimitReached = true;
                    controller.cancel();
                    return;
                  }
                }

                @Override
                public void onError(Throwable t) {
                  future.setException(t);
                }

                @Override
                public void onComplete() {
                  ReadRowsRequest nextNextRequest = null;

                  // When requested rows < limit, the current request will be the last
                  if (byteLimitReached || rows.size() == nextRequest.getRowsLimit()) {
                    nextNextRequest =
                        truncateRequest(nextRequest, rows.get(rows.size() - 1).getKey());
                  }
                  future.set(new UpstreamResults(rows, nextNextRequest));
                }
              },
              createScanCallContext(operationTimeouts));
      return future;
    }

    private void waitReadRowsFuture() throws IOException {
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

        if (startCmp > 0) {
          // If the startKey is passed the split point than add the whole range
          segment.addRowRanges(rowRange);
        } else if (endCmp > 0) {
          // Row is split, remove all read rowKeys and split RowSet at last buffered Row
          RowRange subRange = rowRange.toBuilder().setStartKeyOpen(lastKey).build();
          segment.addRowRanges(subRange);
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
    public void close() throws IOException {
      client.close();
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
    private BigtableDataClient client;
    private BigtableDataSettings settings;
    private Batcher<RowMutationEntry, Void> bulkMutation;
    private String tableId;
    private GrpcCallContext context = GrpcCallContext.createDefault();

    BigtableWriterImpl(BigtableDataClient client, BigtableDataSettings settings, String tableId) {
      this.client = client;
      this.settings = settings;
      this.tableId = tableId;
      this.bulkMutation = client.newBulkMutationBatcher(tableId, context);
    }

    @Override
    public void flush() throws IOException {
      if (bulkMutation != null) {
        try {
          bulkMutation.flush();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // We fail since flush() operation was interrupted.
          throw new IOException(e);
        }
      }
    }

    @Override
    public void close() throws IOException {
      try {
        if (bulkMutation != null) {
          try {
            bulkMutation.flush();
            bulkMutation.close();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // We fail since flush() operation was interrupted.
            throw new IOException(e);
          }
          bulkMutation = null;
        }
      } finally {
        if (client != null) {
          client.close();
          client = null;
        }
      }
    }

    @Override
    public CompletionStage<MutateRowResponse> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException {

      com.google.cloud.bigtable.data.v2.models.Mutation mutation =
          com.google.cloud.bigtable.data.v2.models.Mutation.fromProtoUnsafe(record.getValue());

      RowMutationEntry entry = RowMutationEntry.createFromMutationUnsafe(record.getKey(), mutation);

      // Populate metrics
      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.MutateRows");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.bigtableResource(
              settings.getProjectId(), settings.getInstanceId(), tableId));
      baseLabels.put(MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, settings.getProjectId());
      baseLabels.put(MonitoringInfoConstants.Labels.INSTANCE_ID, settings.getInstanceId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.TABLE_ID,
          GcpResourceIdentifiers.bigtableTableID(
              settings.getProjectId(), settings.getInstanceId(), tableId));
      ServiceCallMetric serviceCallMetric =
          new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);

      CompletableFuture<MutateRowResponse> result = new CompletableFuture<>();

      Futures.addCallback(
          new VendoredListenableFutureAdapter<>(bulkMutation.add(entry)),
          new FutureCallback<MutateRowResponse>() {
            @Override
            public void onSuccess(MutateRowResponse mutateRowResponse) {
              // TODO throttling logic to update dataflow counter
              // long throttledTime = ((ApiCallContext)
              // context).getOption(Batcher.THROTTLED_TIME_KEY);

              result.complete(mutateRowResponse);
              serviceCallMetric.call("ok");
            }

            @Override
            public void onFailure(Throwable throwable) {
              // TODO throttling logic to update dataflow counter
              // long throttledTime = ((ApiCallContext)
              // context).getOption(Batcher.THROTTLED_TIME_KEY);

              if (throwable instanceof StatusRuntimeException) {
                serviceCallMetric.call(
                    ((StatusRuntimeException) throwable).getStatus().getCode().value());
              } else {
                serviceCallMetric.call("unknown");
              }
              result.completeExceptionally(throwable);
            }
          },
          directExecutor());
      return result;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(BigtableServiceImpl.class).add("options", options).toString();
  }

  @Override
  public Reader createReader(BigtableSource source) throws IOException {
    LOG.info(
        "Creating a Reader for Bigtable with settings: " + veneeringSettings.getDataSettings());
    BigtableDataClient client = BigtableDataClient.create(veneeringSettings.getDataSettings());
    if (source.getMaxBufferElementCount() != null) {
      return BigtableSegmentReaderImpl.create(
          client,
          veneeringSettings.getDataSettings(),
          source,
          veneeringSettings.getOperationTimeouts());
    } else {
      return new BigtableReaderImpl(
          client,
          veneeringSettings.getDataSettings(),
          source,
          veneeringSettings.getOperationTimeouts());
    }
  }

  // Support 2 bigtable-hbase features not directly available in veneer:
  // - disabling timeouts - when timeouts are disabled, bigtable-hbase ignores user configured
  //   timeouts and forces 6 minute deadlines per attempt for all RPCs except scans. This is
  //   implemented by an interceptor. However the interceptor must be informed that this is a scan
  // - per attempt deadlines - vener doesn't implement deadlines for attempts. To workaround this,
  //   the timeouts are set per call in the ApiCallContext. However this creates a separate issue of
  //   over running the operation deadline, so gRPC deadline is also set.
  private static GrpcCallContext createScanCallContext(
      BigtableIOOperationTimeouts operationTimeouts) {
    GrpcCallContext ctx = GrpcCallContext.createDefault();

    if (!operationTimeouts.getUseTimeouts()) {
      ctx =
          ctx.withCallOptions(
              CallOptions.DEFAULT.withOption(
                  BigtableHBaseVeneeringSettings.NoTimeoutsInterceptor.SKIP_DEFAULT_ATTEMPT_TIMEOUT,
                  true));
    } else {
      if (operationTimeouts.getBulkReadRowsTimeouts().getOperationTimeout().isPresent()) {
        ctx.withCallOptions(
            CallOptions.DEFAULT.withDeadline(
                Deadline.after(
                    operationTimeouts
                        .getBulkMutateTimeouts()
                        .getOperationTimeout()
                        .get()
                        .toMillis(),
                    TimeUnit.MILLISECONDS)));
      }
      if (operationTimeouts.getBulkReadRowsTimeouts().getAttemptTimeout().isPresent()) {
        ctx =
            ctx.withTimeout(operationTimeouts.getBulkReadRowsTimeouts().getAttemptTimeout().get());
      }
    }
    return ctx;
  }

  @Override
  public List<KeyOffset> getSampleRowKeys(BigtableSource source) throws IOException {
    try (BigtableDataClient client =
        BigtableDataClient.create(veneeringSettings.getDataSettings())) {
      return client.sampleRowKeys(source.getTableId().get());
    }
  }

  @VisibleForTesting
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

  /** Helper class to ease comparison of RowRange start points. */
  private static final class StartPoint implements Comparable<StartPoint> {
    private final ByteString value;
    private final boolean isClosed;

    @Nonnull
    static StartPoint extract(@Nonnull RowRange rowRange) {
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

    private StartPoint(@Nonnull ByteString value, boolean isClosed) {
      this.value = value;
      this.isClosed = isClosed;
    }

    @Override
    public int compareTo(@Nonnull StartPoint o) {
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

    @Nonnull
    static EndPoint extract(@Nonnull RowRange rowRange) {
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

    private EndPoint(@Nonnull ByteString value, boolean isClosed) {
      this.value = value;
      this.isClosed = isClosed;
    }

    @Override
    public int compareTo(@Nonnull EndPoint o) {
      return ComparisonChain.start()
          // Empty string comes last
          .compareFalseFirst(value.isEmpty(), o.value.isEmpty())
          .compare(value, o.value, ByteStringComparator.INSTANCE)
          // Open end point comes before a closed end point: [x,y) ends before [x,y].
          .compareFalseFirst(isClosed, o.isClosed)
          .result();
    }
  }

  static class BeamRowAdapter implements RowAdapter<com.google.bigtable.v2.Row> {
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

      private TreeMap<String, TreeMap<ByteString, ImmutableList.Builder<Cell>>>
          cellsByFamilyColumn = new TreeMap<>();
      private TreeMap<ByteString, ImmutableList.Builder<Cell>> cellsByColumn =
          new TreeMap<>(Comparator.comparing(o -> o.toString(StandardCharsets.UTF_8)));
      private ImmutableList.Builder<Cell> currentColumnCells;

      private ByteString qualifier;
      private ByteString previousQualifier;
      private String family;
      private String previousFamily;

      private ByteString value;
      private List<String> labels;
      private long timestamp;

      @Override
      public void startRow(ByteString key) {
        protoBuilder.setKey(key);
      }

      @Override
      public void startCell(
          String family, ByteString qualifier, long timestamp, List<String> labels, long size) {
        this.family = family;
        this.qualifier = qualifier;
        this.timestamp = timestamp;
        this.labels = labels;
        this.value = ByteString.EMPTY;
      }

      @Override
      public void cellValue(ByteString value) {
        this.value = this.value.concat(value);
      }

      @Override
      public void finishCell() {
        if (!qualifier.equals(previousQualifier)) {
          previousQualifier = qualifier;
          currentColumnCells = ImmutableList.builder();
          cellsByColumn.put(qualifier, currentColumnCells);
        }
        if (!family.equals(previousFamily)) {
          previousFamily = family;
          this.cellsByFamilyColumn.put(family, cellsByColumn);
        }

        Cell cell =
            Cell.newBuilder()
                .setValue(value)
                .addAllLabels(labels)
                .setTimestampMicros(timestamp)
                .build();
        currentColumnCells.add(cell);
      }

      @Override
      public com.google.bigtable.v2.Row finishRow() {
        for (String family : cellsByFamilyColumn.keySet()) {
          Family.Builder f = Family.newBuilder().setName(family);
          for (ByteString column : cellsByFamilyColumn.get(family).keySet()) {
            Column c =
                Column.newBuilder()
                    .setQualifier(column)
                    .addAllCells(cellsByFamilyColumn.get(family).get(column).build())
                    .build();
            f.addColumns(c);
          }
          protoBuilder.addFamilies(f);
        }
        return protoBuilder.build();
      }

      @Override
      public void reset() {
        this.qualifier = null;
        this.previousQualifier = null;
        this.family = null;
        this.previousFamily = null;

        protoBuilder = com.google.bigtable.v2.Row.newBuilder();

        this.cellsByColumn.clear();
        this.cellsByFamilyColumn.clear();
      }

      @Override
      public com.google.bigtable.v2.Row createScanMarkerRow(ByteString key) {
        return com.google.bigtable.v2.Row.newBuilder().getDefaultInstanceForType();
      }
    }
  }
}
