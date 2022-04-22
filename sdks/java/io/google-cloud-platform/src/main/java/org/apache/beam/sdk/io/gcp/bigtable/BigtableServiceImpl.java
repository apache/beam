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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.bigtable.admin.v2.GetTableRequest;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.MutateRowsRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.bigtable.v2.SampleRowKeysRequest;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.FlatRowConverter;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.grpc.scanner.ScanHandler;
import com.google.cloud.bigtable.util.ByteStringComparator;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link BigtableService} that actually communicates with the Cloud Bigtable
 * service.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class BigtableServiceImpl implements BigtableService {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableServiceImpl.class);
  private static final int DEFAULT_SEGMENT_SIZE = 100;
  // Default byte limit is a percentage of the JVM's free memory
  private static final double DEFAULT_BYTE_LIMIT_PERCENTAGE = .8;

  public BigtableServiceImpl(BigtableOptions options) {
    this.options = options;
  }

  private final BigtableOptions options;

  @Override
  public BigtableOptions getBigtableOptions() {
    return options;
  }

  @Override
  public BigtableWriterImpl openForWriting(String tableId) throws IOException {
    BigtableSession session = new BigtableSession(options);
    BigtableTableName tableName = options.getInstanceName().toTableName(tableId);
    return new BigtableWriterImpl(session, tableName);
  }

  @Override
  public boolean tableExists(String tableId) throws IOException {
    try (BigtableSession session = new BigtableSession(options)) {
      GetTableRequest getTable =
          GetTableRequest.newBuilder()
              .setName(options.getInstanceName().toTableNameStr(tableId))
              .build();
      session.getTableAdminClient().getTable(getTable);
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
    private BigtableSession session;
    private final BigtableSource source;
    private ResultScanner<Row> results;
    private Row currentRow;

    @VisibleForTesting
    BigtableReaderImpl(BigtableSession session, BigtableSource source) {
      this.session = session;
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      RowSet.Builder rowSetBuilder = RowSet.newBuilder();
      for (ByteKeyRange sourceRange : source.getRanges()) {
        rowSetBuilder =
            rowSetBuilder.addRowRanges(
                RowRange.newBuilder()
                    .setStartKeyClosed(ByteString.copyFrom(sourceRange.getStartKey().getValue()))
                    .setEndKeyOpen(ByteString.copyFrom(sourceRange.getEndKey().getValue())));
      }
      RowSet rowSet = rowSetBuilder.build();

      String tableNameSr =
          session.getOptions().getInstanceName().toTableNameStr(source.getTableId().get());

      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.ReadRows");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.bigtableResource(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              source.getTableId().get()));
      baseLabels.put(
          MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, session.getOptions().getProjectId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.INSTANCE_ID, session.getOptions().getInstanceId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.TABLE_ID,
          GcpResourceIdentifiers.bigtableTableID(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              source.getTableId().get()));
      ServiceCallMetric serviceCallMetric =
          new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
      ReadRowsRequest.Builder requestB =
          ReadRowsRequest.newBuilder().setRows(rowSet).setTableName(tableNameSr);
      if (source.getRowFilter() != null) {
        requestB.setFilter(source.getRowFilter());
      }
      try {
        results = session.getDataClient().readRows(requestB.build());
        serviceCallMetric.call("ok");
      } catch (StatusRuntimeException e) {
        serviceCallMetric.call(e.getStatus().getCode().value());
        throw e;
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      currentRow = results.next();
      return currentRow != null;
    }

    @Override
    public void close() throws IOException {
      // Goal: by the end of this function, both results and session are null and closed,
      // independent of what errors they throw or prior state.

      if (session == null) {
        // Only possible when previously closed, so we know that results is also null.
        return;
      }

      // Session does not implement Closeable -- it's AutoCloseable. So we can't register it with
      // the Closer, but we can use the Closer to simplify the error handling.
      try (Closer closer = Closer.create()) {
        if (results != null) {
          closer.register(results);
          results = null;
        }

        session.close();
      } finally {
        session = null;
      }
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
    private BigtableSession session;
    private final BigtableSource source;
    private Row currentRow;
    private Queue<FlatRow> buffer;
    private RowSet rowSet;
    private ServiceCallMetric serviceCallMetric;
    private Future<ImmutablePair<List<FlatRow>, Boolean>> future;
    private ByteString lastFetchedRow;
    private boolean lastFillComplete;
    private boolean byteLimitReached;
    private long bufferByteLimit;

    private final int segmentLimit;
    private final int segmentWaterMark;
    private final String tableName;

    @VisibleForTesting
    BigtableSegmentReaderImpl(BigtableSession session, BigtableSource source) {
      this.session = session;
      if (source.getMaxBufferElementCount() != null && source.getMaxBufferElementCount() != 0) {
        this.segmentLimit = source.getMaxBufferElementCount();
      } else {
        this.segmentLimit = DEFAULT_SEGMENT_SIZE;
      }
      this.bufferByteLimit =
          (long) (DEFAULT_BYTE_LIMIT_PERCENTAGE * Runtime.getRuntime().freeMemory());
      // Asynchronously refill buffer when there is 10% of the elements are left
      this.segmentWaterMark = segmentLimit / 10;
      tableName = session.getOptions().getInstanceName().toTableNameStr(source.getTableId().get());
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      buffer = new ArrayDeque<>();
      lastFillComplete = false;
      byteLimitReached = false;
      RowRange[] rowRanges = new RowRange[source.getRanges().size()];
      for (int i = 0; i < source.getRanges().size(); i++) {
        rowRanges[i] =
            RowRange.newBuilder()
                .setStartKeyClosed(
                    ByteString.copyFrom(source.getRanges().get(i).getStartKey().getValue()))
                .setEndKeyOpen(
                    ByteString.copyFrom(source.getRanges().get(i).getEndKey().getValue()))
                .build();
      }
      // Presort the ranges so that future segmentation can exit early when splitting the row set
      Arrays.sort(rowRanges, RANGE_START_COMPARATOR);
      rowSet =
          RowSet.newBuilder()
              .addAllRowRanges(Arrays.stream(rowRanges).collect(Collectors.toList()))
              .build();

      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.ReadRows");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.bigtableResource(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              source.getTableId().get()));
      baseLabels.put(
          MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, session.getOptions().getProjectId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.INSTANCE_ID, session.getOptions().getInstanceId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.TABLE_ID,
          GcpResourceIdentifiers.bigtableTableID(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              source.getTableId().get()));
      serviceCallMetric =
          new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);

      future = startNextSegmentRead();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (buffer.size() <= segmentWaterMark && future == null && !lastFillComplete) {
        if (!splitRowSet(lastFetchedRow)) {
          return false;
        }
        future = startNextSegmentRead();
      }
      if (buffer.isEmpty()) {
        if (future == null || lastFillComplete) return false;
        waitReadRowsFuture();
      }
      // If the last fill is equal to row limit, the lastFillComplete flag will not be true
      // until another RPC is called which will return 0 rows
      if (buffer.isEmpty() && lastFillComplete) {
        return false;
      }
      currentRow = FlatRowConverter.convert(buffer.remove());
      return currentRow != null;
    }

    private SettableFuture<ImmutablePair<List<FlatRow>, Boolean>> startNextSegmentRead() {
      SettableFuture<ImmutablePair<List<FlatRow>, Boolean>> f = SettableFuture.create();
      bufferByteLimit = (long) (DEFAULT_BYTE_LIMIT_PERCENTAGE * Runtime.getRuntime().freeMemory());
      // TODO(diegomez): Remove atomic ScanHandler for simpler StreamObserver/Future implementation
      AtomicReference<ScanHandler> atomic = new AtomicReference<>();
      ScanHandler handler;
      handler =
          session
              .getDataClient()
              .readFlatRows(
                  buildReadRowsRequest(),
                  new StreamObserver<FlatRow>() {
                    List<FlatRow> rows = new ArrayList<>();
                    long currentByteSize = 0;
                    boolean byteLimitReached = false;

                    @Override
                    public void onNext(FlatRow flatRow) {
                      rows.add(flatRow);
                      currentByteSize +=
                          flatRow.getRowKey().size()
                              + flatRow.getCells().stream()
                                  .mapToLong(c -> c.getQualifier().size() + c.getValue().size())
                                  .sum();
                      if (currentByteSize > bufferByteLimit) {
                        byteLimitReached = true;
                        atomic.get().cancel();
                        return;
                      }
                    }

                    @Override
                    public void onError(Throwable e) {
                      f.setException(e);
                    }

                    @Override
                    public void onCompleted() {
                      f.set(ImmutablePair.of(rows, byteLimitReached));
                      return;
                    }
                  });
      atomic.set(handler);
      return f;
    }

    private ReadRowsRequest buildReadRowsRequest() {
      ReadRowsRequest.Builder request =
          ReadRowsRequest.newBuilder()
              .setRows(rowSet)
              .setRowsLimit(segmentLimit)
              .setTableName(tableName);
      if (source.getRowFilter() != null) {
        request.setFilter(source.getRowFilter());
      }
      return request.build();
    }

    private void waitReadRowsFuture() throws IOException {
      try {
        ImmutablePair<List<FlatRow>, Boolean> results = future.get();
        byteLimitReached = results.getRight();
        future = null;
        serviceCallMetric.call("ok");
        fillReadRowsBuffer(results.getLeft());
      } catch (StatusRuntimeException e) {
        serviceCallMetric.call(e.getStatus().getCode().value());
        throw e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    private void fillReadRowsBuffer(List<FlatRow> results) {
      if (results.size() == 0) {
        lastFillComplete = true;
        return;
      } else {
        if (results.size() < segmentLimit && !byteLimitReached) {
          lastFillComplete = true;
        }
        buffer.addAll(results.subList(0, results.size()));
      }
      lastFetchedRow = FlatRowConverter.convert(results.get(results.size() - 1)).getKey();
    }

    private boolean splitRowSet(ByteString splitPoint) {
      if (rowSet.getRowRangesList().isEmpty() && rowSet.getRowKeysList().isEmpty()) {
        rowSet = RowSet.newBuilder().addRowRanges(RowRange.getDefaultInstance()).build();
      }
      RowSet.Builder segment = RowSet.newBuilder();
      for (int i = 0; i < rowSet.getRowRangesCount(); i++) {
        RowRange rowRange = rowSet.getRowRanges(i);
        int startCmp = StartPoint.extract(rowRange).compareTo(new StartPoint(splitPoint, true));
        int endCmp = EndPoint.extract(rowRange).compareTo(new EndPoint(splitPoint, true));

        if (startCmp > 0) {
          // If the startKey is passed the split point than add the whole range
          segment.addRowRanges(rowRange);
        } else if (endCmp > 0) {
          // Row is split, remove all read rowKeys and split RowSet at last buffered Row
          RowRange subRange = rowRange.toBuilder().setStartKeyOpen(splitPoint).build();
          segment.addRowRanges(subRange);
        }
      }
      if (segment.getRowRangesCount() == 0) {
        return false;
      }
      rowSet = segment.build();
      return true;
    }

    @Override
    public void close() throws IOException {
      // Goal: by the end of this function, both results and session are null and closed,
      // independent of what errors they throw or prior state.

      if (session == null) {
        // Only possible when previously closed, so we know that results is also null.
        return;
      }

      // Session does not implement Closeable -- it's AutoCloseable. So we can't register it with
      // the Closer, but we can use the Closer to simplify the error handling.
      try (Closer closer = Closer.create()) {
        session.close();
      } finally {
        session = null;
      }
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
    private BigtableSession session;
    private BulkMutation bulkMutation;
    private BigtableTableName tableName;

    BigtableWriterImpl(BigtableSession session, BigtableTableName tableName) {
      this.session = session;
      bulkMutation = session.createBulkMutation(tableName);
      this.tableName = tableName;
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
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // We fail since flush() operation was interrupted.
            throw new IOException(e);
          }
          bulkMutation = null;
        }
      } finally {
        if (session != null) {
          session.close();
          session = null;
        }
      }
    }

    @Override
    public CompletionStage<MutateRowResponse> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException {
      MutateRowsRequest.Entry request =
          MutateRowsRequest.Entry.newBuilder()
              .setRowKey(record.getKey())
              .addAllMutations(record.getValue())
              .build();

      HashMap<String, String> baseLabels = new HashMap<>();
      baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
      baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "BigTable");
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "google.bigtable.v2.MutateRows");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.bigtableResource(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              tableName.getTableId()));
      baseLabels.put(
          MonitoringInfoConstants.Labels.BIGTABLE_PROJECT_ID, session.getOptions().getProjectId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.INSTANCE_ID, session.getOptions().getInstanceId());
      baseLabels.put(
          MonitoringInfoConstants.Labels.TABLE_ID,
          GcpResourceIdentifiers.bigtableTableID(
              session.getOptions().getProjectId(),
              session.getOptions().getInstanceId(),
              tableName.getTableId()));
      ServiceCallMetric serviceCallMetric =
          new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
      CompletableFuture<MutateRowResponse> result = new CompletableFuture<>();
      Futures.addCallback(
          new VendoredListenableFutureAdapter<>(bulkMutation.add(request)),
          new FutureCallback<MutateRowResponse>() {
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
    BigtableSession session = new BigtableSession(options);
    if (source.getMaxBufferElementCount() != null) {
      return new BigtableSegmentReaderImpl(session, source);
    } else {
      return new BigtableReaderImpl(session, source);
    }
  }

  public Reader createSegmentReader(BigtableSource source) throws IOException {
    BigtableSession session = new BigtableSession(options);
    return new BigtableReaderImpl(session, source);
  }

  @Override
  public List<SampleRowKeysResponse> getSampleRowKeys(BigtableSource source) throws IOException {
    try (BigtableSession session = new BigtableSession(options)) {
      SampleRowKeysRequest request =
          SampleRowKeysRequest.newBuilder()
              .setTableName(options.getInstanceName().toTableNameStr(source.getTableId().get()))
              .build();
      return session.getDataClient().sampleRowKeys(request);
    }
  }

  private static final Comparator<RowRange> RANGE_START_COMPARATOR =
      new Comparator<RowRange>() {
        @Override
        public int compare(@Nonnull RowRange o1, @Nonnull RowRange o2) {
          return StartPoint.extract(o1).compareTo(StartPoint.extract(o2));
        }
      };

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
}
