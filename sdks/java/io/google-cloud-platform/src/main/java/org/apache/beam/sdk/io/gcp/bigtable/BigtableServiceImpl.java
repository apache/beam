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
import com.google.cloud.bigtable.data.v2.internal.ByteStringComparator;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.async.BulkRead;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ComparisonChain;
import com.google.protobuf.ByteString;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Closer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.FutureCallback;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Futures;
import javax.annotation.Nonnull;
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
    private Queue<Row> buffer;
    private RowSet rowSet;
    private ServiceCallMetric serviceCallMetric;
    //private BulkRead bulkRead;
    private String tableNameStr;


    private final int MINI_BATCH_ROW_LIMIT = 100;

    @VisibleForTesting
    BigtableReaderImpl(BigtableSession session, BigtableSource source) {
      this.session = session;
      tableNameStr = session.getOptions().getInstanceName().toTableNameStr(source.getTableId().get());
      //bulkRead = session.createBulkRead(new BigtableTableName(tableNameStr));
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      buffer = new ConcurrentLinkedQueue<Row>();
      RowRange[] rowRanges = new RowRange[source.getRanges().size()];
      for (int i = 0; i < source.getRanges().size(); i++) {
        rowRanges[i] = RowRange.newBuilder()
            .setStartKeyClosed(
                ByteString.copyFrom(source.getRanges().get(i).getStartKey().getValue()))
            .setEndKeyOpen(
                ByteString.copyFrom(source.getRanges().get(i).getEndKey().getValue()))
            .build();
      }
      // Sort the rowRanges by startKey
      Arrays.sort(rowRanges, RANGE_START_COMPARATOR);
      rowSet = RowSet.newBuilder().
          addAllRowRanges(Arrays.stream(rowRanges).collect(Collectors.toList())).build();

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

      fillReadRowsBuffer();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (buffer.isEmpty()) {
        return buildReadRowsRequest();
      }
      currentRow = buffer.remove();
      return currentRow != null;
    }

    public boolean buildReadRowsRequest() throws IOException {
      if (rowSet.getRowRangesCount() == 0) {
        return false;
      }
      RowSet.Builder segment = RowSet.newBuilder();
      ByteString splitPoint = currentRow.getKey();

      for (int i = 0; i < rowSet.getRowRangesCount(); i++) {
        RowRange rowRange = rowSet.getRowRanges(i);

        int startCmp = StartPoint.extract(rowRange).compareTo(new StartPoint(splitPoint, true));
        int endCmp = EndPoint.extract(rowRange).compareTo(new EndPoint(splitPoint, true));

        if (startCmp > 0) {
          // If the startKey is passed the split point than add the whole range
          segment.addRowRanges(rowRange);
        } else if (endCmp > 0) {
          // Row is split, remove all read rowKeys and split RowSet at last Read Row
          RowRange subRange = rowRange.toBuilder().setStartKeyOpen(splitPoint).build();
          segment.addRowRanges(subRange);
        }
      }

      if (segment.getRowRangesCount() == 0) {
        return false;
      }
      rowSet = segment.build();
      if(fillReadRowsBuffer()) {
        currentRow = buffer.remove();
        return currentRow != null;
      } else {
        return false;
      }
    }

    public boolean fillReadRowsBuffer() throws IOException {
      ReadRowsRequest.Builder request =
          ReadRowsRequest.newBuilder().setRows(rowSet)
              .setRowsLimit(MINI_BATCH_ROW_LIMIT).setTableName(tableNameStr);
      if (source.getRowFilter() != null) {
        request.setFilter(source.getRowFilter());
      }
      try {
        results = session.getDataClient().readRows(request.build());
        // results = session.getDataClient(). Add Callable here (Async?)
        if (results.available() == 0) { // Edge Cases?
          return false;
        }
        buffer.addAll(Arrays.asList(
            results.next(MINI_BATCH_ROW_LIMIT)));
        serviceCallMetric.call("ok");
        return true;
      } catch (StatusRuntimeException e) {
        serviceCallMetric.call(e.getStatus().getCode().value());
        throw e;
      }
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

    StartPoint(@Nonnull ByteString value, boolean isClosed) {
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

    EndPoint(@Nonnull ByteString value, boolean isClosed) {
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
