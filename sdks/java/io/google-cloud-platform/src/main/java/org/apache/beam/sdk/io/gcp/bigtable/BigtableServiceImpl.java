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
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.protobuf.ByteString;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
}
