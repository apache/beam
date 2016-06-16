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

import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.values.KV;

import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowRange;
import com.google.bigtable.v1.SampleRowKeysRequest;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.base.MoreObjects;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An implementation of {@link BigtableService} that actually communicates with the Cloud Bigtable
 * service.
 */
class BigtableServiceImpl implements BigtableService {
  private static final Logger logger = LoggerFactory.getLogger(BigtableService.class);

  public BigtableServiceImpl(BigtableOptions options) {
    this.options = options;
  }

  private final BigtableOptions options;

  @Override
  public BigtableWriterImpl openForWriting(String tableId) throws IOException {
    BigtableSession session = new BigtableSession(options);
    BigtableTableName tableName = options.getClusterName().toTableName(tableId);
    return new BigtableWriterImpl(session, tableName);
  }

  @Override
  public boolean tableExists(String tableId) throws IOException {
    if (!BigtableSession.isAlpnProviderEnabled()) {
      logger.info(
          "Skipping existence check for table {} (BigtableOptions {}) because ALPN is not"
              + " configured.",
          tableId,
          options);
      return true;
    }

    try (BigtableSession session = new BigtableSession(options)) {
      GetTableRequest getTable =
          GetTableRequest.newBuilder()
              .setName(options.getClusterName().toTableNameStr(tableId))
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
      logger.error(message, e);
      throw new IOException(message, e);
    }
  }

  private class BigtableReaderImpl implements Reader {
    private BigtableSession session;
    private final BigtableSource source;
    private ResultScanner<Row> results;
    private Row currentRow;

    public BigtableReaderImpl(BigtableSession session, BigtableSource source) {
      this.session = session;
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      RowRange range =
          RowRange.newBuilder()
              .setStartKey(source.getRange().getStartKey().getValue())
              .setEndKey(source.getRange().getEndKey().getValue())
              .build();
      ReadRowsRequest.Builder requestB =
          ReadRowsRequest.newBuilder()
              .setRowRange(range)
              .setTableName(options.getClusterName().toTableNameStr(source.getTableId()));
      if (source.getRowFilter() != null) {
        requestB.setFilter(source.getRowFilter());
      }
      results = session.getDataClient().readRows(requestB.build());
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      currentRow = results.next();
      return (currentRow != null);
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

  private static class BigtableWriterImpl implements Writer {
    private BigtableSession session;
    private AsyncExecutor executor;
    private BulkMutation bulkMutation;
    private final MutateRowRequest.Builder partialBuilder;

    public BigtableWriterImpl(BigtableSession session, BigtableTableName tableName) {
      this.session = session;
      executor = session.createAsyncExecutor();
      bulkMutation = session.createBulkMutation(tableName, executor);

      partialBuilder = MutateRowRequest.newBuilder().setTableName(tableName.toString());
    }

    @Override
    public void close() throws IOException {
      try {
        if (bulkMutation != null) {
          bulkMutation.flush();
          bulkMutation = null;
          executor.flush();
          executor = null;
        }
      } finally {
        if (session != null) {
          session.close();
          session = null;
        }
      }
    }

    @Override
    public ListenableFuture<Empty> writeRecord(KV<ByteString, Iterable<Mutation>> record)
        throws IOException {
      MutateRowRequest r =
          partialBuilder
              .clone()
              .setRowKey(record.getKey())
              .addAllMutations(record.getValue())
              .build();
      return bulkMutation.add(r);
    }
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(BigtableServiceImpl.class)
        .add("options", options)
        .toString();
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
              .setTableName(options.getClusterName().toTableNameStr(source.getTableId()))
              .build();
      return session.getDataClient().sampleRowKeys(request);
    }
  }
}
