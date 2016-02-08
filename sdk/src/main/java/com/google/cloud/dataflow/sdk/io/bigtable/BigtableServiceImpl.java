/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.io.bigtable;

import com.google.bigtable.admin.table.v1.GetTableRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.async.AsyncExecutor;
import com.google.cloud.bigtable.grpc.async.HeapSizeManager;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
    String tableName = options.getClusterName().toTableNameStr(tableId);
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

  private class BigtableWriterImpl implements Writer {
    private BigtableSession session;
    private AsyncExecutor executor;
    private final MutateRowRequest.Builder partialBuilder;

    public BigtableWriterImpl(BigtableSession session, String tableName) {
      this.session = session;
      this.executor =
          new AsyncExecutor(
              session.getDataClient(),
              new HeapSizeManager(
                  AsyncExecutor.ASYNC_MUTATOR_MAX_MEMORY_DEFAULT,
                  AsyncExecutor.MAX_INFLIGHT_RPCS_DEFAULT));

      partialBuilder = MutateRowRequest.newBuilder().setTableName(tableName);
    }

    @Override
    public void close() throws IOException {
      try {
        if (executor != null) {
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
      try {
        return executor.mutateRowAsync(r);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Write interrupted", e);
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(BigtableServiceImpl.class)
        .add("options", options)
        .toString();
  }
}
