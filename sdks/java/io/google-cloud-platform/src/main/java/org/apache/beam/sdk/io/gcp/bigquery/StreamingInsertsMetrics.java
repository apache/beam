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

import com.google.api.services.bigquery.model.TableReference;
import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.values.KV;

/** Stores and exports metrics for a batch of Streaming Inserts RPCs. */
public interface StreamingInsertsMetrics {

  void updateRetriedRowsWithStatus(String status, int retriedRows);

  void updateFailedRpcMetrics(Instant start, Instant end, String status);

  void updateSuccessfulRpcMetrics(Instant start, Instant end);

  void updateStreamingInsertsMetrics(
      @Nullable TableReference tableRef, int totalRows, int failedRows);

  /** No-op implementation of {@code StreamingInsertsResults}. */
  class NoOpStreamingInsertsMetrics implements StreamingInsertsMetrics {
    private NoOpStreamingInsertsMetrics() {}

    @Override
    public void updateRetriedRowsWithStatus(String status, int retriedRows) {}

    @Override
    public void updateFailedRpcMetrics(Instant start, Instant end, String status) {}

    @Override
    public void updateSuccessfulRpcMetrics(Instant start, Instant end) {}

    @Override
    public void updateStreamingInsertsMetrics(
        @Nullable TableReference tableRef, int totalRows, int failedRows) {}

    private static NoOpStreamingInsertsMetrics singleton = new NoOpStreamingInsertsMetrics();

    static NoOpStreamingInsertsMetrics getInstance() {
      return singleton;
    }
  }

  /**
   * Metrics of a batch of InsertAll RPCs. Member variables are thread safe; however, this class
   * does not have atomicity across member variables.
   *
   * <p>Expected usage: A number of threads record metrics in an instance of this class with the
   * member methods. Afterwards, a single thread should call {@code updateStreamingInsertsMetrics}
   * which will export all counters metrics and RPC latency distribution metrics to the underlying
   * {@code perWorkerMetrics} container. Afterwards, metrics should not be written/read from this
   * object.
   */
  @AutoValue
  abstract class StreamingInsertsMetricsImpl implements StreamingInsertsMetrics {
    static final Histogram LATENCY_HISTOGRAM =
        BigQuerySinkMetrics.createRPCLatencyHistogram(
            BigQuerySinkMetrics.RpcMethod.STREAMING_INSERTS);

    abstract ConcurrentLinkedQueue<java.time.Duration> rpcLatencies();

    abstract ConcurrentLinkedQueue<String> rpcErrorStatus();
    // Represents <Rpc Status, Number of Rows> for rows that are retried because of a failed
    // InsertAll RPC.
    abstract ConcurrentLinkedQueue<KV<String, Integer>> retriedRowsByStatus();

    abstract AtomicInteger successfulRpcsCount();

    abstract AtomicBoolean isWritable();

    public static StreamingInsertsMetricsImpl create() {
      return new AutoValue_StreamingInsertsMetrics_StreamingInsertsMetricsImpl(
          new ConcurrentLinkedQueue<>(),
          new ConcurrentLinkedQueue<>(),
          new ConcurrentLinkedQueue<>(),
          new AtomicInteger(),
          new AtomicBoolean(true));
    }

    /** Update metrics for rows that were retried due to an RPC error. */
    @Override
    public void updateRetriedRowsWithStatus(String status, int retriedRows) {
      if (isWritable().get()) {
        retriedRowsByStatus().add(KV.of(status, retriedRows));
      }
    }

    /** Record the rpc status and latency of a failed StreamingInserts RPC call. */
    @Override
    public void updateFailedRpcMetrics(Instant start, Instant end, String status) {
      if (isWritable().get()) {
        rpcErrorStatus().add(status);
        rpcLatencies().add(Duration.between(start, end));
      }
    }

    /** Record the rpc status and latency of a successful StreamingInserts RPC call. */
    @Override
    public void updateSuccessfulRpcMetrics(Instant start, Instant end) {
      if (isWritable().get()) {
        successfulRpcsCount().getAndIncrement();
        rpcLatencies().add(Duration.between(start, end));
      }
    }

    /** Record rpc latency histogram metrics. */
    private void recordRpcLatencyMetrics() {
      for (Duration d : rpcLatencies()) {
        LATENCY_HISTOGRAM.update(d.toMillis());
      }
    }

    /**
     * Export all metrics recorded in this instance to the underlying {@code perWorkerMetrics}
     * containers. This function will only report metrics once per instance. Subsequent calls to
     * this function will no-op.
     *
     * @param tableRef BigQuery table that was written to, return early if null.
     */
    @Override
    public void updateStreamingInsertsMetrics(
        @Nullable TableReference tableRef, int totalRows, int failedRows) {
      if (!isWritable().compareAndSet(true, false)) {
        // Metrics have already been exported.
        return;
      }

      if (tableRef == null) {
        return;
      }

      String shortTableId =
          String.join("/", "datasets", tableRef.getDatasetId(), "tables", tableRef.getTableId());

      Map<String, Integer> rpcRequetsRpcStatusMap = new HashMap<>();
      Map<String, Integer> retriedRowsRpcStatusMap = new HashMap<>();

      for (String status : rpcErrorStatus()) {
        Integer currentVal = rpcRequetsRpcStatusMap.getOrDefault(status, 0);
        rpcRequetsRpcStatusMap.put(status, currentVal + 1);
      }

      for (KV<String, Integer> retryCountByStatus : retriedRowsByStatus()) {
        Integer currentVal = retriedRowsRpcStatusMap.getOrDefault(retryCountByStatus.getKey(), 0);
        retriedRowsRpcStatusMap.put(
            retryCountByStatus.getKey(), currentVal + retryCountByStatus.getValue());
      }

      for (Entry<String, Integer> entry : rpcRequetsRpcStatusMap.entrySet()) {
        BigQuerySinkMetrics.createRPCRequestCounter(
                BigQuerySinkMetrics.RpcMethod.STREAMING_INSERTS, entry.getKey(), shortTableId)
            .inc(entry.getValue());
      }

      for (Entry<String, Integer> entry : retriedRowsRpcStatusMap.entrySet()) {
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
                BigQuerySinkMetrics.RowStatus.RETRIED, entry.getKey(), shortTableId)
            .inc(entry.getValue());
      }

      if (successfulRpcsCount().get() != 0) {
        BigQuerySinkMetrics.createRPCRequestCounter(
                BigQuerySinkMetrics.RpcMethod.STREAMING_INSERTS,
                BigQuerySinkMetrics.OK,
                shortTableId)
            .inc(successfulRpcsCount().longValue());
      }

      if (failedRows >= 0) {
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
                BigQuerySinkMetrics.RowStatus.FAILED, BigQuerySinkMetrics.INTERNAL, shortTableId)
            .inc(failedRows);
      }

      if (totalRows - failedRows >= 0) {
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
                BigQuerySinkMetrics.RowStatus.SUCCESSFUL, BigQuerySinkMetrics.OK, shortTableId)
            .inc(totalRows - failedRows);
      }

      recordRpcLatencyMetrics();
    }
  }
}
