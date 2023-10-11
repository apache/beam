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

import io.grpc.Status;
import java.time.Instant;
import java.util.NavigableMap;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.DelegatingPerWorkerCounter;
import org.apache.beam.sdk.metrics.DelegatingPerWorkerHistogram;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;

/**
 * Helper class to create perworker metrics for BigQuery Sink stages.
 *
 * <p>In general metrics be in the namespace 'BigQuerySink' and have their name formatted as:
 *
 * <p>'{baseName}-{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
 */
public class BigQuerySinkMetrics {
  private static Boolean supportMetricsDeletion = false;

  private static final String METRICS_NAMESPACE = "BigQuerySink";
  private static final String UNKNOWN = Status.Code.UNKNOWN.toString();

  // Base Metric names.
  private static final String RPC_REQUESTS = "RpcRequests";
  private static final String RPC_LATENCY = "RpcLatency";
  private static final String APPEND_ROWS_ROW_STATUS = "AppendRowsRowStatus";
  private static final String THROTTLED_TIME = "ThrottledTime";

  // StorageWriteAPI Method names
  enum RpcMethod {
    APPEND_ROWS,
    FLUSH_ROWS,
    FINALIZE_STREAM
  }

  // Status of a BigQuery row from the AppendRows RPC call.
  enum RowStatus {
    SUCCESSFUL,
    RETRIED,
    FAILED
  }

  // Metric labels
  private static final String TABLE_ID_LABEL = "TableId";
  private static final String RPC_STATUS_LABEL = "Status";
  private static final String RPC_METHOD = "Method";
  private static final String ROW_STATUS = "RowStatus";

  // Delimiters
  private static final char LABEL_DELIMITER = ';';
  private static final char METRIC_KV_DELIMITER = ':';
  private static final char METRIC_NAME_DELIMITER = '-';

  /**
   * Returns a metric name that merges the baseName with metricLables formatted as:
   *
   * <p>'{baseName}-{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
   */
  private static String CreateLabeledMetricName(
      String baseName, NavigableMap<String, String> metricLabels) {
    StringBuilder nameBuilder = new StringBuilder(baseName + METRIC_NAME_DELIMITER);

    metricLabels.forEach(
        (labelKey, labelVal) ->
            nameBuilder.append(labelKey + METRIC_KV_DELIMITER + labelVal + LABEL_DELIMITER));
    return nameBuilder.toString();
  }

  /**
   * @param method StorageWriteAPI write method.
   * @param rpcStatus RPC return status.
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   * @return Counter in namespace BigQuerySink and name
   *     'RpcRequests-Status:{status};TableId:{tableId}' TableId label is dropped if
   *     'supportsMetricsDeletion' is not enabled.
   */
  private static Counter CreateRPCRequestCounter(
      RpcMethod method, String rpcStatus, String tableId) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_STATUS_LABEL, rpcStatus);
    metricLabels.put(RPC_METHOD, method.toString());
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      metricLabels.put(TABLE_ID_LABEL, tableId);
    }

    String fullMetricName = CreateLabeledMetricName(RPC_REQUESTS, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);
    return new DelegatingPerWorkerCounter(metricName);
  }

  /** Creates a counter for the AppendRows RPC call based on the rpcStatus and table. */
  public static Counter AppendRPCsCounter(String rpcStatus, String tableId) {
    return CreateRPCRequestCounter(RpcMethod.APPEND_ROWS, rpcStatus, tableId);
  }

  /**
   * Creates a counter for the FlushRows RPC call based on the rpcStatus. TableId is not known when
   * the stream is flushed so we use the placeholder 'UNKNOWN'.
   */
  public static Counter FlushRowsCounter(String rpcStatus) {
    return CreateRPCRequestCounter(RpcMethod.FLUSH_ROWS, rpcStatus, BigQuerySinkMetrics.UNKNOWN);
  }

  /**
   * Creates a counter for the FinalizeRows RPC call based on the rpcStatus. TableId is not known
   * when the stream is flushed so we use the placeholder 'UNKNOWN'.
   */
  public static Counter FinalizeStreamCounter(String rpcStatus) {
    return CreateRPCRequestCounter(
        RpcMethod.FINALIZE_STREAM, rpcStatus, BigQuerySinkMetrics.UNKNOWN);
  }

  /**
   * Creates an Histogram metric to record RPC latency. Metric will have name:
   *
   * <p>'RpcLatency-Method:{method};'
   *
   * @param method StorageWriteAPI method associated with this metric.
   * @return Histogram with exponential buckets with a sqrt(2) growth factor.
   */
  private static Histogram CreateRPCLatencyHistogram(RpcMethod method) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_METHOD, method.toString());
    String fullMetricName = CreateLabeledMetricName(RPC_LATENCY, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);

    HistogramData.BucketType buckets = HistogramData.ExponentialBuckets.of(1, 34);

    return new DelegatingPerWorkerHistogram(metricName, buckets);
  }

  /**
   * Records the time between operationStartTime and OperationEndTime in a PerWorkerHistogram.
   *
   * @param operationStartTime If null or in the future, this function is a no-op.
   * @param OperationEndTime End time of operation.
   * @param method StorageWriteAPI write method.
   */
  public static void UpdateRpcLatencyMetric(
      @Nullable Instant operationStartTime, Instant operationEndTime, RpcMethod method) {
    if (operationStartTime == null || operationEndTime == null) {
      return;
    }
    long timeElapsed = java.time.Duration.between(operationStartTime, operationEndTime).toMillis();
    if (timeElapsed > 0) {
      BigQuerySinkMetrics.CreateRPCLatencyHistogram(method).update(timeElapsed);
    }
  }

  /**
   * @param rowStatus Status of these BigQuery rows.
   * @param rpcStatus rpcStatus
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   * @return Metric that tracks the status of BigQuery rows after making an AppendRows RPC call.
   */
  public static Counter AppendRowsRowStatusCounter(
      RowStatus rowStatus, String rpcStatus, String tableId) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_STATUS_LABEL, rpcStatus);
    metricLabels.put(ROW_STATUS, rowStatus.toString());
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      metricLabels.put(TABLE_ID_LABEL, tableId);
    }

    String fullMetricName = CreateLabeledMetricName(APPEND_ROWS_ROW_STATUS, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);
    return new DelegatingPerWorkerCounter(metricName);
  }

  /** Metric that tracks throttled time due between RPC retries. */
  public static Counter ThrottledTimeCounter(RpcMethod method) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_METHOD, method.toString());
    String fullMetricName = CreateLabeledMetricName(THROTTLED_TIME, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);

    return new DelegatingPerWorkerCounter(metricName);
  }

  /**
   * Converts a Throwable to a gRPC Status code.
   *
   * @param t Throwable.
   * @return gRPC status code string or 'UNKNOWN' if 't' is null or does not map to a gRPC error.
   */
  public static String ThrowableToGRPCCodeString(@Nullable Throwable t) {
    if (t == null) {
      return BigQuerySinkMetrics.UNKNOWN;
    }
    return Status.fromThrowable(t).getCode().toString();
  }

  public static void setSupportMetricsDeletion(Boolean supportMetricsDeletion) {
    BigQuerySinkMetrics.supportMetricsDeletion = supportMetricsDeletion;
  }
}
