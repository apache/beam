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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.NestedCounter;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.DelegatingCounter;
import org.apache.beam.sdk.metrics.DelegatingHistogram;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.LabeledMetricNameUtils;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Helper class to create perworker metrics for BigQuery Sink stages.
 *
 * <p>In general metrics be in the namespace 'BigQuerySink' and have their name formatted as:
 *
 * <p>'{baseName}*{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
 */
public class BigQuerySinkMetrics {
  private static boolean supportMetricsDeletion = false;
  private static boolean supportStreamingInsertsMetrics = false;

  public static final String METRICS_NAMESPACE = "BigQuerySink";

  // Status codes
  public static final String UNKNOWN = Status.Code.UNKNOWN.toString();
  public static final String OK = Status.Code.OK.toString();
  static final String INTERNAL = "INTERNAL";
  public static final String PAYLOAD_TOO_LARGE = "PayloadTooLarge";

  // Base Metric names
  private static final String RPC_REQUESTS = "RpcRequestsCount";
  private static final String RPC_LATENCY = "RpcLatency";
  private static final String APPEND_ROWS_ROW_STATUS = "RowsAppendedCount";
  public static final String THROTTLED_TIME = "ThrottledTime";

  // BigQuery Write Method names
  public enum RpcMethod {
    STREAMING_INSERTS,
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
  private static final String TABLE_ID_LABEL = "table_id";
  private static final String RPC_STATUS_LABEL = "rpc_status";
  private static final String RPC_METHOD = "rpc_method";
  private static final String ROW_STATUS = "row_status";

  /**
   * @param method StorageWriteAPI method associated with this metric.
   * @param rpcStatus RPC return status.
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   * @return Counter in namespace BigQuerySink and name
   *     'RpcRequests-Method:{method}RpcStatus:{status};TableId:{tableId}' TableId label is dropped
   *     if 'supportsMetricsDeletion' is not enabled.
   */
  @VisibleForTesting
  static Counter createRPCRequestCounter(RpcMethod method, String rpcStatus, String tableId) {
    LabeledMetricNameUtils.MetricNameBuilder nameBuilder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(RPC_REQUESTS);
    nameBuilder.addLabel(RPC_METHOD, method.toString());
    nameBuilder.addLabel(RPC_STATUS_LABEL, rpcStatus);
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      nameBuilder.addLabel(TABLE_ID_LABEL, tableId);
    }

    MetricName metricName = nameBuilder.build(METRICS_NAMESPACE);
    return new DelegatingCounter(metricName, false, true);
  }

  /**
   * Creates an Histogram metric to record RPC latency. Metric will have name.
   *
   * <p>'RpcLatency-Method:{method};'
   *
   * @param method StorageWriteAPI method associated with this metric.
   * @return Histogram with exponential buckets with a size 2 growth factor.
   */
  static Histogram createRPCLatencyHistogram(RpcMethod method) {
    LabeledMetricNameUtils.MetricNameBuilder nameBuilder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(RPC_LATENCY);
    nameBuilder.addLabel(RPC_METHOD, method.toString());
    MetricName metricName = nameBuilder.build(METRICS_NAMESPACE);

    // Create Exponential histogram buckets with the following parameters:
    // 0 scale, resulting in bucket widths with a size 2 growth factor.
    // 17 buckets, so the max latency of that can be stored is (2^17 millis ~= 130 seconds).
    HistogramData.BucketType buckets = HistogramData.ExponentialBuckets.of(0, 17);

    return new DelegatingHistogram(metricName, buckets, false, true);
  }

  /**
   * Records an RPC operation's duration in a PerWorkerHistogram.
   *
   * @param c Retry manager context, used to get the operation start and end time.
   * @param method StorageWriteAPI write method.
   */
  private static void updateRpcLatencyMetric(@Nonnull Context<?> c, RpcMethod method) {
    @Nullable Instant operationStartTime = c.getOperationStartTime();
    @Nullable Instant operationEndTime = c.getOperationEndTime();
    if (operationStartTime == null || operationEndTime == null) {
      return;
    }
    long timeElapsed = java.time.Duration.between(operationStartTime, operationEndTime).toMillis();
    if (timeElapsed > 0) {
      BigQuerySinkMetrics.createRPCLatencyHistogram(method).update(timeElapsed);
    }
  }

  /**
   * @param rowStatus Status of these BigQuery rows.
   * @param rpcStatus rpcStatus
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   * @return Metric that tracks the status of BigQuery rows after making an AppendRows RPC call.
   */
  public static Counter appendRowsRowStatusCounter(
      RowStatus rowStatus, String rpcStatus, String tableId) {
    LabeledMetricNameUtils.MetricNameBuilder nameBuilder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(APPEND_ROWS_ROW_STATUS);
    nameBuilder.addLabel(ROW_STATUS, rowStatus.toString());
    nameBuilder.addLabel(RPC_STATUS_LABEL, rpcStatus);
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      nameBuilder.addLabel(TABLE_ID_LABEL, tableId);
    }

    MetricName metricName = nameBuilder.build(METRICS_NAMESPACE);

    return new DelegatingCounter(metricName, false, true);
  }

  /**
   * @param method StorageWriteAPI write method.
   * @return Counter that tracks throttled time due to RPC retries.
   */
  public static Counter throttledTimeCounter(RpcMethod method) {

    LabeledMetricNameUtils.MetricNameBuilder nameBuilder =
        LabeledMetricNameUtils.MetricNameBuilder.baseNameBuilder(THROTTLED_TIME);
    nameBuilder.addLabel(RPC_METHOD, method.toString());
    MetricName metricName = nameBuilder.build(METRICS_NAMESPACE);
    // for specific method
    Counter fineCounter = new DelegatingCounter(metricName, false, true);
    // for overall throttling time, used by runner for scaling decision
    Counter coarseCounter = BigQueryServicesImpl.StorageClientImpl.THROTTLING_MSECS;
    return new NestedCounter(
        MetricName.named(
            METRICS_NAMESPACE, metricName.getName() + coarseCounter.getName().getName()),
        fineCounter,
        coarseCounter);
  }

  /**
   * Converts a Throwable to a gRPC Status code.
   *
   * @param t Throwable.
   * @return gRPC status code string or 'UNKNOWN' if 't' is null or does not map to a gRPC error.
   */
  public static String throwableToGRPCCodeString(@Nullable Throwable t) {
    if (t == null) {
      return BigQuerySinkMetrics.UNKNOWN;
    }
    return Status.fromThrowable(t).getCode().toString();
  }

  /**
   * Records RpcRequests counter and RpcLatency histogram for this RPC call. If
   * 'SupportMetricsDeletion' is enabled, RpcRequests counter will have tableId label set to {@code
   * UNKNOWN}. RpcRequets counter will have RpcStatus label set to {@code OK}.
   *
   * @param c Context of successful RPC call.
   * @param method StorageWriteAPI method associated with this metric.
   */
  public static void reportSuccessfulRpcMetrics(@Nullable Context<?> c, RpcMethod method) {
    reportSuccessfulRpcMetrics(c, method, UNKNOWN);
  }

  /**
   * Records RpcRequests counter and RpcLatency histogram for this RPC call. RpcRequets counter will
   * have RpcStatus label set to {@code OK}.
   *
   * @param c Context of successful RPC call.
   * @param method StorageWriteAPI method associated with this metric.
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   */
  public static void reportSuccessfulRpcMetrics(
      @Nullable Context<?> c, RpcMethod method, String tableId) {
    if (c == null) {
      return;
    }
    createRPCRequestCounter(method, OK, tableId).inc(1);
    updateRpcLatencyMetric(c, method);
  }

  /**
   * Records RpcRequests counter and RpcLatency histogram for this RPC call. If
   * 'SupportMetricsDeletion' is enabled, RpcRequests counter will have tableId label set to {@code
   * UNKNOWN}. RpcRequets counter will have a RpcStatus label set from the gRPC error.
   *
   * @param c Context of successful RPC call.
   * @param method StorageWriteAPI method associated with this metric.
   */
  public static void reportFailedRPCMetrics(@Nullable Context<?> c, RpcMethod method) {
    reportFailedRPCMetrics(c, method, UNKNOWN);
  }

  /**
   * Records RpcRequests counter and RpcLatency histogram for this RPC call. RpcRequets counter will
   * have a RpcStatus label set from the gRPC error.
   *
   * @param c Context of successful RPC call.
   * @param method StorageWriteAPI method associated with this metric.
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   */
  public static void reportFailedRPCMetrics(
      @Nullable Context<?> c, RpcMethod method, String tableId) {
    if (c == null) {
      return;
    }
    String statusCode = throwableToGRPCCodeString(c.getError());
    createRPCRequestCounter(method, statusCode, tableId).inc(1);
    updateRpcLatencyMetric(c, method);
  }

  /**
   * Returns a container to store metrics for BigQuery's {@code Streaming Inserts} RPC. If these
   * metrics are disabled, then we return a no-op container.
   */
  static StreamingInsertsMetrics streamingInsertsMetrics() {
    if (supportStreamingInsertsMetrics) {
      return StreamingInsertsMetrics.StreamingInsertsMetricsImpl.create();
    } else {
      return StreamingInsertsMetrics.NoOpStreamingInsertsMetrics.getInstance();
    }
  }

  public static void setSupportStreamingInsertsMetrics(boolean supportStreamingInsertsMetrics) {
    BigQuerySinkMetrics.supportStreamingInsertsMetrics = supportStreamingInsertsMetrics;
  }

  public static void setSupportMetricsDeletion(boolean supportMetricsDeletion) {
    BigQuerySinkMetrics.supportMetricsDeletion = supportMetricsDeletion;
  }
}
