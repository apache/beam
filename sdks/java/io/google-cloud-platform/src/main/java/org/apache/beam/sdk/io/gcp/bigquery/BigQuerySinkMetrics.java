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

import com.google.auto.value.AutoValue;
import io.grpc.Status;
import java.time.Instant;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.DelegatingCounter;
import org.apache.beam.sdk.metrics.DelegatingHistogram;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Helper class to create perworker metrics for BigQuery Sink stages.
 *
 * <p>In general metrics be in the namespace 'BigQuerySink' and have their name formatted as:
 *
 * <p>'{baseName}*{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
 */
public class BigQuerySinkMetrics {
  private static boolean supportMetricsDeletion = false;

  public static final String METRICS_NAMESPACE = "BigQuerySink";

  // Status codes
  private static final String UNKNOWN = Status.Code.UNKNOWN.toString();
  public static final String OK = Status.Code.OK.toString();
  public static final String PAYLOAD_TOO_LARGE = "PayloadTooLarge";

  // Base Metric names
  private static final String RPC_REQUESTS = "RpcRequestsCount";
  private static final String RPC_LATENCY = "RpcLatency";
  private static final String APPEND_ROWS_ROW_STATUS = "RowsAppendedCount";
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
  private static final String TABLE_ID_LABEL = "table_id";
  private static final String RPC_STATUS_LABEL = "rpc_status";
  private static final String RPC_METHOD = "rpc_method";
  private static final String ROW_STATUS = "row_status";

  // Delimiters. Avoid using dilimiters that can also be used in a BigQuery table name.
  // ref: https://cloud.google.com/bigquery/docs/tables#table_naming
  private static final char LABEL_DELIMITER = ';';
  private static final char METRIC_KV_DELIMITER = ':';
  private static final char METRIC_NAME_DELIMITER = '*';

  @AutoValue
  public abstract static class ParsedMetricName {
    public abstract String getBaseName();

    public abstract ImmutableMap<String, String> getMetricLabels();

    public static ParsedMetricName create(
        String baseName, ImmutableMap<String, String> metricLabels) {
      return new AutoValue_BigQuerySinkMetrics_ParsedMetricName(baseName, metricLabels);
    }

    public static ParsedMetricName create(String baseName) {
      ImmutableMap<String, String> emptyMap = ImmutableMap.of();
      return new AutoValue_BigQuerySinkMetrics_ParsedMetricName(baseName, emptyMap);
    }
  }

  /**
   * Returns a metric name that merges the baseName with metricLables formatted as.
   *
   * <p>'{baseName}-{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
   */
  private static String createLabeledMetricName(
      String baseName, NavigableMap<String, String> metricLabels) {
    StringBuilder nameBuilder = new StringBuilder(baseName + METRIC_NAME_DELIMITER);

    metricLabels.forEach(
        (labelKey, labelVal) ->
            nameBuilder.append(labelKey + METRIC_KV_DELIMITER + labelVal + LABEL_DELIMITER));
    return nameBuilder.toString();
  }

  /**
   * Parse a 'metric name' String that was created with 'createLabeledMetricName'. The input string
   * should be formatted as.
   *
   * <p>'{baseName}*{metricLabelKey1}:{metricLabelVal1};...{metricLabelKeyN}:{metricLabelValN};'
   *
   * @param metricName
   * @return Returns a ParsedMetricName object if the input string is properly formatted. If the
   *     input string is empty or malformed, returns an empty value.
   */
  public static Optional<ParsedMetricName> parseMetricName(String metricName) {
    if (metricName.isEmpty()) {
      return Optional.empty();
    }

    List<String> metricNameSplit =
        Splitter.on(METRIC_NAME_DELIMITER).limit(2).splitToList(metricName);
    ImmutableMap.Builder<String, String> metricLabelsBuilder = ImmutableMap.builder();

    if (metricNameSplit.size() == 0) {
      return Optional.empty();
    }

    if (metricNameSplit.size() == 1) {
      return Optional.of(ParsedMetricName.create(metricNameSplit.get(0)));
    }
    // metrcNameSplit is assumed to be size two.

    List<String> labels = Splitter.on(LABEL_DELIMITER).splitToList(metricNameSplit.get(1));
    for (String label : labels) {
      List<String> kv = Splitter.on(METRIC_KV_DELIMITER).limit(2).splitToList(label);
      if (kv.size() != 2) {
        continue;
      }
      metricLabelsBuilder.put(kv.get(0), kv.get(1));
    }

    return Optional.of(
        ParsedMetricName.create(metricNameSplit.get(0), metricLabelsBuilder.build()));
  }

  /**
   * @param method StorageWriteAPI method associated with this metric.
   * @param rpcStatus RPC return status.
   * @param tableId Table pertaining to the write method. Only included in the metric key if
   *     'supportsMetricsDeletion' is enabled.
   * @return Counter in namespace BigQuerySink and name
   *     'RpcRequests-Method:{method}RpcStatus:{status};TableId:{tableId}' TableId label is dropped
   *     if 'supportsMetricsDeletion' is not enabled.
   */
  private static Counter createRPCRequestCounter(
      RpcMethod method, String rpcStatus, String tableId) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_STATUS_LABEL, rpcStatus);
    metricLabels.put(RPC_METHOD, method.toString());
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      metricLabels.put(TABLE_ID_LABEL, tableId);
    }

    String fullMetricName = createLabeledMetricName(RPC_REQUESTS, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);
    return new DelegatingCounter(metricName, false, true);
  }

  /**
   * Creates an Histogram metric to record RPC latency. Metric will have name.
   *
   * <p>'RpcLatency-Method:{method};'
   *
   * @param method StorageWriteAPI method associated with this metric.
   * @return Histogram with exponential buckets with a sqrt(2) growth factor.
   */
  private static Histogram createRPCLatencyHistogram(RpcMethod method) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_METHOD, method.toString());
    String fullMetricName = createLabeledMetricName(RPC_LATENCY, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);

    HistogramData.BucketType buckets = HistogramData.ExponentialBuckets.of(1, 34);

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
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_STATUS_LABEL, rpcStatus);
    metricLabels.put(ROW_STATUS, rowStatus.toString());
    if (BigQuerySinkMetrics.supportMetricsDeletion) {
      metricLabels.put(TABLE_ID_LABEL, tableId);
    }

    String fullMetricName = createLabeledMetricName(APPEND_ROWS_ROW_STATUS, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);
    return new DelegatingCounter(metricName, false, true);
  }

  /**
   * @param method StorageWriteAPI write method.
   * @return Counter that tracks throttled time due to RPC retries.
   */
  public static Counter throttledTimeCounter(RpcMethod method) {
    NavigableMap<String, String> metricLabels = new TreeMap<String, String>();
    metricLabels.put(RPC_METHOD, method.toString());
    String fullMetricName = createLabeledMetricName(THROTTLED_TIME, metricLabels);
    MetricName metricName = MetricName.named(METRICS_NAMESPACE, fullMetricName);

    return new DelegatingCounter(metricName, false, true);
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

  public static void setSupportMetricsDeletion(boolean supportMetricsDeletion) {
    BigQuerySinkMetrics.supportMetricsDeletion = supportMetricsDeletion;
  }
}
