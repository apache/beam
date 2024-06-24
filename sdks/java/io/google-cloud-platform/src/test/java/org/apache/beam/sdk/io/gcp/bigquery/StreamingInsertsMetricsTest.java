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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.api.services.bigquery.model.TableReference;
import java.time.Duration;
import java.time.Instant;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics.RowStatus;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.HistogramData;
import org.junit.Test;

public class StreamingInsertsMetricsTest {

  @Test
  public void testNoOpStreamingInsertsMetrics() throws Exception {
    BigQuerySinkMetricsTest.TestMetricsContainer testContainer =
        new BigQuerySinkMetricsTest.TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);

    StreamingInsertsMetrics results =
        StreamingInsertsMetrics.NoOpStreamingInsertsMetrics.getInstance();
    results.updateRetriedRowsWithStatus("INTERNAL", 10);
    Instant t1 = Instant.now();
    results.updateSuccessfulRpcMetrics(t1, t1.plus(Duration.ofMillis(10)));
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    results.updateStreamingInsertsMetrics(ref, 5, 0);

    assertThat(testContainer.perWorkerCounters.size(), equalTo(0));
    assertThat(testContainer.perWorkerHistograms.size(), equalTo(0));
  }

  @Test
  public void testUpdateStreamingInsertsMetrics_nullInput() throws Exception {
    BigQuerySinkMetricsTest.TestMetricsContainer testContainer =
        new BigQuerySinkMetricsTest.TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);

    StreamingInsertsMetrics results = StreamingInsertsMetrics.StreamingInsertsMetricsImpl.create();
    results.updateRetriedRowsWithStatus("INTERNAL", 0);
    Instant t1 = Instant.now();
    results.updateSuccessfulRpcMetrics(t1, t1.plus(Duration.ofMillis(10)));

    results.updateStreamingInsertsMetrics(null, 0, 0);

    assertThat(testContainer.perWorkerCounters.size(), equalTo(0));
    assertThat(testContainer.perWorkerHistograms.size(), equalTo(0));
  }

  MetricName getAppendRowsCounterName(
      BigQuerySinkMetrics.RowStatus rowStatus, String rpcStatus, String tableId) {
    return BigQuerySinkMetrics.appendRowsRowStatusCounter(rowStatus, rpcStatus, tableId).getName();
  }

  MetricName getRpcRequestsCounterName(
      BigQuerySinkMetrics.RpcMethod method, String rpcStatus, String tableId) {
    return BigQuerySinkMetrics.createRPCRequestCounter(method, rpcStatus, tableId).getName();
  }

  @Test
  public void testUpdateStreamingInsertsMetrics_rowsAppendedCounter() throws Exception {
    BigQuerySinkMetricsTest.TestMetricsContainer testContainer =
        new BigQuerySinkMetricsTest.TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    StreamingInsertsMetrics results = StreamingInsertsMetrics.StreamingInsertsMetricsImpl.create();
    results.updateRetriedRowsWithStatus("INTERNAL", 10);
    results.updateRetriedRowsWithStatus("QuotaLimits", 10);
    results.updateRetriedRowsWithStatus("QuotaLimits", 5);
    results.updateRetriedRowsWithStatus("ServiceUnavailable", 5);

    results.updateStreamingInsertsMetrics(ref, 50, 30);

    String tableId = "datasets/d/tables/t";
    MetricName internalErrorRetriedMetricName =
        getAppendRowsCounterName(RowStatus.RETRIED, "INTERNAL", tableId);
    MetricName succssfulRowsMetricName =
        getAppendRowsCounterName(RowStatus.SUCCESSFUL, "OK", tableId);
    MetricName failedRowsMetricName =
        getAppendRowsCounterName(RowStatus.FAILED, "INTERNAL", tableId);
    MetricName retriedRowsQuotaMetricName =
        getAppendRowsCounterName(RowStatus.RETRIED, "QuotaLimits", tableId);
    MetricName retriedRowsUnavailableMetricName =
        getAppendRowsCounterName(RowStatus.RETRIED, "ServiceUnavailable", tableId);

    testContainer.assertPerWorkerCounterValue(internalErrorRetriedMetricName, 10L);
    testContainer.assertPerWorkerCounterValue(succssfulRowsMetricName, 20L);
    testContainer.assertPerWorkerCounterValue(failedRowsMetricName, 30L);
    testContainer.assertPerWorkerCounterValue(retriedRowsQuotaMetricName, 15L);
    testContainer.assertPerWorkerCounterValue(retriedRowsUnavailableMetricName, 5L);
  }

  @Test
  public void testUpdateStreamingInsertsMetrics_rpcLatencyHistogram() throws Exception {
    BigQuerySinkMetricsTest.TestMetricsContainer testContainer =
        new BigQuerySinkMetricsTest.TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    StreamingInsertsMetrics results = StreamingInsertsMetrics.StreamingInsertsMetricsImpl.create();
    Instant t1 = Instant.now();
    results.updateSuccessfulRpcMetrics(t1, t1.plus(Duration.ofMillis(10)));
    results.updateSuccessfulRpcMetrics(t1, t1.plus(Duration.ofMillis(20)));
    results.updateFailedRpcMetrics(t1, t1.plus(Duration.ofMillis(30)), "PermissionDenied");
    results.updateFailedRpcMetrics(t1, t1.plus(Duration.ofMillis(40)), "Unavailable");

    results.updateStreamingInsertsMetrics(ref, 5, 0);

    // Validate RPC latency metric.
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:STREAMING_INSERTS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(0, 17);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 10.0, 20.0, 30.0, 40.0);

    // Validate RPC Status metric.
    BigQuerySinkMetrics.RpcMethod m = BigQuerySinkMetrics.RpcMethod.STREAMING_INSERTS;
    String tableId = "datasets/d/tables/t";
    MetricName okMetricName = getRpcRequestsCounterName(m, "OK", tableId);
    MetricName permissionDeniedMetricName =
        getRpcRequestsCounterName(m, "PermissionDenied", tableId);
    MetricName unavailableMetricName = getRpcRequestsCounterName(m, "Unavailable", tableId);

    testContainer.assertPerWorkerCounterValue(okMetricName, 2L);
    testContainer.assertPerWorkerCounterValue(permissionDeniedMetricName, 1L);
    testContainer.assertPerWorkerCounterValue(unavailableMetricName, 1L);
  }

  @Test
  public void testUpdateStreamingInsertsMetrics_multipleUpdateStreamingInsertsMetrics()
      throws Exception {
    BigQuerySinkMetricsTest.TestMetricsContainer testContainer =
        new BigQuerySinkMetricsTest.TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    StreamingInsertsMetrics results = StreamingInsertsMetrics.StreamingInsertsMetricsImpl.create();
    results.updateRetriedRowsWithStatus("INTERNAL", 10);

    results.updateStreamingInsertsMetrics(ref, 5, 0);

    String tableId = "datasets/d/tables/t";
    MetricName internalErrorRetriedMetricName =
        getAppendRowsCounterName(RowStatus.RETRIED, "INTERNAL", tableId);

    testContainer.assertPerWorkerCounterValue(internalErrorRetriedMetricName, 10L);

    // Subsequent updates to this object should update the underyling metrics.
    results.updateRetriedRowsWithStatus("INTERNAL", 10);
    results.updateStreamingInsertsMetrics(ref, 5, 0);

    testContainer.assertPerWorkerCounterValue(internalErrorRetriedMetricName, 10L);
  }
}
