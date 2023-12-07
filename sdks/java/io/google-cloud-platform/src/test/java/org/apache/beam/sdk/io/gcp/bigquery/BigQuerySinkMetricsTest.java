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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.collection.IsMapContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQuerySinkMetrics}. */
@RunWith(JUnit4.class)
public class BigQuerySinkMetricsTest {

  public static class TestHistogram implements Histogram {
    public List<Double> values = Lists.newArrayList();
    private MetricName metricName = MetricName.named("namespace", "name");

    @Override
    public void update(double value) {
      values.add(value);
    }

    @Override
    public MetricName getName() {
      return metricName;
    }
  }

  public static class TestMetricsContainer extends MetricsContainerImpl {

    // public TestHistogram testHistogram = new TestHistogram();
    public ConcurrentHashMap<KV<MetricName, HistogramData.BucketType>, TestHistogram>
        perWorkerHistograms =
            new ConcurrentHashMap<KV<MetricName, HistogramData.BucketType>, TestHistogram>();
    public ConcurrentHashMap<MetricName, CounterCell> perWorkerCounters =
        new ConcurrentHashMap<MetricName, CounterCell>();

    public TestMetricsContainer() {
      super("TestStep");
    }

    @Override
    public Histogram getPerWorkerHistogram(
        MetricName metricName, HistogramData.BucketType bucketType) {
      perWorkerHistograms.computeIfAbsent(KV.of(metricName, bucketType), kv -> new TestHistogram());
      return perWorkerHistograms.get(KV.of(metricName, bucketType));
      //      return testHistogram;
    }

    @Override
    public Counter getPerWorkerCounter(MetricName metricName) {
      perWorkerCounters.computeIfAbsent(metricName, name -> new CounterCell(name));
      return perWorkerCounters.get(metricName);
    }

    @Override
    public void reset() {
      // testHistogram.values.clear();
      perWorkerHistograms.clear();
      perWorkerCounters.clear();
    }
  }

  @Test
  public void testAppendRowsRowStatusCounter() throws Exception {
    // Setup
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);

    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    Counter deletesDisabledCounter =
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
            BigQuerySinkMetrics.RowStatus.SUCCESSFUL, "rpcStatus", "tableId");
    deletesDisabledCounter.inc();
    MetricName deletesDisabledCounterName =
        MetricName.named(
            "BigQuerySink", "AppendRowsRowStatus-RowStatus:SUCCESSFUL;RpcStatus:rpcStatus;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(deletesDisabledCounterName));
    assertThat(
        testContainer.perWorkerCounters.get(deletesDisabledCounterName).getCumulative(),
        equalTo(1L));

    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    Counter deletesEnabledCounter =
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
            BigQuerySinkMetrics.RowStatus.SUCCESSFUL, "rpcStatus", "tableId");
    deletesEnabledCounter.inc();
    MetricName deletesEnabledCounterName =
        MetricName.named(
            "BigQuerySink",
            "AppendRowsRowStatus-RowStatus:SUCCESSFUL;RpcStatus:rpcStatus;TableId:tableId;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(deletesEnabledCounterName));
    assertThat(
        testContainer.perWorkerCounters.get(deletesEnabledCounterName).getCumulative(),
        equalTo(1L));
  }

  @Test
  public void testThrowableToGRPCCodeString() throws Exception {
    Throwable nullThrowable = null;
    assertThat(BigQuerySinkMetrics.throwableToGRPCCodeString(nullThrowable), equalTo("UNKNOWN"));

    Throwable nonGrpcError = new IndexOutOfBoundsException("Test Error");
    assertThat(BigQuerySinkMetrics.throwableToGRPCCodeString(nonGrpcError), equalTo("UNKNOWN"));

    int notFoundVal = Status.Code.NOT_FOUND.value();
    Throwable grpcError =
        new Exceptions.AppendSerializtionError(notFoundVal, "Test Error", "Stream name", null);
    assertThat(BigQuerySinkMetrics.throwableToGRPCCodeString(grpcError), equalTo("NOT_FOUND"));
  }

  @Test
  public void testThrottledTimeCounter() throws Exception {
    // Setup
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);

    // Test throttleCounter metric.
    Counter appendRowsThrottleCounter =
        BigQuerySinkMetrics.throttledTimeCounter(BigQuerySinkMetrics.RpcMethod.APPEND_ROWS);
    appendRowsThrottleCounter.inc(1);
    assertThat(
        appendRowsThrottleCounter.getName().getName(),
        equalTo("ThrottledTime-Method:APPEND_ROWS;"));
    MetricName counterName = MetricName.named("BigQuerySink", "ThrottledTime-Method:APPEND_ROWS;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterName));
    assertThat(testContainer.perWorkerCounters.get(counterName).getCumulative(), equalTo(1L));
  }

  @Test
  public void testReportSuccessfulRpcMetrics() throws Exception {
    // Setup
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    Context<AppendRowsResponse> c = new Context<AppendRowsResponse>();
    Instant t1 = Instant.now();
    c.setOperationStartTime(t1);
    c.setOperationEndTime(t1.plusMillis(3));

    // Test disabled SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameDisabledDeletes =
        MetricName.named("BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:OK;");
    MetricName histogramName = MetricName.named("BigQuerySink", "RpcLatency-Method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(1, 34);
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameDisabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameDisabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(3.0)));

    // Test enable SupportMetricsDeletion.
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:OK;TableId:tableId;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameEnabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameEnabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(3.0)));
  }

  @Test
  public void testReportFailedRPCMetrics_KnownGrpcError() throws Exception {
    // Setup
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    Context<AppendRowsResponse> c = new Context<AppendRowsResponse>();
    Instant t1 = Instant.now();
    c.setOperationStartTime(t1);
    c.setOperationEndTime(t1.plusMillis(5));
    int notFoundVal = Status.Code.NOT_FOUND.value();
    Throwable grpcError =
        new Exceptions.AppendSerializtionError(notFoundVal, "Test Error", "Stream name", null);
    c.setError(grpcError);

    // Test disabled SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameDisabledDeletes =
        MetricName.named("BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:NOT_FOUND;");
    MetricName histogramName = MetricName.named("BigQuerySink", "RpcLatency-Method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(1, 34);
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameDisabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameDisabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms,
        IsMapContaining.hasKey(KV.of(histogramName, bucketType)));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(5.0)));

    // Test enable SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:NOT_FOUND;TableId:tableId;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameEnabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameEnabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(5.0)));
  }

  @Test
  public void testReportFailedRPCMetrics_UnknownGrpcError() throws Exception {
    // Setup
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    Context<AppendRowsResponse> c = new Context<AppendRowsResponse>();
    Instant t1 = Instant.now();
    c.setOperationStartTime(t1);
    c.setOperationEndTime(t1.plusMillis(15));
    Throwable nonGrpcError = new IndexOutOfBoundsException("Test Error");
    c.setError(nonGrpcError);

    // Test disabled SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameDisabledDeletes =
        MetricName.named("BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:UNKNOWN;");
    MetricName histogramName = MetricName.named("BigQuerySink", "RpcLatency-Method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(1, 34);
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameDisabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameDisabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(15.0)));

    // Test enable SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink", "RpcRequests-Method:APPEND_ROWS;RpcStatus:UNKNOWN;TableId:tableId;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameEnabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameEnabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(15.0)));
  }
}
