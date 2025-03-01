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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.HistogramCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
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

  public static class TestHistogramCell extends HistogramCell {
    public List<Double> values = Lists.newArrayList();
    private MetricName metricName = MetricName.named("namespace", "name");

    public TestHistogramCell(KV<MetricName, HistogramData.BucketType> kv) {
      super(kv);
    }

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

    public ConcurrentHashMap<KV<MetricName, HistogramData.BucketType>, TestHistogramCell>
        perWorkerHistograms =
            new ConcurrentHashMap<KV<MetricName, HistogramData.BucketType>, TestHistogramCell>();
    public ConcurrentHashMap<MetricName, CounterCell> perWorkerCounters =
        new ConcurrentHashMap<MetricName, CounterCell>();

    public TestMetricsContainer() {
      super("TestStep");
    }

    @Override
    public TestHistogramCell getPerWorkerHistogram(
        MetricName metricName, HistogramData.BucketType bucketType) {
      perWorkerHistograms.computeIfAbsent(
          KV.of(metricName, bucketType), kv -> new TestHistogramCell(kv));
      return perWorkerHistograms.get(KV.of(metricName, bucketType));
    }

    @Override
    public Counter getPerWorkerCounter(MetricName metricName) {
      perWorkerCounters.computeIfAbsent(metricName, name -> new CounterCell(name));
      return perWorkerCounters.get(metricName);
    }

    @Override
    public void reset() {
      perWorkerHistograms.clear();
      perWorkerCounters.clear();
    }

    public void assertPerWorkerCounterValue(MetricName name, long value) throws Exception {
      assertThat(perWorkerCounters, IsMapContaining.hasKey(name));
      assertThat(perWorkerCounters.get(name).getCumulative(), equalTo(value));
    }

    public void assertPerWorkerHistogramValues(
        MetricName name, HistogramData.BucketType bucketType, double... values) {
      KV<MetricName, HistogramData.BucketType> kv = KV.of(name, bucketType);
      assertThat(perWorkerHistograms, IsMapContaining.hasKey(kv));

      Double[] objValues = Arrays.stream(values).boxed().toArray(Double[]::new);

      assertThat(perWorkerHistograms.get(kv).values, containsInAnyOrder(objValues));
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
            "BigQuerySink", "RowsAppendedCount*row_status:SUCCESSFUL;rpc_status:rpcStatus;");
    testContainer.assertPerWorkerCounterValue(deletesDisabledCounterName, 1L);

    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    Counter deletesEnabledCounter =
        BigQuerySinkMetrics.appendRowsRowStatusCounter(
            BigQuerySinkMetrics.RowStatus.SUCCESSFUL, "rpcStatus", "tableId");
    deletesEnabledCounter.inc();
    MetricName deletesEnabledCounterName =
        MetricName.named(
            "BigQuerySink",
            "RowsAppendedCount*row_status:SUCCESSFUL;rpc_status:rpcStatus;table_id:tableId;");
    testContainer.assertPerWorkerCounterValue(deletesEnabledCounterName, 1L);
  }

  @Test
  public void testThrowableToGRPCCodeString() throws Exception {
    Throwable nullThrowable = null;
    assertThat(BigQuerySinkMetrics.throwableToGRPCCodeString(nullThrowable), equalTo("UNKNOWN"));

    Throwable nonGrpcError = new IndexOutOfBoundsException("Test Error");
    assertThat(BigQuerySinkMetrics.throwableToGRPCCodeString(nonGrpcError), equalTo("UNKNOWN"));

    int notFoundVal = Status.Code.NOT_FOUND.value();
    Throwable grpcError =
        new Exceptions.AppendSerializationError(notFoundVal, "Test Error", "Stream name", null);
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
        equalTo("ThrottledTime*rpc_method:APPEND_ROWS;throttling-msecs"));

    // check that both sub-counters have been incremented
    MetricName counterName =
        MetricName.named("BigQuerySink", "ThrottledTime*rpc_method:APPEND_ROWS;");
    testContainer.assertPerWorkerCounterValue(counterName, 1L);

    counterName =
        MetricName.named(
            BigQueryServicesImpl.StorageClientImpl.class, Metrics.THROTTLE_TIME_COUNTER_NAME);
    assertEquals(1L, (long) testContainer.getCounter(counterName).getCumulative());
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
        MetricName.named("BigQuerySink", "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:OK;");
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(0, 17);
    testContainer.assertPerWorkerCounterValue(counterNameDisabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 3.0);

    // Test enable SupportMetricsDeletion.
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportSuccessfulRpcMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:OK;table_id:tableId;");
    testContainer.assertPerWorkerCounterValue(counterNameEnabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 3.0);
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
        new Exceptions.AppendSerializationError(notFoundVal, "Test Error", "Stream name", null);
    c.setError(grpcError);

    // Test disabled SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameDisabledDeletes =
        MetricName.named(
            "BigQuerySink", "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:NOT_FOUND;");
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(0, 17);
    testContainer.assertPerWorkerCounterValue(counterNameDisabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 5.0);

    // Test enable SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:NOT_FOUND;table_id:tableId;");
    testContainer.assertPerWorkerCounterValue(counterNameEnabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 5.0);
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
        MetricName.named(
            "BigQuerySink", "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:UNKNOWN;");
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:APPEND_ROWS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(0, 17);
    testContainer.assertPerWorkerCounterValue(counterNameDisabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 15.0);

    // Test enable SupportMetricsDeletion
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    testContainer.reset();
    BigQuerySinkMetrics.reportFailedRPCMetrics(
        c, BigQuerySinkMetrics.RpcMethod.APPEND_ROWS, "tableId");
    MetricName counterNameEnabledDeletes =
        MetricName.named(
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:UNKNOWN;table_id:tableId;");
    testContainer.assertPerWorkerCounterValue(counterNameEnabledDeletes, 1L);
    testContainer.assertPerWorkerHistogramValues(histogramName, bucketType, 15.0);
  }

  @Test
  public void testStreamingInsertsMetrics_disabled() {
    BigQuerySinkMetrics.setSupportStreamingInsertsMetrics(false);
    assertThat(
        BigQuerySinkMetrics.streamingInsertsMetrics(),
        sameInstance(StreamingInsertsMetrics.NoOpStreamingInsertsMetrics.getInstance()));
  }

  @Test
  public void testStreamingInsertsMetrics_enabled() {
    BigQuerySinkMetrics.setSupportStreamingInsertsMetrics(true);
    assertThat(
        BigQuerySinkMetrics.streamingInsertsMetrics(),
        instanceOf(StreamingInsertsMetrics.StreamingInsertsMetricsImpl.class));
  }
}
