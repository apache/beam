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

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.gcp.bigquery.BigQuerySinkMetrics.RowStatus;
import org.apache.beam.sdk.io.gcp.bigquery.RetryManager.Operation.Context;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
            "RowsAppendedCount*row_status:SUCCESSFUL;rpc_status:rpcStatus;table_id:tableId;");
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
        equalTo("ThrottledTime*rpc_method:APPEND_ROWS;"));

    MetricName counterName =
        MetricName.named("BigQuerySink", "ThrottledTime*rpc_method:APPEND_ROWS;");
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
        MetricName.named("BigQuerySink", "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:OK;");
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:APPEND_ROWS;");
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
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:OK;table_id:tableId;");
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
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:NOT_FOUND;table_id:tableId;");
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
        MetricName.named(
            "BigQuerySink", "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:UNKNOWN;");
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:APPEND_ROWS;");
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
            "BigQuerySink",
            "RpcRequestsCount*rpc_method:APPEND_ROWS;rpc_status:UNKNOWN;table_id:tableId;");
    assertThat(testContainer.perWorkerCounters, IsMapContaining.hasKey(counterNameEnabledDeletes));
    assertThat(
        testContainer.perWorkerCounters.get(counterNameEnabledDeletes).getCumulative(),
        equalTo(1L));
    assertThat(
        testContainer.perWorkerHistograms.get(KV.of(histogramName, bucketType)).values,
        containsInAnyOrder(Double.valueOf(15.0)));
  }

  @Test
  public void testParseMetricName_noLabels() {
    String baseMetricName = "baseMetricName";
    BigQuerySinkMetrics.ParsedMetricName expectedName =
        BigQuerySinkMetrics.ParsedMetricName.create(baseMetricName);

    Optional<BigQuerySinkMetrics.ParsedMetricName> parsedMetricName =
        BigQuerySinkMetrics.parseMetricName(baseMetricName);
    assertThat(parsedMetricName.isPresent(), equalTo(true));
    assertThat(parsedMetricName.get(), equalTo(expectedName));
  }

  @Test
  public void testParseMetricName_successfulLabels() {
    String metricName = "baseLabel*key1:val1;key2:val2;key3:val3;";
    ImmutableMap<String, String> metricLabels =
        ImmutableMap.of("key1", "val1", "key2", "val2", "key3", "val3");
    BigQuerySinkMetrics.ParsedMetricName expectedName =
        BigQuerySinkMetrics.ParsedMetricName.create("baseLabel", metricLabels);

    Optional<BigQuerySinkMetrics.ParsedMetricName> parsedMetricName =
        BigQuerySinkMetrics.parseMetricName(metricName);

    assertThat(parsedMetricName.isPresent(), equalTo(true));
    assertThat(parsedMetricName.get(), equalTo(expectedName));
  }

  @Test
  public void testParseMetricName_malformedMetricLabels() {
    String metricName = "baseLabel*malformed_kv_pair;key2:val2;";
    ImmutableMap<String, String> metricLabels = ImmutableMap.of("key2", "val2");
    BigQuerySinkMetrics.ParsedMetricName expectedName =
        BigQuerySinkMetrics.ParsedMetricName.create("baseLabel", metricLabels);

    Optional<BigQuerySinkMetrics.ParsedMetricName> parsedMetricName =
        BigQuerySinkMetrics.parseMetricName(metricName);

    assertThat(parsedMetricName.isPresent(), equalTo(true));
    assertThat(parsedMetricName.get(), equalTo(expectedName));
  }

  @Test
  public void testParseMetricName_emptyString() {
    assertThat(BigQuerySinkMetrics.parseMetricName("").isPresent(), equalTo(false));
  }

  @Test
  public void testUpdateStreamingInsertsMetrics_nullInput() throws Exception {
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);

    BigQuerySinkMetrics.StreamingInsertsResults results =
        BigQuerySinkMetrics.StreamingInsertsResults.create();
    results.updateInternalRetriedRows(10);
    Instant t1 = Instant.now();
    results.updateRpcResults(t1, t1.plus(Duration.ofMillis(10)), "OK");

    results.updateStreamingInsertsMetrics(null);

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
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    BigQuerySinkMetrics.StreamingInsertsResults results =
        BigQuerySinkMetrics.StreamingInsertsResults.create();
    results.updateInternalRetriedRows(10);
    results.updateSuccessfulAndFailedRows(50, 30);
    results.updateRetriedRowsWithStatus("QuotaLimits", 10);
    results.updateRetriedRowsWithStatus("QuotaLimits", 5);
    results.updateRetriedRowsWithStatus("ServiceUnavailable", 5);

    results.updateStreamingInsertsMetrics(ref);

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
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    BigQuerySinkMetrics.StreamingInsertsResults results =
        BigQuerySinkMetrics.StreamingInsertsResults.create();
    Instant t1 = Instant.now();
    results.updateRpcResults(t1, t1.plus(Duration.ofMillis(10)), "OK");
    results.updateRpcResults(t1, t1.plus(Duration.ofMillis(20)), "OK");
    results.updateRpcResults(t1, t1.plus(Duration.ofMillis(30)), "PermissionDenied");
    results.updateRpcResults(t1, t1.plus(Duration.ofMillis(40)), "Unavailable");

    results.updateStreamingInsertsMetrics(ref);

    // Validate RPC latency metric.
    MetricName histogramName =
        MetricName.named("BigQuerySink", "RpcLatency*rpc_method:STREAMING_INSERTS;");
    HistogramData.BucketType bucketType = HistogramData.ExponentialBuckets.of(1, 34);
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
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    TableReference ref = new TableReference().setTableId("t").setDatasetId("d");

    BigQuerySinkMetrics.StreamingInsertsResults results =
        BigQuerySinkMetrics.StreamingInsertsResults.create();
    results.updateInternalRetriedRows(10);

    results.updateStreamingInsertsMetrics(ref);

    String tableId = "datasets/d/tables/t";
    MetricName internalErrorRetriedMetricName =
        getAppendRowsCounterName(RowStatus.RETRIED, "INTERNAL", tableId);

    testContainer.assertPerWorkerCounterValue(internalErrorRetriedMetricName, 10L);

    // Subsequent updates to this object should update the underyling metrics.
    results.updateInternalRetriedRows(10);
    results.updateStreamingInsertsMetrics(ref);

    testContainer.assertPerWorkerCounterValue(internalErrorRetriedMetricName, 10L);
  }
}
