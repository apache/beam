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

import com.google.cloud.bigquery.storage.v1.Exceptions;
import io.grpc.Status;
import java.time.Instant;
import java.util.List;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
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

    public TestHistogram testHistogram = new TestHistogram();

    public TestMetricsContainer() {
      super("TestStep");
    }

    @Override
    public Histogram getPerWorkerHistogram(
        MetricName metricName, HistogramData.BucketType bucketType) {
      return testHistogram;
    }
  }

  @Test
  public void testAppendRPCsCounter() throws Exception {
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    Counter deletesEnabledCounter = BigQuerySinkMetrics.AppendRPCsCounter("rpcStatus", "tableId");
    assertThat(
        deletesEnabledCounter.getName().getName(),
        equalTo("RpcRequests-Method:APPEND_ROWS;Status:rpcStatus;"));

    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    Counter deletesDisabledCounter = BigQuerySinkMetrics.AppendRPCsCounter("rpcStatus", "tableId");
    assertThat(
        deletesDisabledCounter.getName().getName(),
        equalTo("RpcRequests-Method:APPEND_ROWS;Status:rpcStatus;TableId:tableId;"));
  }

  @Test
  public void testFlushRowsCounter() throws Exception {
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    Counter deletesEnabledCounter = BigQuerySinkMetrics.FlushRowsCounter("rpcStatus");
    assertThat(
        deletesEnabledCounter.getName().getName(),
        equalTo("RpcRequests-Method:FLUSH_ROWS;Status:rpcStatus;"));

    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    Counter deletesDisabledCounter = BigQuerySinkMetrics.FlushRowsCounter("rpcStatus");
    assertThat(
        deletesDisabledCounter.getName().getName(),
        equalTo("RpcRequests-Method:FLUSH_ROWS;Status:rpcStatus;TableId:UNKNOWN;"));
  }

  @Test
  public void testAppendRowsRowStatusCounter() throws Exception {
    BigQuerySinkMetrics.setSupportMetricsDeletion(false);
    Counter deletesEnabledCounter =
        BigQuerySinkMetrics.AppendRowsRowStatusCounter(
            BigQuerySinkMetrics.RowStatus.SUCCESSFUL, "rpcStatus", "tableId");
    assertThat(
        deletesEnabledCounter.getName().getName(),
        equalTo("AppendRowsRowStatus-RowStatus:SUCCESSFUL;Status:rpcStatus;"));

    BigQuerySinkMetrics.setSupportMetricsDeletion(true);
    Counter deletesDisabledCounter =
        BigQuerySinkMetrics.AppendRowsRowStatusCounter(
            BigQuerySinkMetrics.RowStatus.SUCCESSFUL, "rpcStatus", "tableId");
    assertThat(
        deletesDisabledCounter.getName().getName(),
        equalTo("AppendRowsRowStatus-RowStatus:SUCCESSFUL;Status:rpcStatus;TableId:tableId;"));
  }

  @Test
  public void testThrowableToGRPCCodeString() throws Exception {
    Throwable nullThrowable = null;
    assertThat(BigQuerySinkMetrics.ThrowableToGRPCCodeString(nullThrowable), equalTo("UNKNOWN"));

    Throwable nonGrpcError = new IndexOutOfBoundsException("Test Error");
    assertThat(BigQuerySinkMetrics.ThrowableToGRPCCodeString(nonGrpcError), equalTo("UNKNOWN"));

    int not_found_val = Status.Code.NOT_FOUND.value();
    Throwable grpcError =
        new Exceptions.AppendSerializtionError(not_found_val, "Test Error", "Stream name", null);
    assertThat(BigQuerySinkMetrics.ThrowableToGRPCCodeString(grpcError), equalTo("NOT_FOUND"));
  }

  @Test
  public void testThrottledTimeCounter() throws Exception {
    Counter appendRowsThrottleCounter =
        BigQuerySinkMetrics.ThrottledTimeCounter(BigQuerySinkMetrics.RpcMethod.APPEND_ROWS);
    assertThat(
        appendRowsThrottleCounter.getName().getName(),
        equalTo("ThrottledTime-Method:APPEND_ROWS;"));
  }

  @Test
  public void testUpdateRpcLatencyMetric() throws Exception {
    TestMetricsContainer testContainer = new TestMetricsContainer();
    MetricsEnvironment.setCurrentContainer(testContainer);
    BigQuerySinkMetrics.RpcMethod append = BigQuerySinkMetrics.RpcMethod.APPEND_ROWS;
    Instant t1 = Instant.now();

    // Expect no updates to the histogram when we pass a null instant.
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(null, t1, append);
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(t1, null, append);
    assertThat(testContainer.testHistogram.values.size(), equalTo(0));

    // Expect no updates when end time is before start time.
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(t1, t1.minusMillis(5), append);
    assertThat(testContainer.testHistogram.values.size(), equalTo(0));

    // Expect valid updates to be recorded in the underlying histogram.
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(t1.minusMillis(5), t1, append);
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(t1.minusMillis(10), t1, append);
    BigQuerySinkMetrics.UpdateRpcLatencyMetric(t1.minusMillis(15), t1, append);
    assertThat(
        testContainer.testHistogram.values,
        containsInAnyOrder(Double.valueOf(5.0), Double.valueOf(10.0), Double.valueOf(15.0)));
  }
}
