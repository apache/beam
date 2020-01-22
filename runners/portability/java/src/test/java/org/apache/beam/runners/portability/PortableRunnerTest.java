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
package org.apache.beam.runners.portability;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobApi.JobState;
import org.apache.beam.model.pipeline.v1.Endpoints.ApiServiceDescriptor;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.construction.InMemoryArtifactStagerService;
import org.apache.beam.runners.portability.testing.TestJobService;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.fn.test.InProcessManagedChannelFactory;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Timestamp;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.inprocess.InProcessServerBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PortableRunner}. */
@RunWith(JUnit4.class)
public class PortableRunnerTest implements Serializable {

  private static final String ENDPOINT_URL = "foo:3000";
  private static final ApiServiceDescriptor ENDPOINT_DESCRIPTOR =
      ApiServiceDescriptor.newBuilder().setUrl(ENDPOINT_URL).build();
  private static final String COUNTER_TYPE = "beam:metrics:sum_int_64";
  private static final String DIST_TYPE = "beam:metrics:distribution_int_64";
  private static final String GAUGE_TYPE = "beam:metrics:latest_int_64";
  private static final String NAMESPACE_LABEL = "NAMESPACE";
  private static final String METRIC_NAME_LABEL = "NAME";
  private static final String STEP_NAME_LABEL = "PTRANSFORM";
  private static final String NAMESPACE = "test";
  private static final String METRIC_NAME = "testMetric";
  private static final String STEP_NAME = "testStep";
  private static final Long COUNTER_VALUE = 42L;
  private static final Long GAUGE_VALUE = 64L;
  private static final Long DIST_SUM = 1000L;
  private static final Long DIST_MIN = 0L;
  private static final Long DIST_MAX = 1000L;
  private static final Long DIST_COUNT = 2L;

  private final PipelineOptions options = createPipelineOptions();

  @Rule public transient TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void stagesAndRunsJob() throws Exception {
    try (CloseableResource<Server> server =
        createJobServer(JobState.Enum.DONE, JobApi.MetricResults.getDefaultInstance())) {
      PortableRunner runner =
          PortableRunner.create(options, InProcessManagedChannelFactory.create());
      State state = runner.run(p).waitUntilFinish();
      assertThat(state, is(State.DONE));
    }
  }

  @Test
  public void extractsMetrics() throws Exception {
    JobApi.MetricResults metricResults = generateMetricResults();
    try (CloseableResource<Server> server = createJobServer(JobState.Enum.DONE, metricResults)) {
      PortableRunner runner =
          PortableRunner.create(options, InProcessManagedChannelFactory.create());
      PipelineResult result = runner.run(p);
      result.waitUntilFinish();
      MetricQueryResults metricQueryResults = result.metrics().allMetrics();
      assertThat(
          metricQueryResults.getCounters().iterator().next().getAttempted(), is(COUNTER_VALUE));
      assertThat(
          metricQueryResults.getDistributions().iterator().next().getAttempted().getCount(),
          is(DIST_COUNT));
      assertThat(
          metricQueryResults.getDistributions().iterator().next().getAttempted().getMax(),
          is(DIST_MAX));
      assertThat(
          metricQueryResults.getDistributions().iterator().next().getAttempted().getMin(),
          is(DIST_MIN));
      assertThat(
          metricQueryResults.getDistributions().iterator().next().getAttempted().getSum(),
          is(DIST_SUM));
      assertThat(
          metricQueryResults.getGauges().iterator().next().getAttempted().getValue(),
          is(GAUGE_VALUE));
    }
  }

  private JobApi.MetricResults generateMetricResults() {
    Map<String, String> labelMap = new HashMap<>();
    labelMap.put(NAMESPACE_LABEL, NAMESPACE);
    labelMap.put(METRIC_NAME_LABEL, METRIC_NAME);
    labelMap.put(STEP_NAME_LABEL, STEP_NAME);

    MetricsApi.CounterData counter =
        MetricsApi.CounterData.newBuilder().setInt64Value(COUNTER_VALUE).build();
    MetricsApi.Metric counterValue = MetricsApi.Metric.newBuilder().setCounterData(counter).build();
    MetricsApi.MonitoringInfo counterMonitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(COUNTER_TYPE)
            .putAllLabels(labelMap)
            .setMetric(counterValue)
            .build();

    MetricsApi.IntDistributionData intDistributionData =
        MetricsApi.IntDistributionData.newBuilder()
            .setMax(DIST_MAX)
            .setMin(DIST_MIN)
            .setSum(DIST_SUM)
            .setCount(DIST_COUNT)
            .build();
    MetricsApi.DistributionData distributionData =
        MetricsApi.DistributionData.newBuilder()
            .setIntDistributionData(intDistributionData)
            .build();
    MetricsApi.Metric distributionValue =
        MetricsApi.Metric.newBuilder().setDistributionData(distributionData).build();
    MetricsApi.MonitoringInfo distMonitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(DIST_TYPE)
            .putAllLabels(labelMap)
            .setMetric(distributionValue)
            .build();

    MetricsApi.IntExtremaData intExtremaData =
        MetricsApi.IntExtremaData.newBuilder().addIntValues(GAUGE_VALUE).build();
    MetricsApi.ExtremaData extremaData =
        MetricsApi.ExtremaData.newBuilder().setIntExtremaData(intExtremaData).build();
    MetricsApi.Metric gaugeValue =
        MetricsApi.Metric.newBuilder().setExtremaData(extremaData).build();
    MetricsApi.MonitoringInfo gaugeMonitoringInfo =
        MetricsApi.MonitoringInfo.newBuilder()
            .setType(GAUGE_TYPE)
            .setTimestamp(Timestamp.getDefaultInstance())
            .putAllLabels(labelMap)
            .setMetric(gaugeValue)
            .build();

    return JobApi.MetricResults.newBuilder()
        .addAttempted(counterMonitoringInfo)
        .addAttempted(distMonitoringInfo)
        .addAttempted(gaugeMonitoringInfo)
        .build();
  }

  private static CloseableResource<Server> createJobServer(
      JobState.Enum jobState, JobApi.MetricResults metricResults) throws IOException {
    CloseableResource<Server> server =
        CloseableResource.of(
            InProcessServerBuilder.forName(ENDPOINT_URL)
                .addService(
                    new TestJobService(
                        ENDPOINT_DESCRIPTOR, "prepId", "jobId", jobState, metricResults))
                .addService(new InMemoryArtifactStagerService())
                .build(),
            Server::shutdown);
    server.get().start();
    return server;
  }

  private static PipelineOptions createPipelineOptions() {
    PortablePipelineOptions options =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);
    options.setJobEndpoint(ENDPOINT_URL);
    options.setRunner(PortableRunner.class);
    return options;
  }
}
