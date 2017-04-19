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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.sdk.metrics.MetricMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricMatchers.committedMetricsResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.runners.dataflow.testing.TestDataflowPipelineOptions;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.NoopPathValidator;
import org.apache.beam.sdk.util.TestCredential;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link DataflowMetrics}.
 */
@RunWith(JUnit4.class)
public class DataflowMetricsTest {
  private static final String PROJECT_ID = "some-project";
  private static final String JOB_ID = "1234";
  private static final String REGION_ID = "some-region";
  private static final String REPLACEMENT_JOB_ID = "4321";

  @Mock
  private Dataflow mockWorkflowClient;
  @Mock
  private Dataflow.Projects mockProjects;
  @Mock
  private Dataflow.Projects.Locations mockLocations;
  @Mock
  private Dataflow.Projects.Locations.Jobs mockJobs;

  private TestDataflowPipelineOptions options;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(mockWorkflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockLocations);
    when(mockLocations.jobs()).thenReturn(mockJobs);

    options = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setDataflowClient(mockWorkflowClient);
    options.setProject(PROJECT_ID);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://fakebucket/temp");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setGcpCredential(new TestCredential());
  }

  @Test
  public void testEmptyMetricUpdates() throws IOException {
    Job modelJob = new Job();
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    when(job.getState()).thenReturn(State.RUNNING);
    job.jobId = JOB_ID;

    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(null /* this is how the APIs represent empty metrics */);
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.queryMetrics();
    assertThat(ImmutableList.copyOf(result.counters()), is(empty()));
    assertThat(ImmutableList.copyOf(result.distributions()), is(empty()));
  }

  @Test
  public void testCachingMetricUpdates() throws IOException {
    Job modelJob = new Job();
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    when(job.getState()).thenReturn(State.DONE);
    job.jobId = JOB_ID;

    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(ImmutableList.<MetricUpdate>of());
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    verify(dataflowClient, times(0)).getJobMetrics(JOB_ID);
    dataflowMetrics.queryMetrics(null);
    verify(dataflowClient, times(1)).getJobMetrics(JOB_ID);
    dataflowMetrics.queryMetrics(null);
    verify(dataflowClient, times(1)).getJobMetrics(JOB_ID);
  }

  private MetricUpdate makeCounterMetricUpdate(String name, String namespace, String step,
      long scalar, boolean tentative) {
    MetricUpdate update = new MetricUpdate();
    update.setScalar(new BigDecimal(scalar));

    MetricStructuredName structuredName = new MetricStructuredName();
    structuredName.setName(name);
    structuredName.setOrigin("user");
    ImmutableMap.Builder contextBuilder = new ImmutableMap.Builder<String, String>();
    contextBuilder.put("step", step)
        .put("namespace", namespace);
    if (tentative) {
      contextBuilder.put("tentative", "true");
    }
    structuredName.setContext(contextBuilder.build());
    update.setName(structuredName);
    return update;
  }

  @Test
  public void testSingleCounterUpdates() throws IOException {
    JobMetrics jobMetrics = new JobMetrics();
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    when(job.getState()).thenReturn(State.RUNNING);
    job.jobId = JOB_ID;

    MetricUpdate update = new MetricUpdate();
    long stepValue = 1234L;
    update.setScalar(new BigDecimal(stepValue));

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    MetricUpdate mu1 = makeCounterMetricUpdate("counterName", "counterNamespace",
        "s2", 1234L, false);
    MetricUpdate mu1Tentative = makeCounterMetricUpdate("counterName",
        "counterNamespace", "s2", 1233L, true);
    jobMetrics.setMetrics(ImmutableList.of(mu1, mu1Tentative));
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.queryMetrics(null);
    assertThat(result.counters(), containsInAnyOrder(
        attemptedMetricsResult("counterNamespace", "counterName", "s2", 1233L)));
    assertThat(result.counters(), containsInAnyOrder(
        committedMetricsResult("counterNamespace", "counterName", "s2", 1234L)));
  }

  @Test
  public void testIgnoreDistributionButGetCounterUpdates() throws IOException {
    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    when(job.getState()).thenReturn(State.RUNNING);
    job.jobId = JOB_ID;

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(ImmutableList.of(
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, false),
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1234L, true),
        makeCounterMetricUpdate("otherCounter[MIN]", "otherNamespace", "s3", 0L, false),
        makeCounterMetricUpdate("otherCounter[MIN]", "otherNamespace", "s3", 0L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.queryMetrics(null);
    assertThat(result.counters(), containsInAnyOrder(
        attemptedMetricsResult("counterNamespace", "counterName", "s2", 1234L)));
    assertThat(result.counters(), containsInAnyOrder(
        committedMetricsResult("counterNamespace", "counterName", "s2", 1233L)));
  }

  @Test
  public void testMultipleCounterUpdates() throws IOException {
    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    when(job.getState()).thenReturn(State.RUNNING);
    job.jobId = JOB_ID;

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(ImmutableList.of(
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, false),
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1234L, true),
        makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, false),
        makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, true),
        makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1200L, false),
        makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1233L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.queryMetrics(null);
    assertThat(result.counters(), containsInAnyOrder(
        attemptedMetricsResult("counterNamespace", "counterName", "s2", 1234L),
        attemptedMetricsResult("otherNamespace", "otherCounter", "s3", 12L),
        attemptedMetricsResult("otherNamespace", "counterName", "s4", 1233L)));
    assertThat(result.counters(), containsInAnyOrder(
        committedMetricsResult("counterNamespace", "counterName", "s2", 1233L),
        committedMetricsResult("otherNamespace", "otherCounter", "s3", 12L),
        committedMetricsResult("otherNamespace", "counterName", "s4", 1200L)));
  }
}
