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

import static org.apache.beam.sdk.metrics.MetricResultsMatchers.attemptedMetricsResult;
import static org.apache.beam.sdk.metrics.MetricResultsMatchers.committedMetricsResult;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Set;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.DataflowTemplateJob;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.metrics.StringSetResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DataflowMetrics}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class DataflowMetricsTest {
  private static final String PROJECT_ID = "some-project";
  private static final String JOB_ID = "1234";

  @Mock private Dataflow mockWorkflowClient;
  @Mock private Dataflow.Projects mockProjects;
  @Mock private Dataflow.Projects.Locations mockLocations;
  @Mock private Dataflow.Projects.Locations.Jobs mockJobs;

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
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);

    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(null /* this is how the APIs represent empty metrics */);
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(ImmutableList.copyOf(result.getCounters()), is(empty()));
    assertThat(ImmutableList.copyOf(result.getDistributions()), is(empty()));
    assertThat(ImmutableList.copyOf(result.getStringSets()), is(empty()));
  }

  @Test
  public void testCachingMetricUpdates() throws IOException {
    Job modelJob = new Job();
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.DONE);
    when(job.getJobId()).thenReturn(JOB_ID);

    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(ImmutableList.of());
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    verify(dataflowClient, times(0)).getJobMetrics(JOB_ID);
    dataflowMetrics.allMetrics();
    verify(dataflowClient, times(1)).getJobMetrics(JOB_ID);
    dataflowMetrics.allMetrics();
    verify(dataflowClient, times(1)).getJobMetrics(JOB_ID);
  }

  private MetricUpdate setStructuredName(
      MetricUpdate update, String name, String namespace, String step, boolean tentative) {
    MetricStructuredName structuredName = new MetricStructuredName();
    structuredName.setName(name);
    structuredName.setOrigin("user");
    ImmutableMap.Builder contextBuilder = new ImmutableMap.Builder<>();
    contextBuilder.put("step", step).put("namespace", namespace);
    if (tentative) {
      contextBuilder.put("tentative", "true");
    }
    structuredName.setContext(contextBuilder.build());
    update.setName(structuredName);
    return update;
  }

  private MetricUpdate makeDistributionMetricUpdate(
      String name,
      String namespace,
      String step,
      Long sum,
      Long count,
      Long min,
      Long max,
      boolean tentative) {
    MetricUpdate update = new MetricUpdate();
    ArrayMap<String, BigDecimal> distribution = ArrayMap.create();
    distribution.add("count", new BigDecimal(count));
    distribution.add("mean", new BigDecimal(sum / count));
    distribution.add("sum", new BigDecimal(sum));
    distribution.add("min", new BigDecimal(min));
    distribution.add("max", new BigDecimal(max));
    update.setDistribution(distribution);
    return setStructuredName(update, name, namespace, step, tentative);
  }

  private MetricUpdate makeCounterMetricUpdate(
      String name, String namespace, String step, long scalar, boolean tentative) {
    MetricUpdate update = new MetricUpdate();
    update.setScalar(new BigDecimal(scalar));
    return setStructuredName(update, name, namespace, step, tentative);
  }

  private MetricUpdate makeStringSetMetricUpdate(
      String name, String namespace, String step, Set<String> setValues, boolean tentative) {
    MetricUpdate update = new MetricUpdate();
    update.setSet(setValues);
    return setStructuredName(update, name, namespace, step, tentative);
  }

  @Test
  public void testSingleCounterUpdates() throws IOException {
    AppliedPTransform<?, ?, ?> myStep = mock(AppliedPTransform.class);
    when(myStep.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep, "s2");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    MetricUpdate update = new MetricUpdate();
    long stepValue = 1234L;
    update.setScalar(new BigDecimal(stepValue));

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    MetricUpdate mu1 =
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1234L, false);
    MetricUpdate mu1Tentative =
        makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, true);
    jobMetrics.setMetrics(ImmutableList.of(mu1, mu1Tentative));
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("counterNamespace", "counterName", "myStepName", 1234L)));
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("counterNamespace", "counterName", "myStepName", 1234L)));
  }

  @Test
  public void testSingleStringSetUpdates() throws IOException {
    AppliedPTransform<?, ?, ?> myStep = mock(AppliedPTransform.class);
    when(myStep.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep, "s2");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    MetricUpdate mu1 =
        makeStringSetMetricUpdate(
            "counterName", "counterNamespace", "s2", ImmutableSet.of("ab", "cd"), false);
    MetricUpdate mu1Tentative =
        makeStringSetMetricUpdate(
            "counterName", "counterNamespace", "s2", ImmutableSet.of("ab", "cd"), true);
    jobMetrics.setMetrics(ImmutableList.of(mu1, mu1Tentative));
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(
        result.getStringSets(),
        containsInAnyOrder(
            attemptedMetricsResult(
                "counterNamespace",
                "counterName",
                "myStepName",
                StringSetResult.create(ImmutableSet.of("ab", "cd")))));
    assertThat(
        result.getStringSets(),
        containsInAnyOrder(
            committedMetricsResult(
                "counterNamespace",
                "counterName",
                "myStepName",
                StringSetResult.create(ImmutableSet.of("ab", "cd")))));
  }

  @Test
  public void testIgnoreDistributionButGetCounterUpdates() throws IOException {
    AppliedPTransform<?, ?, ?> myStep = mock(AppliedPTransform.class);
    when(myStep.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep, "s2");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(
        ImmutableList.of(
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, false),
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, true),
            makeCounterMetricUpdate("otherCounter[MIN]", "otherNamespace", "s2", 0L, false),
            makeCounterMetricUpdate("otherCounter[MIN]", "otherNamespace", "s2", 0L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("counterNamespace", "counterName", "myStepName", 1233L)));
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("counterNamespace", "counterName", "myStepName", 1233L)));
  }

  @Test
  public void testDistributionUpdates() throws IOException {
    AppliedPTransform<?, ?, ?> myStep2 = mock(AppliedPTransform.class);
    when(myStep2.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep2, "s2");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(
        ImmutableList.of(
            makeDistributionMetricUpdate(
                "distributionName", "distributionNamespace", "s2", 18L, 2L, 2L, 16L, false),
            makeDistributionMetricUpdate(
                "distributionName", "distributionNamespace", "s2", 18L, 2L, 2L, 16L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(
        result.getDistributions(),
        contains(
            attemptedMetricsResult(
                "distributionNamespace",
                "distributionName",
                "myStepName",
                DistributionResult.create(18, 2, 2, 16))));
    assertThat(
        result.getDistributions(),
        contains(
            committedMetricsResult(
                "distributionNamespace",
                "distributionName",
                "myStepName",
                DistributionResult.create(18, 2, 2, 16))));
  }

  @Test
  public void testDistributionUpdatesStreaming() throws IOException {
    AppliedPTransform<?, ?, ?> myStep2 = mock(AppliedPTransform.class);
    when(myStep2.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep2, "s2");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(true);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(
        ImmutableList.of(
            makeDistributionMetricUpdate(
                "distributionName", "distributionNamespace", "s2", 18L, 2L, 2L, 16L, false),
            makeDistributionMetricUpdate(
                "distributionName", "distributionNamespace", "s2", 18L, 2L, 2L, 16L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    try {
      result.getDistributions().iterator().next().getCommitted();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertThat(
          expected.getMessage(),
          containsString(
              "This runner does not currently support committed"
                  + " metrics results. Please use 'attempted' instead."));
    }
    assertThat(
        result.getDistributions(),
        contains(
            attemptedMetricsResult(
                "distributionNamespace",
                "distributionName",
                "myStepName",
                DistributionResult.create(18, 2, 2, 16))));
  }

  @Test
  public void testMultipleCounterUpdates() throws IOException {
    AppliedPTransform<?, ?, ?> myStep2 = mock(AppliedPTransform.class);
    when(myStep2.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep2, "s2");
    AppliedPTransform<?, ?, ?> myStep3 = mock(AppliedPTransform.class);
    when(myStep3.getFullName()).thenReturn("myStepName3");
    transformStepNames.put(myStep3, "s3");
    AppliedPTransform<?, ?, ?> myStep4 = mock(AppliedPTransform.class);
    when(myStep4.getFullName()).thenReturn("myStepName4");
    transformStepNames.put(myStep4, "s4");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(false);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(
        ImmutableList.of(
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, false),
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1234L, true),
            makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, false),
            makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, true),
            makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1200L, false),
            makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1233L, true),
            // The following counter can not have its name translated thus it won't appear.
            makeCounterMetricUpdate("lostName", "otherNamespace", "s5", 1200L, false),
            makeCounterMetricUpdate("lostName", "otherNamespace", "s5", 1200L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("counterNamespace", "counterName", "myStepName", 1233L),
            attemptedMetricsResult("otherNamespace", "otherCounter", "myStepName3", 12L),
            attemptedMetricsResult("otherNamespace", "counterName", "myStepName4", 1200L)));
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            committedMetricsResult("counterNamespace", "counterName", "myStepName", 1233L),
            committedMetricsResult("otherNamespace", "otherCounter", "myStepName3", 12L),
            committedMetricsResult("otherNamespace", "counterName", "myStepName4", 1200L)));
  }

  @Test
  public void testMultipleCounterUpdatesStreaming() throws IOException {
    AppliedPTransform<?, ?, ?> myStep2 = mock(AppliedPTransform.class);
    when(myStep2.getFullName()).thenReturn("myStepName");
    BiMap<AppliedPTransform<?, ?, ?>, String> transformStepNames = HashBiMap.create();
    transformStepNames.put(myStep2, "s2");
    AppliedPTransform<?, ?, ?> myStep3 = mock(AppliedPTransform.class);
    when(myStep3.getFullName()).thenReturn("myStepName3");
    transformStepNames.put(myStep3, "s3");
    AppliedPTransform<?, ?, ?> myStep4 = mock(AppliedPTransform.class);
    when(myStep4.getFullName()).thenReturn("myStepName4");
    transformStepNames.put(myStep4, "s4");

    JobMetrics jobMetrics = new JobMetrics();
    DataflowClient dataflowClient = mock(DataflowClient.class);
    when(dataflowClient.getJobMetrics(JOB_ID)).thenReturn(jobMetrics);
    DataflowPipelineJob job = mock(DataflowPipelineJob.class);
    DataflowPipelineOptions options = mock(DataflowPipelineOptions.class);
    when(options.isStreaming()).thenReturn(true);
    when(job.getDataflowOptions()).thenReturn(options);
    when(job.getState()).thenReturn(State.RUNNING);
    when(job.getJobId()).thenReturn(JOB_ID);
    when(job.getTransformStepNames()).thenReturn(transformStepNames);

    // The parser relies on the fact that one tentative and one committed metric update exist in
    // the job metrics results.
    jobMetrics.setMetrics(
        ImmutableList.of(
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1233L, false),
            makeCounterMetricUpdate("counterName", "counterNamespace", "s2", 1234L, true),
            makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, false),
            makeCounterMetricUpdate("otherCounter", "otherNamespace", "s3", 12L, true),
            makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1200L, false),
            makeCounterMetricUpdate("counterName", "otherNamespace", "s4", 1233L, true)));

    DataflowMetrics dataflowMetrics = new DataflowMetrics(job, dataflowClient);
    MetricQueryResults result = dataflowMetrics.allMetrics();
    try {
      result.getCounters().iterator().next().getCommitted();
      fail("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      assertThat(
          expected.getMessage(),
          containsString(
              "This runner does not currently support committed"
                  + " metrics results. Please use 'attempted' instead."));
    }
    assertThat(
        result.getCounters(),
        containsInAnyOrder(
            attemptedMetricsResult("counterNamespace", "counterName", "myStepName", 1233L),
            attemptedMetricsResult("otherNamespace", "otherCounter", "myStepName3", 12L),
            attemptedMetricsResult("otherNamespace", "counterName", "myStepName4", 1200L)));
  }

  @Test
  public void testTemplateJobMetricsThrowsUsefulError() throws Exception {
    DataflowClient dataflowClient = mock(DataflowClient.class);
    DataflowMetrics metrics = new DataflowMetrics(new DataflowTemplateJob(), dataflowClient);

    assertThrows(
        "The result of template creation should not be used.",
        UnsupportedOperationException.class,
        () -> metrics.allMetrics());
    assertThrows(
        "The result of template creation should not be used.",
        UnsupportedOperationException.class,
        () -> metrics.queryMetrics(MetricsFilter.builder().build()));
  }
}
