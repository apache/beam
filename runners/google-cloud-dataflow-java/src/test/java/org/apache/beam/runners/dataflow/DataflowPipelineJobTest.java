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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Get;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.GetMetrics;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Messages;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.testing.TestDataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.FastNanoClockAndSleeper;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.NoopPathValidator;
import org.apache.beam.sdk.util.TestCredential;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for DataflowPipelineJob.
 */
@RunWith(JUnit4.class)
public class DataflowPipelineJobTest {
  private static final String PROJECT_ID = "some-project";
  private static final String REGION_ID = "some-region-2b";
  private static final String JOB_ID = "1234";
  private static final String REPLACEMENT_JOB_ID = "4321";

  @Mock
  private Dataflow mockWorkflowClient;
  @Mock
  private Dataflow.Projects mockProjects;
  @Mock
  private Dataflow.Projects.Locations mockLocations;
  @Mock
  private Dataflow.Projects.Locations.Jobs mockJobs;
  @Rule
  public FastNanoClockAndSleeper fastClock = new FastNanoClockAndSleeper();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(DataflowPipelineJob.class);

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
    options.setRegion(REGION_ID);
    options.setRunner(DataflowRunner.class);
    options.setTempLocation("gs://fakebucket/temp");
    options.setPathValidatorClass(NoopPathValidator.class);
    options.setGcpCredential(new TestCredential());
  }

  /**
   * Validates that a given time is valid for the total time slept by a
   * BackOff given the number of retries and
   * an initial polling interval.
   *
   * @param pollingInterval The initial polling interval given.
   * @param retries The number of retries made
   * @param timeSleptMillis The amount of time slept by the clock. This is checked against the valid
   * interval.
   */
  private void checkValidInterval(Duration pollingInterval, int retries, long timeSleptMillis) {
    long highSum = 0;
    long lowSum = 0;
    for (int i = 0; i < retries; i++) {
      double currentInterval =
          pollingInterval.getMillis()
              * Math.pow(DataflowPipelineJob.DEFAULT_BACKOFF_EXPONENT, i);
      double randomOffset = 0.5 * currentInterval;
      highSum += Math.round(currentInterval + randomOffset);
      lowSum += Math.round(currentInterval - randomOffset);
    }
    assertThat(timeSleptMillis, allOf(greaterThanOrEqualTo(lowSum), lessThanOrEqualTo(highSum)));
  }

  @Test
  public void testWaitToFinishMessagesFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.DONE.name());
    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    MonitoringUtil.JobMessagesHandler jobHandler = mock(MonitoringUtil.JobMessagesHandler.class);
    Dataflow.Projects.Locations.Jobs.Messages mockMessages =
        mock(Dataflow.Projects.Locations.Jobs.Messages.class);
    Messages.List listRequest = mock(Dataflow.Projects.Locations.Jobs.Messages.List.class);
    when(mockJobs.messages()).thenReturn(mockMessages);
    when(mockMessages.list(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(listRequest);
    when(listRequest.setPageToken(eq((String) null))).thenReturn(listRequest);
    when(listRequest.execute()).thenThrow(SocketTimeoutException.class);
    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, dataflowAggregatorTransforms);

    State state = job.waitUntilFinish(
        Duration.standardMinutes(5), jobHandler, fastClock, fastClock);
    assertEquals(null, state);
  }

  public State mockWaitToFinishInState(State state) throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + state.name());
    if (state == State.UPDATED) {
      statusResponse.setReplacedByJobId(REPLACEMENT_JOB_ID);
    }

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);
    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, dataflowAggregatorTransforms);

    return job.waitUntilFinish(Duration.standardMinutes(1), null, fastClock, fastClock);
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#DONE DONE}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishDone() throws Exception {
    assertEquals(State.DONE, mockWaitToFinishInState(State.DONE));
    expectedLogs.verifyInfo(String.format("Job %s finished with status DONE.", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#FAILED FAILED}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishFailed() throws Exception {
    assertEquals(State.FAILED, mockWaitToFinishInState(State.FAILED));
    expectedLogs.verifyInfo(String.format("Job %s failed with status FAILED.", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the
   * {@link State#CANCELLED CANCELLED} state is terminal.
   */
  @Test
  public void testWaitToFinishCancelled() throws Exception {
    assertEquals(State.CANCELLED, mockWaitToFinishInState(State.CANCELLED));
    expectedLogs.verifyInfo(String.format("Job %s finished with status CANCELLED", JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#UPDATED UPDATED}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishUpdated() throws Exception {
    assertEquals(State.UPDATED, mockWaitToFinishInState(State.UPDATED));
    expectedLogs.verifyInfo(String.format(
        "Job %s has been updated and is running as the new job with id %s.",
        JOB_ID, REPLACEMENT_JOB_ID));
  }

  /**
   * Tests that the {@link DataflowPipelineJob} understands that the {@link State#UNKNOWN UNKNOWN}
   * state is terminal.
   */
  @Test
  public void testWaitToFinishUnknown() throws Exception {
    assertEquals(null, mockWaitToFinishInState(State.UNKNOWN));
    expectedLogs.verifyWarn("No terminal state was returned. State value UNKNOWN");
  }

  @Test
  public void testWaitToFinishFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);
    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, dataflowAggregatorTransforms);

    long startTime = fastClock.nanoTime();
    State state = job.waitUntilFinish(Duration.standardMinutes(5), null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(DataflowPipelineJob.MESSAGES_POLLING_INTERVAL,
        DataflowPipelineJob.MESSAGES_POLLING_RETRIES, timeDiff);
  }

  @Test
  public void testWaitToFinishTimeFail() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);
    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, dataflowAggregatorTransforms);
    long startTime = fastClock.nanoTime();
    State state = job.waitUntilFinish(Duration.millis(4), null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    // Should only have slept for the 4 ms allowed.
    assertEquals(4L, timeDiff);
  }

  @Test
  public void testCumulativeTimeOverflow() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    FastNanoClockAndFuzzySleeper clock = new FastNanoClockAndFuzzySleeper();

    DataflowPipelineJob job = new DataflowPipelineJob(
        JOB_ID, options, dataflowAggregatorTransforms);
    long startTime = clock.nanoTime();
    State state = job.waitUntilFinish(Duration.millis(4), null, clock, clock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(clock.nanoTime() - startTime);
    // Should only have slept for the 4 ms allowed.
    assertThat(timeDiff, lessThanOrEqualTo(4L));
  }

  @Test
  public void testGetStateReturnsServiceState() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.RUNNING.name());

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job = new DataflowPipelineJob(
        JOB_ID, options, dataflowAggregatorTransforms);

    assertEquals(
        State.RUNNING,
        job.getStateWithRetries(DataflowPipelineJob.STATUS_BACKOFF_FACTORY.backoff(), fastClock));
  }

  @Test
  public void testGetStateWithExceptionReturnsUnknown() throws Exception {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);
    DataflowAggregatorTransforms dataflowAggregatorTransforms =
        mock(DataflowAggregatorTransforms.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, dataflowAggregatorTransforms);

    long startTime = fastClock.nanoTime();
    assertEquals(
        State.UNKNOWN,
        job.getStateWithRetries(DataflowPipelineJob.STATUS_BACKOFF_FACTORY.backoff(), fastClock));
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(DataflowPipelineJob.STATUS_POLLING_INTERVAL,
        DataflowPipelineJob.STATUS_POLLING_RETRIES, timeDiff);
  }

  @Test
  public void testGetAggregatorValuesWithNoMetricUpdatesReturnsEmptyValue()
      throws IOException, AggregatorRetrievalException {
    Aggregator<?, ?> aggregator = mock(Aggregator.class);
    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform =
        appliedPTransform(fullName, pTransform, Pipeline.create(options));

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(aggregator, pTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(appliedTransform, stepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    JobMetrics jobMetrics = new JobMetrics();
    when(getMetrics.execute()).thenReturn(jobMetrics);

    jobMetrics.setMetrics(ImmutableList.<MetricUpdate>of());

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    AggregatorValues<?> values = job.getAggregatorValues(aggregator);

    assertThat(values.getValues(), empty());
  }

  @Test
  public void testGetAggregatorValuesWithNullMetricUpdatesReturnsEmptyValue()
      throws IOException, AggregatorRetrievalException {
    Aggregator<?, ?> aggregator = mock(Aggregator.class);
    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform =
        appliedPTransform(fullName, pTransform, Pipeline.create(options));

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(aggregator, pTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(appliedTransform, stepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    JobMetrics jobMetrics = new JobMetrics();
    when(getMetrics.execute()).thenReturn(jobMetrics);

    jobMetrics.setMetrics(null);

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    AggregatorValues<?> values = job.getAggregatorValues(aggregator);

    assertThat(values.getValues(), empty());
  }

  @Test
  public void testGetAggregatorValuesWithSingleMetricUpdateReturnsSingletonCollection()
      throws IOException, AggregatorRetrievalException {
    CombineFn<Long, long[], Long> combineFn = Sum.ofLongs();
    String aggregatorName = "agg";
    Aggregator<Long, Long> aggregator = new TestAggregator<>(combineFn, aggregatorName);
    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform =
        appliedPTransform(fullName, pTransform, Pipeline.create(options));

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(aggregator, pTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(appliedTransform, stepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    JobMetrics jobMetrics = new JobMetrics();
    when(getMetrics.execute()).thenReturn(jobMetrics);

    MetricUpdate update = new MetricUpdate();
    long stepValue = 1234L;
    update.setScalar(new BigDecimal(stepValue));

    MetricStructuredName structuredName = new MetricStructuredName();
    structuredName.setName(aggregatorName);
    structuredName.setContext(ImmutableMap.of("step", stepName));
    update.setName(structuredName);

    jobMetrics.setMetrics(ImmutableList.of(update));

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    AggregatorValues<Long> values = job.getAggregatorValues(aggregator);

    assertThat(values.getValuesAtSteps(), hasEntry(fullName, stepValue));
    assertThat(values.getValuesAtSteps().size(), equalTo(1));
    assertThat(values.getValues(), contains(stepValue));
    assertThat(values.getTotalValue(combineFn), equalTo(Long.valueOf(stepValue)));
  }

  @Test
  public void testGetAggregatorValuesWithMultipleMetricUpdatesReturnsCollection()
      throws IOException, AggregatorRetrievalException {
    CombineFn<Long, long[], Long> combineFn = Sum.ofLongs();
    String aggregatorName = "agg";
    Aggregator<Long, Long> aggregator = new TestAggregator<>(combineFn, aggregatorName);

    Pipeline p = Pipeline.create(options);

    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform = appliedPTransform(fullName, pTransform, p);

    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> otherTransform = mock(PTransform.class);
    String otherStepName = "s88";
    String otherFullName = "Spam/Ham/Eggs";
    AppliedPTransform<?, ?, ?> otherAppliedTransform =
        appliedPTransform(otherFullName, otherTransform, p);

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(
            aggregator, pTransform, aggregator, otherTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(
            appliedTransform, stepName, otherAppliedTransform, otherStepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    JobMetrics jobMetrics = new JobMetrics();
    when(getMetrics.execute()).thenReturn(jobMetrics);

    MetricUpdate updateOne = new MetricUpdate();
    long stepValue = 1234L;
    updateOne.setScalar(new BigDecimal(stepValue));

    MetricStructuredName structuredNameOne = new MetricStructuredName();
    structuredNameOne.setName(aggregatorName);
    structuredNameOne.setContext(ImmutableMap.of("step", stepName));
    updateOne.setName(structuredNameOne);

    MetricUpdate updateTwo = new MetricUpdate();
    long stepValueTwo = 1024L;
    updateTwo.setScalar(new BigDecimal(stepValueTwo));

    MetricStructuredName structuredNameTwo = new MetricStructuredName();
    structuredNameTwo.setName(aggregatorName);
    structuredNameTwo.setContext(ImmutableMap.of("step", otherStepName));
    updateTwo.setName(structuredNameTwo);

    jobMetrics.setMetrics(ImmutableList.of(updateOne, updateTwo));

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    AggregatorValues<Long> values = job.getAggregatorValues(aggregator);

    assertThat(values.getValuesAtSteps(), hasEntry(fullName, stepValue));
    assertThat(values.getValuesAtSteps(), hasEntry(otherFullName, stepValueTwo));
    assertThat(values.getValuesAtSteps().size(), equalTo(2));
    assertThat(values.getValues(), containsInAnyOrder(stepValue, stepValueTwo));
    assertThat(values.getTotalValue(combineFn), equalTo(Long.valueOf(stepValue + stepValueTwo)));
  }

  @Test
  public void testGetAggregatorValuesWithUnrelatedMetricUpdateIgnoresUpdate()
      throws IOException, AggregatorRetrievalException {
    CombineFn<Long, long[], Long> combineFn = Sum.ofLongs();
    String aggregatorName = "agg";
    Aggregator<Long, Long> aggregator = new TestAggregator<>(combineFn, aggregatorName);
    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform =
        appliedPTransform(fullName, pTransform, Pipeline.create(options));

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(aggregator, pTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(appliedTransform, stepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    JobMetrics jobMetrics = new JobMetrics();
    when(getMetrics.execute()).thenReturn(jobMetrics);

    MetricUpdate ignoredUpdate = new MetricUpdate();
    ignoredUpdate.setScalar(null);

    MetricStructuredName ignoredName = new MetricStructuredName();
    ignoredName.setName("ignoredAggregator.elementCount.out0");
    ignoredName.setContext(null);
    ignoredUpdate.setName(ignoredName);

    jobMetrics.setMetrics(ImmutableList.of(ignoredUpdate));

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    AggregatorValues<Long> values = job.getAggregatorValues(aggregator);

    assertThat(values.getValuesAtSteps().entrySet(), empty());
    assertThat(values.getValues(), empty());
  }

  @Test
  public void testGetAggregatorValuesWithUnusedAggregatorThrowsException()
      throws AggregatorRetrievalException {
    Aggregator<?, ?> aggregator = mock(Aggregator.class);

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of().asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of());

    DataflowPipelineJob job =
        new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("not used in this pipeline");
    job.getAggregatorValues(aggregator);
  }

  @Test
  public void testGetAggregatorValuesWhenClientThrowsExceptionThrowsAggregatorRetrievalException()
      throws IOException, AggregatorRetrievalException {
    CombineFn<Long, long[], Long> combineFn = Sum.ofLongs();
    String aggregatorName = "agg";
    Aggregator<Long, Long> aggregator = new TestAggregator<>(combineFn, aggregatorName);
    @SuppressWarnings("unchecked")
    PTransform<PInput, POutput> pTransform = mock(PTransform.class);
    String stepName = "s1";
    String fullName = "Foo/Bar/Baz";
    AppliedPTransform<?, ?, ?> appliedTransform =
        appliedPTransform(fullName, pTransform, Pipeline.create(options));

    DataflowAggregatorTransforms aggregatorTransforms = new DataflowAggregatorTransforms(
        ImmutableSetMultimap.<Aggregator<?, ?>, PTransform<?, ?>>of(aggregator, pTransform).asMap(),
        ImmutableMap.<AppliedPTransform<?, ?, ?>, String>of(appliedTransform, stepName));

    GetMetrics getMetrics = mock(GetMetrics.class);
    when(mockJobs.getMetrics(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getMetrics);
    IOException cause = new IOException();
    when(getMetrics.execute()).thenThrow(cause);

    Get getState = mock(Get.class);
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(getState);
    Job modelJob = new Job();
    when(getState.execute()).thenReturn(modelJob);
    modelJob.setCurrentState(State.RUNNING.toString());

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, aggregatorTransforms);

    thrown.expect(AggregatorRetrievalException.class);
    thrown.expectCause(is(cause));
    thrown.expectMessage(aggregator.toString());
    thrown.expectMessage("when retrieving Aggregator values for");
    job.getAggregatorValues(aggregator);
  }

  private static class TestAggregator<InT, OutT> implements Aggregator<InT, OutT> {
    private final CombineFn<InT, ?, OutT> combineFn;
    private final String name;

    public TestAggregator(CombineFn<InT, ?, OutT> combineFn, String name) {
      this.combineFn = combineFn;
      this.name = name;
    }

    @Override
    public void addValue(InT value) {
      throw new AssertionError();
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public CombineFn<InT, ?, OutT> getCombineFn() {
      return combineFn;
    }
  }

  private AppliedPTransform<?, ?, ?> appliedPTransform(
      String fullName, PTransform<PInput, POutput> transform, Pipeline p) {
    PInput input = mock(PInput.class);
    when(input.getPipeline()).thenReturn(p);
    return AppliedPTransform.of(
        fullName,
        Collections.<TupleTag<?>, PValue>emptyMap(),
        Collections.<TupleTag<?>, PValue>emptyMap(),
        transform,
        p);
  }


  private static class FastNanoClockAndFuzzySleeper implements NanoClock, Sleeper {

    private long fastNanoTime;

    public FastNanoClockAndFuzzySleeper() {
      fastNanoTime = NanoClock.SYSTEM.nanoTime();
    }

    @Override
    public long nanoTime() {
      return fastNanoTime;
    }

    @Override
    public void sleep(long millis) throws InterruptedException {
      fastNanoTime += millis * 1000000L + ThreadLocalRandom.current().nextInt(500000);
    }
  }

  @Test
  public void testCancelUnterminatedJobThatSucceeds() throws IOException {
    Dataflow.Projects.Locations.Jobs.Update update =
        mock(Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenReturn(new Job().setCurrentState("JOB_STATE_CANCELLED"));

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, null);

    assertEquals(State.CANCELLED, job.cancel());
    Job content = new Job();
    content.setProjectId(PROJECT_ID);
    content.setId(JOB_ID);
    content.setRequestedState("JOB_STATE_CANCELLED");
    verify(mockJobs).update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), eq(content));
    verifyNoMoreInteractions(mockJobs);
  }

  @Test
  public void testCancelUnterminatedJobThatFails() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update = mock(
        Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException("Some random IOException"));

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, null);

    thrown.expect(IOException.class);
    thrown.expectMessage("Failed to cancel job in state RUNNING, "
        + "please go to the Developers Console to cancel it manually:");
    job.cancel();
  }

  /**
   * Test that {@link DataflowPipelineJob#cancel} doesn't throw if the Dataflow service returns
   * non-terminal state even though the cancel API call failed, which can happen in practice.
   *
   * <p>TODO: delete this code if the API calls become consistent.
   */
  @Test
  public void testCancelTerminatedJobWithStaleState() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest =
        mock(Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_RUNNING");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update = mock(
        Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException("Job has terminated in state SUCCESS"));

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, null);
    State returned = job.cancel();
    assertThat(returned, equalTo(State.RUNNING));
    expectedLogs.verifyWarn("Cancel failed because job is already terminated.");
  }

  @Test
  public void testCancelTerminatedJob() throws IOException {
    Dataflow.Projects.Locations.Jobs.Get statusRequest = mock(
        Dataflow.Projects.Locations.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_FAILED");
    when(mockJobs.get(PROJECT_ID, REGION_ID, JOB_ID)).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    Dataflow.Projects.Locations.Jobs.Update update = mock(
        Dataflow.Projects.Locations.Jobs.Update.class);
    when(mockJobs.update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), any(Job.class)))
        .thenReturn(update);
    when(update.execute()).thenThrow(new IOException());

    DataflowPipelineJob job = new DataflowPipelineJob(JOB_ID, options, null);

    assertEquals(State.FAILED, job.cancel());
    Job content = new Job();
    content.setProjectId(PROJECT_ID);
    content.setId(JOB_ID);
    content.setRequestedState("JOB_STATE_CANCELLED");
    verify(mockJobs).update(eq(PROJECT_ID), eq(REGION_ID), eq(JOB_ID), eq(content));
    verify(mockJobs).get(PROJECT_ID, REGION_ID, JOB_ID);
    verifyNoMoreInteractions(mockJobs);
  }
}
