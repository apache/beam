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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.storage.NoopPathValidator;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** Tests for {@link TestDataflowRunner}. */
@RunWith(JUnit4.class)
public class TestDataflowRunnerTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Mock private DataflowClient mockClient;

  private TestDataflowPipelineOptions options;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    options = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setAppName("TestAppName");
    options.setProject("test-project");
    options.setRegion("some-region1");
    options.setTempLocation("gs://test/temp/location");
    options.setTempRoot("gs://test");
    options.setGcpCredential(new TestCredential());
    options.setRunner(TestDataflowRunner.class);
    options.setPathValidatorClass(NoopPathValidator.class);
  }

  @Test
  public void testToString() {
    assertEquals(
        "TestDataflowRunner#TestAppName", TestDataflowRunner.fromOptions(options).toString());
  }

  @Test
  public void testRunBatchJobThatSucceeds() throws Exception {
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    assertEquals(mockJob, runner.run(p, mockRunner));
  }

  /**
   * Job success on Dataflow means that it handled transient errors (if any) successfully by
   * retrying failed bundles.
   */
  @Test
  public void testRunBatchJobThatSucceedsDespiteTransientErrors() throws Exception {
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenAnswer(
            invocation -> {
              JobMessage message = new JobMessage();
              message.setMessageText("TransientError");
              message.setTime(TimeUtil.toCloudTime(Instant.now()));
              message.setMessageImportance("JOB_MESSAGE_ERROR");
              ((JobMessagesHandler) invocation.getArguments()[1]).process(Arrays.asList(message));
              return State.DONE;
            });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    assertEquals(mockJob, runner.run(p, mockRunner));
  }

  /**
   * Tests that when a batch job terminates in a failure state even if all assertions passed, it
   * throws an error to that effect.
   */
  @Test
  public void testRunBatchJobThatFails() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, false /* tentative */));
    expectedException.expect(RuntimeException.class);
    runner.run(p, mockRunner);
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testBatchPipelineFailsIfException() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenAnswer(
            invocation -> {
              JobMessage message = new JobMessage();
              message.setMessageText("FooException");
              message.setTime(TimeUtil.toCloudTime(Instant.now()));
              message.setMessageImportance("JOB_MESSAGE_ERROR");
              ((JobMessagesHandler) invocation.getArguments()[1]).process(Arrays.asList(message));
              return State.CANCELLED;
            });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("FooException"));
      verify(mockJob, never()).cancel();
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  /** A streaming job that terminates with no error messages is a success. */
  @Test
  public void testRunStreamingJobUsingPAssertThatSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    runner.run(p, mockRunner);
  }

  @Test
  public void testRunStreamingJobNotUsingPAssertThatSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    p.apply(Create.of(1, 2, 3));

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockStreamingMetricResponse(ImmutableMap.of()));
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    runner.run(p, mockRunner);
  }

  /**
   * Tests that a streaming job with a false {@link PAssert} fails.
   *
   * <p>Currently, this failure is indistinguishable from a non-{@link PAssert} failure, because it
   * is detected only by failure job messages. With fuller metric support, this can detect a PAssert
   * failure via metrics and raise an {@link AssertionError} in just that case.
   */
  @Test
  public void testRunStreamingJobThatFails() throws Exception {
    testStreamingPipelineFailsIfException();
  }

  private JobMetrics generateMockMetricResponse(boolean success, boolean tentative)
      throws Exception {
    List<MetricUpdate> metrics = generateMockMetrics(success, tentative);
    return buildJobMetrics(metrics);
  }

  private List<MetricUpdate> generateMockMetrics(boolean success, boolean tentative) {
    MetricStructuredName name = new MetricStructuredName();
    name.setName(success ? "PAssertSuccess" : "PAssertFailure");
    name.setContext(tentative ? ImmutableMap.of("tentative", "") : ImmutableMap.of());

    MetricUpdate metric = new MetricUpdate();
    metric.setName(name);
    metric.setScalar(BigDecimal.ONE);
    return Lists.newArrayList(metric);
  }

  private JobMetrics generateMockStreamingMetricResponse(Map<String, BigDecimal> metricMap)
      throws IOException {
    return buildJobMetrics(generateMockStreamingMetrics(metricMap));
  }

  private List<MetricUpdate> generateMockStreamingMetrics(Map<String, BigDecimal> metricMap) {
    List<MetricUpdate> metrics = Lists.newArrayList();
    for (Map.Entry<String, BigDecimal> entry : metricMap.entrySet()) {
      MetricStructuredName name = new MetricStructuredName();
      name.setName(entry.getKey());

      MetricUpdate metric = new MetricUpdate();
      metric.setName(name);
      metric.setScalar(entry.getValue());
      metrics.add(metric);
    }
    return metrics;
  }

  private JobMetrics buildJobMetrics(List<MetricUpdate> metricList) {
    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(metricList);
    // N.B. Setting the factory is necessary in order to get valid JSON.
    jobMetrics.setFactory(Transport.getJsonFactory());
    return jobMetrics;
  }

  /**
   * Tests that a tentative {@code true} from metrics indicates that every {@link PAssert} has
   * succeeded.
   */
  @Test
  public void testCheckingForSuccessWhenPAssertSucceeds() throws Exception {
    DataflowPipelineJob job = spy(new DataflowPipelineJob(mockClient, "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(buildJobMetrics(generateMockMetrics(true /* success */, true /* tentative */)));

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    doReturn(State.DONE).when(job).getState();
    assertThat(runner.checkForPAssertSuccess(job), equalTo(Optional.of(true)));
  }

  /**
   * Tests that when we just see a tentative failure for a {@link PAssert} it is considered a
   * conclusive failure.
   */
  @Test
  public void testCheckingForSuccessWhenPAssertFails() throws Exception {
    DataflowPipelineJob job = spy(new DataflowPipelineJob(mockClient, "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(
            buildJobMetrics(generateMockMetrics(false /* success */, true /* tentative */)));

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    doReturn(State.DONE).when(job).getState();
    assertThat(runner.checkForPAssertSuccess(job), equalTo(Optional.of(false)));
  }

  @Test
  public void testCheckingForSuccessSkipsNonTentativeMetrics() throws Exception {
    DataflowPipelineJob job = spy(new DataflowPipelineJob(mockClient, "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(
            buildJobMetrics(generateMockMetrics(true /* success */, false /* tentative */)));

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    runner.updatePAssertCount(p);
    doReturn(State.RUNNING).when(job).getState();
    assertThat(runner.checkForPAssertSuccess(job), equalTo(Optional.<Boolean>absent()));
  }

  /**
   * Tests that if a streaming pipeline crash loops for a non-assertion reason that the test run
   * throws an {@link AssertionError}.
   *
   * <p>This is a known limitation/bug of the runner that it does not distinguish the two modes of
   * failure.
   */
  @Test
  public void testStreamingPipelineFailsIfException() throws Exception {
    options.setStreaming(true);
    Pipeline pipeline = TestPipeline.create(options);
    PCollection<Integer> pc = pipeline.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenAnswer(
            invocation -> {
              JobMessage message = new JobMessage();
              message.setMessageText("FooException");
              message.setTime(TimeUtil.toCloudTime(Instant.now()));
              message.setMessageImportance("JOB_MESSAGE_ERROR");
              ((JobMessagesHandler) invocation.getArguments()[1]).process(Arrays.asList(message));
              return State.CANCELLED;
            });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);

    expectedException.expect(RuntimeException.class);
    runner.run(pipeline, mockRunner);
  }

  @Test
  public void testGetJobMetricsThatSucceeds() throws Exception {
    DataflowPipelineJob job = spy(new DataflowPipelineJob(mockClient, "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    p.apply(Create.of(1, 2, 3));

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    JobMetrics metrics = runner.getJobMetrics(job);

    assertEquals(1, metrics.getMetrics().size());
    assertEquals(
        generateMockMetrics(true /* success */, true /* tentative */), metrics.getMetrics());
  }

  @Test
  public void testGetJobMetricsThatFailsForException() throws Exception {
    DataflowPipelineJob job = spy(new DataflowPipelineJob(mockClient, "test-job", options, null));
    Pipeline p = TestPipeline.create(options);
    p.apply(Create.of(1, 2, 3));

    when(mockClient.getJobMetrics(anyString())).thenThrow(new IOException());
    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    assertNull(runner.getJobMetrics(job));
  }

  @Test
  public void testBatchOnCreateMatcher() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testStreamingOnCreateMatcher() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testBatchOnSuccessMatcherWhenPipelineSucceeds() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnSuccessMatcher(new TestSuccessMatcher(mockJob, 1));

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  /**
   * Tests that when a streaming pipeline terminates and doesn't fail due to {@link PAssert} that
   * the {@link TestPipelineOptions#setOnSuccessMatcher(SerializableMatcher) on success matcher} is
   * invoked.
   */
  @Test
  public void testStreamingOnSuccessMatcherWhenPipelineSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnSuccessMatcher(new TestSuccessMatcher(mockJob, 1));

    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

  @Test
  public void testBatchOnSuccessMatcherWhenPipelineFails() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnSuccessMatcher(new TestFailureMatcher());

    when(mockClient.getJobMetrics(anyString()))
        .thenReturn(generateMockMetricResponse(false /* success */, true /* tentative */));
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      verify(mockJob, Mockito.times(1))
          .waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class));
      return;
    }
    fail("Expected an exception on pipeline failure.");
  }

  /**
   * Tests that when a streaming pipeline terminates in FAIL that the {@link
   * TestPipelineOptions#setOnSuccessMatcher(SerializableMatcher) on success matcher} is not
   * invoked.
   */
  @Test
  public void testStreamingOnSuccessMatcherWhenPipelineFails() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    final DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.FAILED);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = TestDataflowRunner.fromOptionsAndClient(options, mockClient);
    options.as(TestPipelineOptions.class).setOnSuccessMatcher(new TestFailureMatcher());

    when(mockJob.waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class)))
        .thenReturn(State.FAILED);

    expectedException.expect(RuntimeException.class);
    runner.run(p, mockRunner);
    // If the onSuccessMatcher were invoked, it would have crashed here with AssertionError
  }

  static class TestSuccessMatcher extends BaseMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {
    private final transient DataflowPipelineJob mockJob;
    private final int called;

    public TestSuccessMatcher(DataflowPipelineJob job, int times) {
      this.mockJob = job;
      this.called = times;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof PipelineResult)) {
        fail(String.format("Expected PipelineResult but received %s", o));
      }
      try {
        verify(mockJob, Mockito.times(called))
            .waitUntilFinish(any(Duration.class), any(JobMessagesHandler.class));
      } catch (IOException | InterruptedException e) {
        throw new AssertionError(e);
      }
      assertSame(mockJob, o);
      return true;
    }

    @Override
    public void describeTo(Description description) {}
  }

  static class TestFailureMatcher extends BaseMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {
    @Override
    public boolean matches(Object o) {
      fail("OnSuccessMatcher should not be called on pipeline failure.");
      return false;
    }

    @Override
    public void describeTo(Description description) {}
  }
}
