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
package org.apache.beam.runners.dataflow.testing;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.JobMessagesHandler;
import org.apache.beam.runners.dataflow.util.TimeUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.NoopPathValidator;
import org.apache.beam.sdk.util.TestCredential;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Tests for {@link TestDataflowRunner}. */
@RunWith(JUnit4.class)
public class TestDataflowRunnerTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Mock private MockHttpTransport transport;
  @Mock private MockLowLevelHttpRequest request;
  @Mock private GcsUtil mockGcsUtil;

  private TestDataflowPipelineOptions options;
  private Dataflow service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(transport.buildRequest(anyString(), anyString())).thenReturn(request);
    doCallRealMethod().when(request).getContentAsString();
    service = new Dataflow(transport, Transport.getJsonFactory(), null);

    options = PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setAppName("TestAppName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setTempRoot("gs://test");
    options.setGcpCredential(new TestCredential());
    options.setDataflowClient(service);
    options.setRunner(TestDataflowRunner.class);
    options.setPathValidatorClass(NoopPathValidator.class);
  }

  @Test
  public void testToString() {
    assertEquals("TestDataflowRunner#TestAppName",
        new TestDataflowRunner(options).toString());
  }

  @Test
  public void testRunBatchJobThatSucceeds() throws Exception {
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.DONE);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    assertEquals(mockJob, runner.run(p, mockRunner));
  }

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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      return;
    }
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
    when(mockJob.waitToFinish(any(Long.class), any(TimeUnit.class), any(JobMessagesHandler.class)))
        .thenAnswer(new Answer<State>() {
          @Override
          public State answer(InvocationOnMock invocation) {
            JobMessage message = new JobMessage();
            message.setMessageText("FooException");
            message.setTime(TimeUtil.toCloudTime(Instant.now()));
            message.setMessageImportance("JOB_MESSAGE_ERROR");
            ((MonitoringUtil.JobMessagesHandler) invocation.getArguments()[2])
                .process(Arrays.asList(message));
            return State.CANCELLED;
          }
        });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("FooException"));
      verify(mockJob, atLeastOnce()).cancel();
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testRunStreamingJobThatSucceeds() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    runner.run(p, mockRunner);
  }

  @Test
  public void testRunStreamingJobThatFails() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
  }

  @Test
  public void testCheckingForSuccessWhenPAssertSucceeds() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", service, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    doReturn(State.DONE).when(job).getState();
    assertEquals(Optional.of(true), runner.checkForSuccess(job));
  }

  @Test
  public void testCheckingForSuccessWhenPAssertFails() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", service, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    doReturn(State.DONE).when(job).getState();
    assertEquals(Optional.of(false), runner.checkForSuccess(job));
  }

  @Test
  public void testCheckingForSuccessSkipsNonTentativeMetrics() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", service, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, false /* tentative */));
    doReturn(State.RUNNING).when(job).getState();
    assertEquals(Optional.absent(), runner.checkForSuccess(job));
  }

  private LowLevelHttpResponse generateMockMetricResponse(boolean success, boolean tentative)
      throws Exception {
    MetricStructuredName name = new MetricStructuredName();
    name.setName(success ? "PAssertSuccess" : "PAssertFailure");
    name.setContext(
        tentative ? ImmutableMap.of("tentative", "") : ImmutableMap.<String, String>of());

    MetricUpdate metric = new MetricUpdate();
    metric.setName(name);
    metric.setScalar(BigDecimal.ONE);

    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContentType(Json.MEDIA_TYPE);
    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(Lists.newArrayList(metric));
    // N.B. Setting the factory is necessary in order to get valid JSON.
    jobMetrics.setFactory(Transport.getJsonFactory());
    response.setContent(jobMetrics.toPrettyString());
    return response;
  }

  @Test
  public void testStreamingPipelineFailsIfServiceFails() throws Exception {
    DataflowPipelineJob job =
        spy(new DataflowPipelineJob("test-project", "test-job", service, null));
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, false /* tentative */));
    doReturn(State.FAILED).when(job).getState();
    assertEquals(Optional.of(false), runner.checkForSuccess(job));
  }

  @Test
  public void testStreamingPipelineFailsIfException() throws Exception {
    options.setStreaming(true);
    Pipeline p = TestPipeline.create(options);
    PCollection<Integer> pc = p.apply(Create.of(1, 2, 3));
    PAssert.that(pc).containsInAnyOrder(1, 2, 3);

    DataflowPipelineJob mockJob = Mockito.mock(DataflowPipelineJob.class);
    when(mockJob.getState()).thenReturn(State.RUNNING);
    when(mockJob.getProjectId()).thenReturn("test-project");
    when(mockJob.getJobId()).thenReturn("test-job");
    when(mockJob.waitToFinish(any(Long.class), any(TimeUnit.class), any(JobMessagesHandler.class)))
        .thenAnswer(new Answer<State>() {
          @Override
          public State answer(InvocationOnMock invocation) {
            JobMessage message = new JobMessage();
            message.setMessageText("FooException");
            message.setTime(TimeUtil.toCloudTime(Instant.now()));
            message.setMessageImportance("JOB_MESSAGE_ERROR");
            ((MonitoringUtil.JobMessagesHandler) invocation.getArguments()[2])
                .process(Arrays.asList(message));
            return State.CANCELLED;
          }
        });

    DataflowRunner mockRunner = Mockito.mock(DataflowRunner.class);
    when(mockRunner.run(any(Pipeline.class))).thenReturn(mockJob);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      assertThat(expected.getMessage(), containsString("FooException"));
      verify(mockJob, atLeastOnce()).cancel();
      return;
    }
    // Note that fail throws an AssertionError which is why it is placed out here
    // instead of inside the try-catch block.
    fail("AssertionError expected");
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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnCreateMatcher(new TestSuccessMatcher(mockJob, 0));

    when(mockJob.waitToFinish(any(Long.class), any(TimeUnit.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatcher(new TestSuccessMatcher(mockJob, 1));

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
    runner.run(p, mockRunner);
  }

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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatcher(new TestSuccessMatcher(mockJob, 1));

    when(mockJob.waitToFinish(any(Long.class), any(TimeUnit.class), any(JobMessagesHandler.class)))
        .thenReturn(State.DONE);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(true /* success */, true /* tentative */));
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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatcher(new TestFailureMatcher());

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      verify(mockJob, Mockito.times(1)).waitToFinish(any(Long.class), any(TimeUnit.class),
          any(JobMessagesHandler.class));
      return;
    }
    fail("Expected an exception on pipeline failure.");
  }

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

    TestDataflowRunner runner = (TestDataflowRunner) p.getRunner();
    p.getOptions().as(TestPipelineOptions.class)
        .setOnSuccessMatcher(new TestFailureMatcher());

    when(mockJob.waitToFinish(any(Long.class), any(TimeUnit.class), any(JobMessagesHandler.class)))
        .thenReturn(State.FAILED);

    when(request.execute()).thenReturn(
        generateMockMetricResponse(false /* success */, true /* tentative */));
    try {
      runner.run(p, mockRunner);
    } catch (AssertionError expected) {
      verify(mockJob, Mockito.times(1)).waitToFinish(any(Long.class), any(TimeUnit.class),
          any(JobMessagesHandler.class));
      return;
    }
    fail("Expected an exception on pipeline failure.");
  }

  static class TestSuccessMatcher extends BaseMatcher<PipelineResult> implements
      SerializableMatcher<PipelineResult> {
    private final DataflowPipelineJob mockJob;
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
        verify(mockJob, Mockito.times(called)).waitToFinish(any(Long.class), any(TimeUnit.class),
            any(JobMessagesHandler.class));
      } catch (IOException | InterruptedException e) {
        throw new AssertionError(e);
      }
      assertSame(mockJob, o);
      return true;
    }

    @Override
    public void describeTo(Description description) {
    }
  }

  static class TestFailureMatcher extends BaseMatcher<PipelineResult> implements
      SerializableMatcher<PipelineResult> {
    @Override
    public boolean matches(Object o) {
      fail("OnSuccessMatcher should not be called on pipeline failure.");
      return false;
    }

    @Override
    public void describeTo(Description description) {
    }
  }
}
