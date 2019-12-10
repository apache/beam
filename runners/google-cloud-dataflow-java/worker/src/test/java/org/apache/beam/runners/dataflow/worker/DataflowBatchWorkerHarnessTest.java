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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.services.dataflow.Dataflow;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.util.FastNanoClockAndSleeper;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link DataflowBatchWorkerHarness}. */
@RunWith(JUnit4.class)
public class DataflowBatchWorkerHarnessTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TestRule restoreLogging = new RestoreDataflowLoggingMDC();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
  @Mock private MockHttpTransport transport;
  @Mock private MockLowLevelHttpRequest request;
  @Mock private WorkUnitClient mockWorkUnitClient;
  private DataflowWorkerHarnessOptions pipelineOptions;

  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final String WORKER_ID = "TEST_WORKER_ID";

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(transport.buildRequest(anyString(), anyString())).thenReturn(request);
    doCallRealMethod().when(request).getContentAsString();

    Dataflow service = new Dataflow(transport, Transport.getJsonFactory(), null);
    pipelineOptions = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());
    pipelineOptions.setDataflowClient(service);
  }

  // This test uses a FakeWorker rather than a mock DataflowWorker since mockito mocks are not
  // thread safe (see https://github.com/mockito/mockito/wiki/FAQ#is-mockito-thread-safe).
  public void runTestThatWeRetryIfTaskExecutionFailsAgainAndAgain(FakeWorker worker)
      throws Exception {
    final int numWorkers = Math.max(Runtime.getRuntime().availableProcessors(), 1);
    final AtomicInteger sleepCount = new AtomicInteger(0);
    final AtomicInteger illegalIntervalCount = new AtomicInteger(0);
    DataflowBatchWorkerHarness.processWork(
        pipelineOptions,
        worker,
        millis -> {
          if ((millis > DataflowBatchWorkerHarness.BACKOFF_MAX_INTERVAL_MILLIS * 1.5)) {
            // We count the times the sleep interval is greater than the backoff max interval with
            // randomization to make sure it does not happen.
            illegalIntervalCount.incrementAndGet();
          }
          if (sleepCount.incrementAndGet() > 1000) {
            throw new InterruptedException("Stopping the retry loop.");
          }
        });
    // Test that the backoff mechanism will allow at least 1000 failures.
    assertEquals(numWorkers + 1000, worker.getNumberOfCallsToGetAndPerformWork());
    assertEquals(0, illegalIntervalCount.get());
  }

  @Test
  public void testThatWeRetryIfTaskExecutionFailAgainAndAgain() throws Exception {
    FakeWorker fakeWorker = new FakeWorker(pipelineOptions, false);
    runTestThatWeRetryIfTaskExecutionFailsAgainAndAgain(fakeWorker);
  }

  @Test
  public void testThatWeRetryIfTaskExecutionFailAgainAndAgainByIOException() throws Exception {
    FakeWorker fakeWorker = new FakeWorker(pipelineOptions, new IOException());
    runTestThatWeRetryIfTaskExecutionFailsAgainAndAgain(fakeWorker);
  }

  @Test
  public void testThatWeRetryIfTaskExecutionFailAgainAndAgainByUnknownException() throws Exception {
    FakeWorker fakeWorker = new FakeWorker(pipelineOptions, new RuntimeException());
    runTestThatWeRetryIfTaskExecutionFailsAgainAndAgain(fakeWorker);
  }

  @Test
  public void testNumberOfWorkerHarnessThreadsIsHonored() throws Exception {
    final int expectedNumberOfThreads = 5;
    pipelineOptions.setNumberOfWorkerHarnessThreads(expectedNumberOfThreads);

    FakeWorker fakeWorker = new FakeWorker(pipelineOptions, false);

    DataflowBatchWorkerHarness.processWork(
        pipelineOptions,
        fakeWorker,
        millis -> {
          throw new InterruptedException("Stopping the retry loop.");
        });
    // Verify that the number of requested worker harness threads is honored.
    assertEquals(expectedNumberOfThreads, fakeWorker.getNumberOfCallsToGetAndPerformWork());
  }

  /**
   * A fake worker implementation to replace non-thread safe Mockito mocks of the DataflowWorker in
   * multi-threaded tests.
   */
  private class FakeWorker extends BatchDataflowWorker {

    private final IOException ioExceptionValue;
    private final RuntimeException runtimeExceptionValue;
    private final boolean returnValue;
    private AtomicInteger count = new AtomicInteger(0);

    public FakeWorker(DataflowWorkerHarnessOptions options, boolean returnValue) {
      super(
          null /* pipeline */,
          SdkHarnessRegistries.emptySdkHarnessRegistry(),
          mockWorkUnitClient,
          IntrinsicMapTaskExecutorFactory.defaultFactory(),
          options);
      ioExceptionValue = null;
      runtimeExceptionValue = null;
      this.returnValue = returnValue;
    }

    public FakeWorker(DataflowWorkerHarnessOptions options, IOException e) {
      super(
          null /* pipeline */,
          SdkHarnessRegistries.emptySdkHarnessRegistry(),
          mockWorkUnitClient,
          IntrinsicMapTaskExecutorFactory.defaultFactory(),
          options);
      ioExceptionValue = e;
      runtimeExceptionValue = null;
      this.returnValue = false;
    }

    public FakeWorker(DataflowWorkerHarnessOptions options, RuntimeException e) {
      super(
          null /* pipeline */,
          SdkHarnessRegistries.emptySdkHarnessRegistry(),
          mockWorkUnitClient,
          IntrinsicMapTaskExecutorFactory.defaultFactory(),
          options);
      ioExceptionValue = null;
      runtimeExceptionValue = e;
      this.returnValue = false;
    }

    @Override
    public boolean getAndPerformWork() throws IOException {
      count.incrementAndGet();
      if (ioExceptionValue != null) {
        throw ioExceptionValue;
      }
      if (runtimeExceptionValue != null) {
        throw runtimeExceptionValue;
      }
      return returnValue;
    }

    public int getNumberOfCallsToGetAndPerformWork() {
      return count.get();
    }
  }
}
