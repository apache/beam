/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil.JobState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Tests for BlockingDataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
public class BlockingDataflowPipelineRunnerTest {
  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(BlockingDataflowPipelineRunner.class);

  // This class mocks a call to DataflowPipelineJob.waitToFinish():
  //    it blocks the thread to simulate waiting,
  //    and releases the blocking once signaled
  static class MockWaitToFinish implements Answer<JobState> {
    NotificationHelper jobCompleted = new NotificationHelper();

    public JobState answer(InvocationOnMock invocation) throws InterruptedException {
      System.out.println("MockWaitToFinish.answer(): Wait for signaling job completion.");
      assertTrue("Test did not receive mock job completion signal",
          jobCompleted.waitTillSet(10000));

      System.out.println("MockWaitToFinish.answer(): job completed.");
      return JobState.DONE;
    }

    public void signalJobComplete() {
      jobCompleted.set();
    }
  }

  // Mini helper class for wait-notify
  static class NotificationHelper {
    private boolean isSet = false;

    public synchronized void set() {
      isSet = true;
      notifyAll();
    }

    public synchronized boolean check() {
      return isSet;
    }

    public synchronized boolean waitTillSet(long timeout) throws InterruptedException {
      long remainingTimeout = timeout;
      long startTime = new Date().getTime();
      while (!isSet && remainingTimeout > 0) {
        wait(remainingTimeout);
        remainingTimeout = timeout - (new Date().getTime() - startTime);
      }

      return isSet;
    }
  }

  @Test
  public void testJobWaitComplete() throws IOException, InterruptedException {
    expectedLogs.expectInfo("Job finished with status DONE");

    DataflowPipelineRunner mockDataflowPipelineRunner = mock(DataflowPipelineRunner.class);
    DataflowPipelineJob mockJob = mock(DataflowPipelineJob.class);
    MockWaitToFinish mockWait = new MockWaitToFinish();

    when(mockJob.waitToFinish(
        anyLong(), isA(TimeUnit.class), isA(MonitoringUtil.JobMessagesHandler.class)))
        .thenAnswer(mockWait);
    when(mockDataflowPipelineRunner.run(isA(Pipeline.class))).thenReturn(mockJob);

    // Construct a BlockingDataflowPipelineRunner with mockDataflowPipelineRunner inside
    final BlockingDataflowPipelineRunner blockingRunner =
        new BlockingDataflowPipelineRunner(
            mockDataflowPipelineRunner,
            new MonitoringUtil.PrintHandler(System.out));

    final NotificationHelper executionStarted = new NotificationHelper();
    final NotificationHelper jobCompleted = new NotificationHelper();

    new Thread() {
      public void run() {
        executionStarted.set();

        // Run on an empty test pipeline.
        blockingRunner.run(DirectPipeline.createForTest());

        // Test following code is not reached till mock job completion signal.
        jobCompleted.set();
      }
    }.start();

    assertTrue("'executionStarted' event not set till timeout.",
        executionStarted.waitTillSet(2000));
    assertFalse("Code after job completion should not be reached before mock signal.",
        jobCompleted.check());

    mockWait.signalJobComplete();
    assertTrue("run() should return after job completion is mocked.",
        jobCompleted.waitTillSet(2000));
  }
}
