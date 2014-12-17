/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link DataflowWorker}. */
@RunWith(JUnit4.class)
public class DataflowWorkerTest {
  @Rule
  public FastNanoClockAndSleeper clockAndSleeper = new FastNanoClockAndSleeper();

  @Mock
  DataflowWorker.WorkUnitClient mockWorkUnitClient;

  @Mock
  DataflowWorkerHarnessOptions options;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testWhenNoWorkThatWeReturnFalse() throws Exception {
    DataflowWorker worker = new DataflowWorker(mockWorkUnitClient, options);
    when(mockWorkUnitClient.getWorkItem()).thenReturn(null);

    assertFalse(worker.getAndPerformWork());
  }

  @Test
  public void testWhenProcessingWorkUnitFailsWeReportStatus() throws Exception {
    DataflowWorker worker = new DataflowWorker(mockWorkUnitClient, options);
    when(mockWorkUnitClient.getWorkItem()).thenReturn(
        new WorkItem().setId(1L).setJobId("Expected to fail the job")).thenReturn(null);

    assertFalse(worker.getAndPerformWork());
    verify(mockWorkUnitClient).reportWorkItemStatus(argThat(cloudWorkHasErrors()));
  }

  private Matcher<WorkItemStatus> cloudWorkHasErrors() {
    return new TypeSafeMatcher<WorkItemStatus>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("WorkItemStatus expected to have errors");
      }

      @Override
      protected boolean matchesSafely(WorkItemStatus status) {
        return status.getCompleted() && !status.getErrors().isEmpty();
      }
    };
  }
}
