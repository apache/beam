/*
 * Copyright (C) 2015 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LeaseWorkItemRequest;
import com.google.api.services.dataflow.model.LeaseWorkItemResponse;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingFormatter;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
import com.google.cloud.dataflow.sdk.testing.RestoreDataflowLoggingFormatter;
import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

/** Unit tests for {@link DataflowWorkerHarness}. */
@RunWith(JUnit4.class)
public class DataflowWorkerHarnessTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TestRule restoreLogging = new RestoreDataflowLoggingFormatter();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
  @Mock private MockHttpTransport transport;
  @Mock private MockLowLevelHttpRequest request;
  @Mock private DataflowWorker mockDataflowWorker;
  private DataflowWorkerHarnessOptions pipelineOptions;

  private Dataflow service;

  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final String WORKER_ID = "TEST_WORKER_ID";

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(transport.buildRequest(anyString(), anyString())).thenReturn(request);
    doCallRealMethod().when(request).getContentAsString();

    service = new Dataflow(transport, Transport.getJsonFactory(), null);
    pipelineOptions = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
  }

  @Test
  public void testThatWeRetryIfTaskExecutionFailAgainAndAgain() throws Exception {
    int numWorkers = Math.max(Runtime.getRuntime().availableProcessors(), 1);
    when(mockDataflowWorker.getAndPerformWork()).thenReturn(false);
    DataflowWorkerHarness.processWork(
            pipelineOptions, mockDataflowWorker, fastNanoClockAndSleeper);
    // Test that the backoff mechanism will retry the BACKOFF_MAX_ATTEMPTS number of times.
    verify(mockDataflowWorker, times(numWorkers * DataflowWorkerHarness.BACKOFF_MAX_ATTEMPTS))
        .getAndPerformWork();
    verifyNoMoreInteractions(mockDataflowWorker);
  }

  @Test
  public void testCreationOfWorkerHarness() throws Exception {
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());

    assertNotNull(DataflowWorkerHarness.create(pipelineOptions));
    assertEquals(JOB_ID, DataflowWorkerLoggingFormatter.getJobId());
    assertEquals(WORKER_ID, DataflowWorkerLoggingFormatter.getWorkerId());
  }

  @Test
  public void testCloudServiceCall() throws Exception {
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());

    WorkItem workItem = createWorkItem(PROJECT_ID, JOB_ID);

    when(request.execute()).thenReturn(generateMockResponse(workItem));

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, pipelineOptions);

    assertEquals(workItem, client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
    assertEquals("1234", DataflowWorkerLoggingFormatter.getWorkId());
  }

  @Test
  public void testCloudServiceCallNoWorkId() throws Exception {
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());

    // If there's no work the service should return an empty work item.
    WorkItem workItem = new WorkItem();

    when(request.execute()).thenReturn(generateMockResponse(workItem));

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, pipelineOptions);

    assertNull(client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task",  "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallNoWorkItem() throws Exception {
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());

    when(request.execute()).thenReturn(generateMockResponse());

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, pipelineOptions);

    assertNull(client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task",  "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallMultipleWorkItems() throws Exception {
    expectedException.expect(IOException.class);
    expectedException.expectMessage(
        "This version of the SDK expects no more than one work item from the service");
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());


    WorkItem workItem1 = createWorkItem(PROJECT_ID, JOB_ID);
    WorkItem workItem2 = createWorkItem(PROJECT_ID, JOB_ID);

    when(request.execute()).thenReturn(generateMockResponse(workItem1, workItem2));

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, pipelineOptions);

    client.getWorkItem();
  }

  private LowLevelHttpResponse generateMockResponse(WorkItem ... workItems) throws Exception {
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContentType(Json.MEDIA_TYPE);
    LeaseWorkItemResponse lease = new LeaseWorkItemResponse();
    lease.setWorkItems(Lists.newArrayList(workItems));
    // N.B. Setting the factory is necessary in order to get valid JSON.
    lease.setFactory(Transport.getJsonFactory());
    response.setContent(lease.toPrettyString());
    return response;
  }

  private WorkItem createWorkItem(String projectId, String jobId) {
    WorkItem workItem = new WorkItem();
    workItem.setFactory(Transport.getJsonFactory());
    workItem.setProjectId(projectId);
    workItem.setJobId(jobId);

    // We need to set a work id because otherwise the client will treat the response as
    // indicating no work is available.
    workItem.setId(1234L);
    return workItem;
  }
}
