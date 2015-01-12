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
import com.google.cloud.dataflow.sdk.testing.RestoreMappedDiagnosticContext;
import com.google.cloud.dataflow.sdk.testing.RestoreSystemProperties;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.slf4j.MDC;

import java.io.IOException;

/** Unit tests for {@link DataflowWorkerHarness}. */
@RunWith(JUnit4.class)
public class DataflowWorkerHarnessTest {
  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TestRule restoreMDC = new RestoreMappedDiagnosticContext();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Mock private MockHttpTransport transport;
  @Mock private MockLowLevelHttpRequest request;
  @Mock private DataflowWorker mockDataflowWorker;
  private DataflowWorkerHarnessOptions pipelineOptions;

  private Dataflow service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(transport.buildRequest(anyString(), anyString())).thenReturn(request);
    doCallRealMethod().when(request).getContentAsString();

    service = new Dataflow(transport, Transport.getJsonFactory(), null);
    pipelineOptions = PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
  }

  @Test
  public void testThatWeOnlyProcessWorkOncePerAvailableProcessor() throws Exception {
    int numWorkers = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    when(mockDataflowWorker.getAndPerformWork()).thenReturn(true);
    DataflowWorkerHarness.processWork(pipelineOptions, mockDataflowWorker);
    verify(mockDataflowWorker, times(numWorkers)).getAndPerformWork();
    verifyNoMoreInteractions(mockDataflowWorker);
  }

  @Test
  public void testThatWeOnlyProcessWorkOncePerAvailableProcessorEvenWhenFailing() throws Exception {
    int numWorkers = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
    when(mockDataflowWorker.getAndPerformWork()).thenReturn(false);
    DataflowWorkerHarness.processWork(pipelineOptions, mockDataflowWorker);
    verify(mockDataflowWorker, times(numWorkers)).getAndPerformWork();
    verifyNoMoreInteractions(mockDataflowWorker);
  }

  @Test
  public void testCreationOfWorkerHarness() throws Exception {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("project_id", "projectId")
        .put("job_id", "jobId")
        .put("worker_id", "workerId")
        .build());
    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();
    options.setGcpCredential(new TestCredential());
    assertNotNull(DataflowWorkerHarness.create(options));
    assertEquals("jobId", MDC.get("dataflow.jobId"));
    assertEquals("workerId", MDC.get("dataflow.workerId"));
  }

  @Test
  public void testCloudServiceCall() throws Exception {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("project_id", "projectId")
        .put("job_id", "jobId")
        .put("worker_id", "workerId")
        .build());
    WorkItem workItem = createWorkItem("projectId", "jobId");

    when(request.execute()).thenReturn(generateMockResponse(workItem));

    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, options);

    assertEquals(workItem, client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals("workerId", actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of("workerId", "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
    assertEquals("1234", MDC.get("dataflow.workId"));
  }

  @Test
  public void testCloudServiceCallNoWorkId() throws Exception {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("project_id", "projectId")
        .put("job_id", "jobId")
        .put("worker_id", "workerId")
        .build());

    // If there's no work the service should return an empty work item.
    WorkItem workItem = new WorkItem();

    when(request.execute()).thenReturn(generateMockResponse(workItem));

    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, options);

    assertNull(client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals("workerId", actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of("workerId", "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task",  "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallNoWorkItem() throws Exception {
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("project_id", "projectId")
        .put("job_id", "jobId")
        .put("worker_id", "workerId")
        .build());

    when(request.execute()).thenReturn(generateMockResponse());

    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, options);

    assertNull(client.getWorkItem());

    LeaseWorkItemRequest actualRequest = Transport.getJsonFactory().fromString(
        request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals("workerId", actualRequest.getWorkerId());
    assertEquals(ImmutableList.<String>of("workerId", "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(ImmutableList.<String>of("map_task", "seq_map_task",  "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallMultipleWorkItems() throws Exception {
    expectedException.expect(IOException.class);
    expectedException.expectMessage(
        "This version of the SDK expects no more than one work item from the service");
    System.getProperties().putAll(ImmutableMap
        .<String, String>builder()
        .put("project_id", "projectId")
        .put("job_id", "jobId")
        .put("worker_id", "workerId")
        .build());

    WorkItem workItem1 = createWorkItem("projectId", "jobId");
    WorkItem workItem2 = createWorkItem("projectId", "jobId");

    when(request.execute()).thenReturn(generateMockResponse(workItem1, workItem2));

    DataflowWorkerHarnessOptions options = PipelineOptionsFactory.createFromSystemProperties();

    DataflowWorker.WorkUnitClient client =
        new DataflowWorkerHarness.DataflowWorkUnitClient(service, options);

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
