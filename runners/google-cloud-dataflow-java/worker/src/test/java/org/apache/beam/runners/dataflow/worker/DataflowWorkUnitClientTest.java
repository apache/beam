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

import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LeaseWorkItemRequest;
import com.google.api.services.dataflow.model.LeaseWorkItemResponse;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MetricValue;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import com.google.api.services.dataflow.model.PerWorkerMetrics;
import com.google.api.services.dataflow.model.SendWorkerMessagesRequest;
import com.google.api.services.dataflow.model.SendWorkerMessagesResponse;
import com.google.api.services.dataflow.model.SeqMapTask;
import com.google.api.services.dataflow.model.StreamingScalingReport;
import com.google.api.services.dataflow.model.StreamingScalingReportResponse;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkerMessage;
import com.google.api.services.dataflow.model.WorkerMessageResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.testing.RestoreDataflowLoggingMDC;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.RestoreSystemProperties;
import org.apache.beam.sdk.util.FastNanoClockAndSleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for {@link DataflowWorkUnitClient}. */
@RunWith(JUnit4.class)
public class DataflowWorkUnitClientTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorkUnitClientTest.class);
  private static final String PROJECT_ID = "TEST_PROJECT_ID";
  private static final String JOB_ID = "TEST_JOB_ID";
  private static final String WORKER_ID = "TEST_WORKER_ID";

  @Rule public TestRule restoreSystemProperties = new RestoreSystemProperties();
  @Rule public TestRule restoreLogging = new RestoreDataflowLoggingMDC();
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();

  DataflowWorkerHarnessOptions createPipelineOptionsWithTransport(MockHttpTransport transport) {
    Dataflow service = new Dataflow(transport, Transport.getJsonFactory(), null);
    DataflowWorkerHarnessOptions pipelineOptions =
        PipelineOptionsFactory.as(DataflowWorkerHarnessOptions.class);
    pipelineOptions.setProject(PROJECT_ID);
    pipelineOptions.setJobId(JOB_ID);
    pipelineOptions.setWorkerId(WORKER_ID);
    pipelineOptions.setGcpCredential(new TestCredential());
    pipelineOptions.setDataflowClient(service);
    pipelineOptions.setRegion("us-central1");
    return pipelineOptions;
  }

  @Test
  public void testCloudServiceCall() throws Exception {
    WorkItem workItem = createWorkItem(PROJECT_ID, JOB_ID);

    MockLowLevelHttpResponse response = generateMockResponse(workItem);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.of(workItem), client.getWorkItem());

    LeaseWorkItemRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(
        ImmutableList.of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(
        ImmutableList.of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
    assertEquals("1234", DataflowWorkerLoggingMDC.getWorkId());
  }

  @Test
  public void testCloudServiceCallMapTaskStagePropagation() throws Exception {
    // Publish and acquire a map task work item, and verify we're now processing that stage.
    final String stageName = "test_stage_name";
    MapTask mapTask = new MapTask();
    mapTask.setStageName(stageName);
    WorkItem workItem = createWorkItem(PROJECT_ID, JOB_ID);
    workItem.setMapTask(mapTask);

    MockLowLevelHttpResponse response = generateMockResponse(workItem);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.of(workItem), client.getWorkItem());
    assertEquals(stageName, DataflowWorkerLoggingMDC.getStageName());
  }

  @Test
  public void testCloudServiceCallSeqMapTaskStagePropagation() throws Exception {
    // Publish and acquire a seq map task work item, and verify we're now processing that stage.
    final String stageName = "test_stage_name";
    SeqMapTask seqMapTask = new SeqMapTask();
    seqMapTask.setStageName(stageName);
    WorkItem workItem = createWorkItem(PROJECT_ID, JOB_ID);
    workItem.setSeqMapTask(seqMapTask);

    MockLowLevelHttpResponse response = generateMockResponse(workItem);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.of(workItem), client.getWorkItem());
    assertEquals(stageName, DataflowWorkerLoggingMDC.getStageName());
  }

  @Test
  public void testCloudServiceCallNoWorkPresent() throws Exception {
    // If there's no work the service should return an empty work item.
    WorkItem workItem = new WorkItem();

    MockLowLevelHttpResponse response = generateMockResponse(workItem);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.empty(), client.getWorkItem());

    LeaseWorkItemRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(
        ImmutableList.of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(
        ImmutableList.of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallNoWorkId() throws Exception {
    // If there's no work the service should return an empty work item.
    WorkItem workItem = createWorkItem(PROJECT_ID, JOB_ID);
    workItem.setId(null);

    MockLowLevelHttpResponse response = generateMockResponse(workItem);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.empty(), client.getWorkItem());

    LeaseWorkItemRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(
        ImmutableList.of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(
        ImmutableList.of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallNoWorkItem() throws Exception {
    MockLowLevelHttpResponse response = generateMockResponse();
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    assertEquals(Optional.empty(), client.getWorkItem());

    LeaseWorkItemRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), LeaseWorkItemRequest.class);
    assertEquals(WORKER_ID, actualRequest.getWorkerId());
    assertEquals(
        ImmutableList.of(WORKER_ID, "remote_source", "custom_source"),
        actualRequest.getWorkerCapabilities());
    assertEquals(
        ImmutableList.of("map_task", "seq_map_task", "remote_source_task"),
        actualRequest.getWorkItemTypes());
  }

  @Test
  public void testCloudServiceCallMultipleWorkItems() throws Exception {
    expectedException.expect(IOException.class);
    expectedException.expectMessage(
        "This version of the SDK expects no more than one work item from the service");

    WorkItem workItem1 = createWorkItem(PROJECT_ID, JOB_ID);
    WorkItem workItem2 = createWorkItem(PROJECT_ID, JOB_ID);

    MockLowLevelHttpResponse response = generateMockResponse(workItem1, workItem2);
    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    client.getWorkItem();
  }

  @Test
  public void testReportWorkerMessage_streamingScalingReport() throws Exception {
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContentType(Json.MEDIA_TYPE);
    SendWorkerMessagesResponse workerMessage = new SendWorkerMessagesResponse();
    StreamingScalingReportResponse streamingScalingReportResponse =
        new StreamingScalingReportResponse().setMaximumThreadCount(10);
    WorkerMessageResponse workerMessageResponse =
        new WorkerMessageResponse()
            .setStreamingScalingReportResponse(streamingScalingReportResponse);
    workerMessage.setWorkerMessageResponses(Collections.singletonList(workerMessageResponse));
    workerMessage.setFactory(Transport.getJsonFactory());
    response.setContent(workerMessage.toPrettyString());

    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    StreamingScalingReport activeThreadsReport =
        new StreamingScalingReport()
            .setActiveThreadCount(1)
            .setActiveBundleCount(2)
            .setOutstandingBytes(3L)
            .setMaximumThreadCount(4)
            .setMaximumBundleCount(5)
            .setMaximumBytes(6L);
    WorkerMessage msg = client.createWorkerMessageFromStreamingScalingReport(activeThreadsReport);
    List<WorkerMessageResponse> responses =
        client.reportWorkerMessage(Collections.singletonList(msg));

    SendWorkerMessagesRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), SendWorkerMessagesRequest.class);
    assertEquals(ImmutableList.of(msg), actualRequest.getWorkerMessages());
    assertEquals(ImmutableList.of(workerMessageResponse), responses);
  }

  @Test
  public void testReportWorkerMessage_perWorkerMetrics() throws Exception {
    MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
    response.setContentType(Json.MEDIA_TYPE);
    SendWorkerMessagesResponse workerMessage = new SendWorkerMessagesResponse();
    workerMessage.setFactory(Transport.getJsonFactory());
    response.setContent(workerMessage.toPrettyString());

    MockLowLevelHttpRequest request = new MockLowLevelHttpRequest().setResponse(response);
    MockHttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    DataflowWorkerHarnessOptions pipelineOptions = createPipelineOptionsWithTransport(transport);
    WorkUnitClient client = new DataflowWorkUnitClient(pipelineOptions, LOG);

    PerStepNamespaceMetrics stepNamespaceMetrics =
        new PerStepNamespaceMetrics()
            .setOriginalStep("s1")
            .setMetricsNamespace("ns")
            .setMetricValues(
                Collections.singletonList(new MetricValue().setMetric("metric").setValueInt64(3L)));
    PerWorkerMetrics perWorkerMetrics =
        new PerWorkerMetrics()
            .setPerStepNamespaceMetrics(Collections.singletonList(stepNamespaceMetrics));

    WorkerMessage perWorkerMetricsMsg =
        client.createWorkerMessageFromPerWorkerMetrics(perWorkerMetrics);
    client.reportWorkerMessage(Collections.singletonList(perWorkerMetricsMsg));

    SendWorkerMessagesRequest actualRequest =
        Transport.getJsonFactory()
            .fromString(request.getContentAsString(), SendWorkerMessagesRequest.class);
    assertEquals(ImmutableList.of(perWorkerMetricsMsg), actualRequest.getWorkerMessages());
  }

  private MockLowLevelHttpResponse generateMockResponse(WorkItem... workItems) throws Exception {
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
