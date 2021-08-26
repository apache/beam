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
package org.apache.beam.runners.dataflow.worker.status;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.GetDebugConfigRequest;
import com.google.api.services.dataflow.model.GetDebugConfigResponse;
import com.google.api.services.dataflow.model.SendDebugCaptureRequest;
import com.google.api.services.dataflow.model.SendDebugCaptureResponse;
import java.util.Collections;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DebugCapture}. */
@RunWith(JUnit4.class)
public class DebugCaptureTest {
  private static final String PROJECT_ID = "some-project";
  private static final String REGION = "some-region";
  private static final String JOB_ID = "some-job";
  private static final String WORKER_ID = "some-host-abcd";
  private static final String UPDATE_CONFIG_JSON =
      "{\"expire_timestamp_usec\":11,\"capture_frequency_usec\":12}";
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();

  private Dataflow.Projects.Locations.Jobs.Debug.SendCapture mockSendCapture;
  private GetDebugConfigResponse fakeGetConfigResponse;

  private Dataflow buildMockDataflow() throws Exception {
    Dataflow mockDataflowClient = mock(Dataflow.class);
    Dataflow.Projects mockProjects = mock(Dataflow.Projects.class);
    Dataflow.Projects.Locations mockRegion = mock(Dataflow.Projects.Locations.class);
    Dataflow.Projects.Locations.Jobs mockJobs = mock(Dataflow.Projects.Locations.Jobs.class);
    Dataflow.Projects.Locations.Jobs.Debug mockDebug =
        mock(Dataflow.Projects.Locations.Jobs.Debug.class);
    Dataflow.Projects.Locations.Jobs.Debug.GetConfig mockGetConfig =
        mock(Dataflow.Projects.Locations.Jobs.Debug.GetConfig.class);
    when(mockDataflowClient.projects()).thenReturn(mockProjects);
    when(mockProjects.locations()).thenReturn(mockRegion);
    when(mockRegion.jobs()).thenReturn(mockJobs);
    when(mockJobs.debug()).thenReturn(mockDebug);
    when(mockDebug.getConfig(
            eq(PROJECT_ID), eq(REGION), eq(JOB_ID), isA(GetDebugConfigRequest.class)))
        .thenReturn(mockGetConfig);
    when(mockDebug.sendCapture(
            eq(PROJECT_ID), eq(REGION), eq(JOB_ID), isA(SendDebugCaptureRequest.class)))
        .thenReturn(mockSendCapture);

    when(mockGetConfig.execute()).thenReturn(fakeGetConfigResponse);

    return mockDataflowClient;
  }

  private StreamingDataflowWorkerOptions buildDataflowWorkerOptions() throws Exception {
    StreamingDataflowWorkerOptions options =
        PipelineOptionsFactory.as(StreamingDataflowWorkerOptions.class);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION);
    options.setJobId(JOB_ID);
    options.setWorkerId(WORKER_ID);
    options.setDataflowClient(buildMockDataflow());
    return options;
  }

  @Before
  public void setUp() throws Exception {
    fakeGetConfigResponse = new GetDebugConfigResponse();
    fakeGetConfigResponse.setConfig(UPDATE_CONFIG_JSON);

    mockSendCapture = mock(Dataflow.Projects.Locations.Jobs.Debug.SendCapture.class);
    when(mockSendCapture.execute()).thenReturn(new SendDebugCaptureResponse());
  }

  @Test
  public void testUpdateConfig() throws Exception {
    DebugCapture.Manager debugCaptureManager =
        new DebugCapture.Manager(buildDataflowWorkerOptions(), Collections.emptyList());
    assertEquals(0, debugCaptureManager.captureConfig.expireTimestampUsec);
    assertEquals(0, debugCaptureManager.captureConfig.captureFrequencyUsec);
    debugCaptureManager.updateConfig();
    assertEquals(11, debugCaptureManager.captureConfig.expireTimestampUsec);
    assertEquals(12, debugCaptureManager.captureConfig.captureFrequencyUsec);
  }

  @Test
  public void testSendCapture() throws Exception {
    DebugCapture.Manager debugCaptureManager =
        new DebugCapture.Manager(buildDataflowWorkerOptions(), Collections.emptyList());
    DebugCapture.Config config = debugCaptureManager.captureConfig;

    // If the config is disabled, regardless it is expired or not, we don't send capture.
    config.captureFrequencyUsec = 0; // disabled
    config.expireTimestampUsec = Long.MAX_VALUE; // never expired
    debugCaptureManager.maybeSendCapture();
    verify(mockSendCapture, never()).execute();

    // If the config is enabled but expired, then we don't send capture.
    config.captureFrequencyUsec = 1000;
    config.expireTimestampUsec = 0; // expired
    debugCaptureManager.maybeSendCapture();
    verify(mockSendCapture, never()).execute();

    // If the config is enabled and not expired, but it is not the time to send capture, then we
    // don't send capture.
    config.captureFrequencyUsec = Long.MAX_VALUE; // never send capture
    config.expireTimestampUsec = Long.MAX_VALUE; // never expired
    debugCaptureManager.maybeSendCapture();
    verify(mockSendCapture, never()).execute();

    // Otherwise, we will send capture.
    config.captureFrequencyUsec = 1000;
    config.expireTimestampUsec = Long.MAX_VALUE; // never expired
    debugCaptureManager.maybeSendCapture();
    verify(mockSendCapture).execute();
  }
}
