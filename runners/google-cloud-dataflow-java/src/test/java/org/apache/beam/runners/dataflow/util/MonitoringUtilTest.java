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
package org.apache.beam.runners.dataflow.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowClient;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil.LoggingHandler;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.chrono.ISOChronology;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for MonitoringUtil. */
@RunWith(JUnit4.class)
public class MonitoringUtilTest {
  private static final String PROJECT_ID = "someProject";
  private static final String REGION_ID = "thatRegion";
  private static final String JOB_ID = "1234";

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(LoggingHandler.class);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetJobMessages() throws IOException {
    DataflowClient dataflowClient = mock(DataflowClient.class);

    ListJobMessagesResponse firstResponse = new ListJobMessagesResponse();
    firstResponse.setJobMessages(new ArrayList<>());
    for (long i = 0; i < 100; ++i) {
      JobMessage message = new JobMessage();
      message.setId("message_" + i);
      message.setTime(TimeUtil.toCloudTime(new Instant(i)));
      firstResponse.getJobMessages().add(message);
    }
    String pageToken = "page_token";
    firstResponse.setNextPageToken(pageToken);

    ListJobMessagesResponse secondResponse = new ListJobMessagesResponse();
    secondResponse.setJobMessages(new ArrayList<>());
    for (long i = 100; i < 150; ++i) {
      JobMessage message = new JobMessage();
      message.setId("message_" + i);
      message.setTime(TimeUtil.toCloudTime(new Instant(i)));
      secondResponse.getJobMessages().add(message);
    }

    when(dataflowClient.listJobMessages(JOB_ID, null)).thenReturn(firstResponse);
    when(dataflowClient.listJobMessages(JOB_ID, pageToken)).thenReturn(secondResponse);

    MonitoringUtil util = new MonitoringUtil(dataflowClient);

    List<JobMessage> messages = util.getJobMessages(JOB_ID, -1);

    assertEquals(150, messages.size());
  }

  @Test
  public void testToStateNormal() {
    // Trivially mapped cases
    assertEquals(State.UNKNOWN, MonitoringUtil.toState("JOB_STATE_UNKNOWN"));
    assertEquals(State.STOPPED, MonitoringUtil.toState("JOB_STATE_STOPPED"));
    assertEquals(State.RUNNING, MonitoringUtil.toState("JOB_STATE_RUNNING"));
    assertEquals(State.DONE, MonitoringUtil.toState("JOB_STATE_DONE"));
    assertEquals(State.FAILED, MonitoringUtil.toState("JOB_STATE_FAILED"));
    assertEquals(State.CANCELLED, MonitoringUtil.toState("JOB_STATE_CANCELLED"));
    assertEquals(State.UPDATED, MonitoringUtil.toState("JOB_STATE_UPDATED"));

    // Non-trivially mapped cases
    assertEquals(State.RUNNING, MonitoringUtil.toState("JOB_STATE_DRAINING"));
    assertEquals(State.DONE, MonitoringUtil.toState("JOB_STATE_DRAINED"));
  }

  @Test
  public void testToStateWithNullReturnsUnrecognized() {
    assertEquals(State.UNRECOGNIZED, MonitoringUtil.toState(null));
  }

  @Test
  public void testToStateWithOtherValueReturnsUnknown() {
    assertEquals(State.UNRECOGNIZED, MonitoringUtil.toState("FOO_BAR_BAZ"));
  }

  @Test
  public void testDontOverrideEndpointWithDefaultApi() {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION_ID);
    options.setGcpCredential(new TestCredential());
    String cancelCommand = MonitoringUtil.getGcloudCancelCommand(options, JOB_ID);
    assertEquals(
        "gcloud dataflow jobs --project=someProject cancel --region=thatRegion 1234",
        cancelCommand);
  }

  @Test
  public void testOverridesEndpointWithStagedDataflowEndpoint() {
    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION_ID);
    options.setGcpCredential(new TestCredential());
    String stagingDataflowEndpoint = "v0neverExisted";
    options.setDataflowEndpoint(stagingDataflowEndpoint);
    String cancelCommand = MonitoringUtil.getGcloudCancelCommand(options, JOB_ID);
    assertEquals(
        "CLOUDSDK_API_ENDPOINT_OVERRIDES_DATAFLOW=https://dataflow.googleapis.com/v0neverExisted/ "
            + "gcloud dataflow jobs --project=someProject cancel --region=thatRegion 1234",
        cancelCommand);
  }

  @Test
  public void testLoggingHandler() {
    DateTime errorTime = new DateTime(1000L, ISOChronology.getInstanceUTC());
    DateTime warningTime = new DateTime(2000L, ISOChronology.getInstanceUTC());
    DateTime basicTime = new DateTime(3000L, ISOChronology.getInstanceUTC());
    DateTime detailedTime = new DateTime(4000L, ISOChronology.getInstanceUTC());
    DateTime debugTime = new DateTime(5000L, ISOChronology.getInstanceUTC());
    DateTime unknownTime = new DateTime(6000L, ISOChronology.getInstanceUTC());
    JobMessage errorJobMessage = new JobMessage();
    errorJobMessage.setMessageImportance("JOB_MESSAGE_ERROR");
    errorJobMessage.setMessageText("ERRORERROR");
    errorJobMessage.setTime(TimeUtil.toCloudTime(errorTime));
    JobMessage warningJobMessage = new JobMessage();
    warningJobMessage.setMessageImportance("JOB_MESSAGE_WARNING");
    warningJobMessage.setMessageText("WARNINGWARNING");
    warningJobMessage.setTime(TimeUtil.toCloudTime(warningTime));
    JobMessage basicJobMessage = new JobMessage();
    basicJobMessage.setMessageImportance("JOB_MESSAGE_BASIC");
    basicJobMessage.setMessageText("BASICBASIC");
    basicJobMessage.setTime(TimeUtil.toCloudTime(basicTime));
    JobMessage detailedJobMessage = new JobMessage();
    detailedJobMessage.setMessageImportance("JOB_MESSAGE_DETAILED");
    detailedJobMessage.setMessageText("DETAILEDDETAILED");
    detailedJobMessage.setTime(TimeUtil.toCloudTime(detailedTime));
    JobMessage debugJobMessage = new JobMessage();
    debugJobMessage.setMessageImportance("JOB_MESSAGE_DEBUG");
    debugJobMessage.setMessageText("DEBUGDEBUG");
    debugJobMessage.setTime(TimeUtil.toCloudTime(debugTime));
    JobMessage unknownJobMessage = new JobMessage();
    unknownJobMessage.setMessageImportance("JOB_MESSAGE_UNKNOWN");
    unknownJobMessage.setMessageText("UNKNOWNUNKNOWN");
    unknownJobMessage.setTime("");
    JobMessage emptyJobMessage = new JobMessage();
    emptyJobMessage.setMessageImportance("JOB_MESSAGE_EMPTY");
    emptyJobMessage.setTime(TimeUtil.toCloudTime(unknownTime));

    new LoggingHandler()
        .process(
            Arrays.asList(
                errorJobMessage,
                warningJobMessage,
                basicJobMessage,
                detailedJobMessage,
                debugJobMessage,
                unknownJobMessage));

    expectedLogs.verifyError("ERRORERROR");
    expectedLogs.verifyError(errorTime.toString());
    expectedLogs.verifyWarn("WARNINGWARNING");
    expectedLogs.verifyWarn(warningTime.toString());
    expectedLogs.verifyInfo("BASICBASIC");
    expectedLogs.verifyInfo(basicTime.toString());
    expectedLogs.verifyInfo("DETAILEDDETAILED");
    expectedLogs.verifyInfo(detailedTime.toString());
    expectedLogs.verifyDebug("DEBUGDEBUG");
    expectedLogs.verifyDebug(debugTime.toString());
    expectedLogs.verifyTrace("UNKNOWN TIMESTAMP");
    expectedLogs.verifyTrace("UNKNOWNUNKNOWN");
    expectedLogs.verifyNotLogged(unknownTime.toString());
  }
}
