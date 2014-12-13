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
package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for MonitoringUtil.
 */
@RunWith(JUnit4.class)
public class MonitoringUtilTest {
  private static final String PROJECT_ID = "someProject";
  private static final String JOB_ID = "1234";

  @Test
  public void testGetJobMessages() throws IOException {
    Dataflow.V1b3.Projects.Jobs.Messages mockMessages =
        mock(Dataflow.V1b3.Projects.Jobs.Messages.class);

    // Two requests are needed to get all the messages.
    Dataflow.V1b3.Projects.Jobs.Messages.List firstRequest =
        mock(Dataflow.V1b3.Projects.Jobs.Messages.List.class);
    Dataflow.V1b3.Projects.Jobs.Messages.List secondRequest =
        mock(Dataflow.V1b3.Projects.Jobs.Messages.List.class);

    when(mockMessages.list(PROJECT_ID, JOB_ID))
        .thenReturn(firstRequest)
        .thenReturn(secondRequest);

    ListJobMessagesResponse firstResponse = new ListJobMessagesResponse();
    firstResponse.setJobMessages(new ArrayList<JobMessage>());
    for (int i = 0; i < 100; ++i) {
      JobMessage message = new JobMessage();
      message.setId("message_" + i);
      message.setTime(TimeUtil.toCloudTime(new Instant(i)));
      firstResponse.getJobMessages().add(message);
    }
    String pageToken = "page_token";
    firstResponse.setNextPageToken(pageToken);

    ListJobMessagesResponse secondResponse = new ListJobMessagesResponse();
    secondResponse.setJobMessages(new ArrayList<JobMessage>());
    for (int i = 100; i < 150; ++i) {
      JobMessage message = new JobMessage();
      message.setId("message_" + i);
      message.setTime(TimeUtil.toCloudTime(new Instant(i)));
      secondResponse.getJobMessages().add(message);
    }

    when(firstRequest.execute()).thenReturn(firstResponse);
    when(secondRequest.execute()).thenReturn(secondResponse);

    MonitoringUtil util = new MonitoringUtil(PROJECT_ID, mockMessages);

    List<JobMessage> messages = util.getJobMessages(JOB_ID, -1);

    verify(secondRequest).setPageToken(pageToken);

    assertEquals(150, messages.size());
  }
}
