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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.util.BackOff;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Project;
import java.net.SocketTimeoutException;
import org.apache.beam.sdk.options.CloudResourceManagerOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.FastNanoClockAndSleeper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Test case for {@link GcpProjectUtil}. */
@RunWith(JUnit4.class)
public class GcpProjectUtilTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static CloudResourceManagerOptions crmOptionsWithTestCredential() {
    CloudResourceManagerOptions pipelineOptions =
        PipelineOptionsFactory.as(CloudResourceManagerOptions.class);
    pipelineOptions.setGcpCredential(new TestCredential());
    return pipelineOptions;
  }

  @Test
  public void testGetProjectNumber() throws Exception {
    CloudResourceManagerOptions pipelineOptions = crmOptionsWithTestCredential();
    GcpProjectUtil projectUtil = pipelineOptions.getGcpProjectUtil();

    CloudResourceManager.Projects mockProjects = Mockito.mock(
        CloudResourceManager.Projects.class);
    CloudResourceManager mockCrm = Mockito.mock(CloudResourceManager.class);
    projectUtil.setCrmClient(mockCrm);

    CloudResourceManager.Projects.Get mockProjectsGet =
        Mockito.mock(CloudResourceManager.Projects.Get.class);

    BackOff mockBackOff = FluentBackoff.DEFAULT.backoff();
    Project project = new Project();
    project.setProjectNumber(5L);

    when(mockCrm.projects()).thenReturn(mockProjects);
    when(mockProjects.get(any(String.class))).thenReturn(mockProjectsGet);
    when(mockProjectsGet.execute())
      .thenThrow(new SocketTimeoutException("SocketException"))
      .thenReturn(project);

    assertEquals(5L, projectUtil.getProjectNumber(
        "foo", mockBackOff, new FastNanoClockAndSleeper()));
  }
}
