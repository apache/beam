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

package com.google.cloud.dataflow.sdk.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.api.services.bigquery.Bigquery.Datasets.Delete;
import com.google.api.services.dataflow.Dataflow.V1b3.Projects.Jobs.Create;
import com.google.api.services.dataflow.Dataflow.V1b3.Projects.Jobs.Get;
import com.google.cloud.dataflow.sdk.options.GoogleApiDebugOptions.GoogleApiTracer;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.Transport;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GoogleApiDebugOptions}. */
@RunWith(JUnit4.class)
public class GoogleApiDebugOptionsTest {
  @Test
  public void testWhenTracingMatches() throws Exception {
    String[] args = new String[] {"--googleApiTrace=Projects.Jobs.Get#GetTestToken"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get request =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("GetTestToken", request.get("trace"));
  }

  @Test
  public void testWhenTracingDoesNotMatch() throws Exception {
    String[] args = new String[] {"--googleApiTrace=Projects.Jobs.Create#testToken"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get request =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertNull(request.get("trace"));
  }

  @Test
  public void testWithMultipleTraces() throws Exception {
    String[] args = new String[] {
        "--googleApiTrace=Projects.Jobs.Create#CreateTestToken,Projects.Jobs.Get#GetTestToken"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get getRequest =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("GetTestToken", getRequest.get("trace"));

    Create createRequest =
        options.getDataflowClient().v1b3().projects().jobs().create("testProjectId", null);
    assertEquals("CreateTestToken", createRequest.get("trace"));
  }

  @Test
  public void testMatchingAllDataflowV1b3Calls() throws Exception {
    String[] args = new String[] {"--googleApiTrace=Dataflow.V1b3#TestToken"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get getRequest =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TestToken", getRequest.get("trace"));

    Create createRequest =
        options.getDataflowClient().v1b3().projects().jobs().create("testProjectId", null);
    assertEquals("TestToken", createRequest.get("trace"));
  }

  @Test
  public void testMatchingAgainstClient() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(new GoogleApiTracer[] {
        GoogleApiTracer.create(Transport.newDataflowClient(options).build(), "TestToken")});

    Get getRequest =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TestToken", getRequest.get("trace"));

    Delete deleteRequest = Transport.newBigQueryClient(options).build().datasets()
        .delete("testProjectId", "testDatasetId");
    assertNull(deleteRequest.get("trace"));
  }

  @Test
  public void testMatchingAgainstRequestType() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(new GoogleApiTracer[] {GoogleApiTracer.create(
        Transport.newDataflowClient(options).build().v1b3().projects().jobs()
            .get("aProjectId", "aJobId"), "TestToken")});

    Get getRequest =
        options.getDataflowClient().v1b3().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TestToken", getRequest.get("trace"));

    Create createRequest =
        options.getDataflowClient().v1b3().projects().jobs().create("testProjectId", null);
    assertNull(createRequest.get("trace"));
  }
}
