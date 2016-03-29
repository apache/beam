/*
 * Copyright (C) 2015 Google Inc.
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
import com.google.api.services.dataflow.Dataflow.Projects.Jobs.Create;
import com.google.api.services.dataflow.Dataflow.Projects.Jobs.Get;
import com.google.cloud.dataflow.sdk.options.GoogleApiDebugOptions.GoogleApiTracer;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.Transport;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GoogleApiDebugOptions}. */
@RunWith(JUnit4.class)
public class GoogleApiDebugOptionsTest {
  @Test
  public void testWhenTracingMatches() throws Exception {
    String[] args =
        new String[] {"--googleApiTrace={\"Projects.Jobs.Get\":\"GetTraceDestination\"}"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get request =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("GetTraceDestination", request.get("$trace"));
  }

  @Test
  public void testWhenTracingDoesNotMatch() throws Exception {
    String[] args = new String[] {"--googleApiTrace={\"Projects.Jobs.Create\":\"testToken\"}"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get request =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertNull(request.get("$trace"));
  }

  @Test
  public void testWithMultipleTraces() throws Exception {
    String[] args = new String[] {
        "--googleApiTrace={\"Projects.Jobs.Create\":\"CreateTraceDestination\","
        + "\"Projects.Jobs.Get\":\"GetTraceDestination\"}"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get getRequest =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("GetTraceDestination", getRequest.get("$trace"));

    Create createRequest =
        options.getDataflowClient().projects().jobs().create("testProjectId", null);
    assertEquals("CreateTraceDestination", createRequest.get("$trace"));
  }

  @Test
  public void testMatchingAllDataflowCalls() throws Exception {
    String[] args = new String[] {"--googleApiTrace={\"Dataflow\":\"TraceDestination\"}"};
    DataflowPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Get getRequest =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Create createRequest =
        options.getDataflowClient().projects().jobs().create("testProjectId", null);
    assertEquals("TraceDestination", createRequest.get("$trace"));
  }

  @Test
  public void testMatchingAgainstClient() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(new GoogleApiTracer().addTraceFor(
        Transport.newDataflowClient(options).build(), "TraceDestination"));

    Get getRequest =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Delete deleteRequest = Transport.newBigQueryClient(options).build().datasets()
        .delete("testProjectId", "testDatasetId");
    assertNull(deleteRequest.get("$trace"));
  }

  @Test
  public void testMatchingAgainstRequestType() throws Exception {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(new GoogleApiTracer().addTraceFor(
        Transport.newDataflowClient(options).build().projects().jobs()
            .get("aProjectId", "aJobId"), "TraceDestination"));

    Get getRequest =
        options.getDataflowClient().projects().jobs().get("testProjectId", "testJobId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Create createRequest =
        options.getDataflowClient().projects().jobs().create("testProjectId", null);
    assertNull(createRequest.get("$trace"));
  }

  @Test
  public void testDeserializationAndSerializationOfGoogleApiTracer() throws Exception {
    String serializedValue = "{\"Api\":\"Token\"}";
    ObjectMapper objectMapper = new ObjectMapper();
    assertEquals(serializedValue,
        objectMapper.writeValueAsString(
            objectMapper.readValue(serializedValue, GoogleApiTracer.class)));
  }
}
