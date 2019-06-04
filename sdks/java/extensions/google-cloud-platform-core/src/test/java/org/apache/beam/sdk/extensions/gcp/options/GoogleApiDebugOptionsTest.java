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
package org.apache.beam.sdk.extensions.gcp.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.cloudresourcemanager.CloudResourceManager.Projects.Delete;
import com.google.api.services.storage.Storage;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GoogleApiDebugOptions.GoogleApiTracer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GoogleApiDebugOptions}. */
@RunWith(JUnit4.class)
public class GoogleApiDebugOptionsTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  private static final String STORAGE_GET_TRACE =
      "--googleApiTrace={\"Objects.Get\":\"GetTraceDestination\"}";
  private static final String STORAGE_GET_AND_LIST_TRACE =
      "--googleApiTrace={\"Objects.Get\":\"GetTraceDestination\","
          + "\"Objects.List\":\"ListTraceDestination\"}";
  private static final String STORAGE_TRACE = "--googleApiTrace={\"Storage\":\"TraceDestination\"}";

  @Test
  public void testWhenTracingMatches() throws Exception {
    String[] args = new String[] {STORAGE_GET_TRACE};
    GcsOptions options = PipelineOptionsFactory.fromArgs(args).as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());
    assertNotNull(options.getGoogleApiTrace());

    Storage.Objects.Get request =
        Transport.newStorageClient(options).build().objects().get("testBucketId", "testObjectId");
    assertEquals("GetTraceDestination", request.get("$trace"));
  }

  @Test
  public void testWhenTracingDoesNotMatch() throws Exception {
    String[] args = new String[] {STORAGE_GET_TRACE};
    GcsOptions options = PipelineOptionsFactory.fromArgs(args).as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Storage.Objects.List request =
        Transport.newStorageClient(options).build().objects().list("testProjectId");
    assertNull(request.get("$trace"));
  }

  @Test
  public void testWithMultipleTraces() throws Exception {
    String[] args = new String[] {STORAGE_GET_AND_LIST_TRACE};
    GcsOptions options = PipelineOptionsFactory.fromArgs(args).as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Storage.Objects.Get getRequest =
        Transport.newStorageClient(options).build().objects().get("testBucketId", "testObjectId");
    assertEquals("GetTraceDestination", getRequest.get("$trace"));

    Storage.Objects.List listRequest =
        Transport.newStorageClient(options).build().objects().list("testProjectId");
    assertEquals("ListTraceDestination", listRequest.get("$trace"));
  }

  @Test
  public void testMatchingAllCalls() throws Exception {
    String[] args = new String[] {STORAGE_TRACE};
    GcsOptions options = PipelineOptionsFactory.fromArgs(args).as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());

    assertNotNull(options.getGoogleApiTrace());

    Storage.Objects.Get getRequest =
        Transport.newStorageClient(options).build().objects().get("testBucketId", "testObjectId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Storage.Objects.List listRequest =
        Transport.newStorageClient(options).build().objects().list("testProjectId");
    assertEquals("TraceDestination", listRequest.get("$trace"));
  }

  @Test
  public void testMatchingAgainstClient() throws Exception {
    GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(
        new GoogleApiTracer()
            .addTraceFor(Transport.newStorageClient(options).build(), "TraceDestination"));

    Storage.Objects.Get getRequest =
        Transport.newStorageClient(options).build().objects().get("testBucketId", "testObjectId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Delete deleteRequest =
        GcpOptions.GcpTempLocationFactory.newCloudResourceManagerClient(
                options.as(CloudResourceManagerOptions.class))
            .build()
            .projects()
            .delete("testProjectId");
    assertNull(deleteRequest.get("$trace"));
  }

  @Test
  public void testMatchingAgainstRequestType() throws Exception {
    GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setGoogleApiTrace(
        new GoogleApiTracer()
            .addTraceFor(
                Transport.newStorageClient(options)
                    .build()
                    .objects()
                    .get("aProjectId", "aObjectId"),
                "TraceDestination"));

    Storage.Objects.Get getRequest =
        Transport.newStorageClient(options).build().objects().get("testBucketId", "testObjectId");
    assertEquals("TraceDestination", getRequest.get("$trace"));

    Storage.Objects.List listRequest =
        Transport.newStorageClient(options).build().objects().list("testProjectId");
    assertNull(listRequest.get("$trace"));
  }

  @Test
  public void testDeserializationAndSerializationOfGoogleApiTracer() throws Exception {
    String serializedValue = "{\"Api\":\"Token\"}";
    assertEquals(
        serializedValue,
        MAPPER.writeValueAsString(MAPPER.readValue(serializedValue, GoogleApiTracer.class)));
  }
}
