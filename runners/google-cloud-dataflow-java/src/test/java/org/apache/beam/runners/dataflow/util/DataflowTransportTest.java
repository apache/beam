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
import static org.junit.Assert.assertNull;
import static org.junit.Assume.assumeTrue;

import com.google.api.services.dataflow.Dataflow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowTransport}. */
@RunWith(JUnit4.class)
public class DataflowTransportTest {

  private static final String UNIVERSE_DOMAIN = "example-universe.goog";

  private DataflowPipelineOptions options;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
  }

  @Test
  public void testApiRootUrlIsUnsetByDefault() {
    assertNull(options.getApiRootUrl());
    assertEquals("", options.getDataflowEndpoint());
  }

  @Test
  public void testDefaultEndpoint() {
    // The default endpoint is resolved by the client library, which honors
    // GOOGLE_CLOUD_UNIVERSE_DOMAIN. Only assert the public default if it is not set.
    assumeTrue(System.getenv("GOOGLE_CLOUD_UNIVERSE_DOMAIN") == null);
    Dataflow client = DataflowTransport.newDataflowClient(options).build();
    assertEquals(Dataflow.DEFAULT_BASE_URL, client.getBaseUrl());
  }

  @Test
  public void testDefaultEndpointHonorsUniverseDomain() {
    Dataflow client =
        DataflowTransport.newDataflowClient(options).setUniverseDomain(UNIVERSE_DOMAIN).build();
    assertEquals("https://dataflow." + UNIVERSE_DOMAIN + "/", client.getBaseUrl());
  }

  @Test
  public void testRelativeDataflowEndpointHonorsUniverseDomain() {
    options.setDataflowEndpoint("v0neverExisted");
    Dataflow client =
        DataflowTransport.newDataflowClient(options).setUniverseDomain(UNIVERSE_DOMAIN).build();
    assertEquals("https://dataflow." + UNIVERSE_DOMAIN + "/v0neverExisted/", client.getBaseUrl());
  }

  @Test
  public void testAbsoluteDataflowEndpointOverridesUniverseDomain() {
    options.setDataflowEndpoint("https://custom-dataflow.example.com:8443/some/path/");
    Dataflow client =
        DataflowTransport.newDataflowClient(options).setUniverseDomain(UNIVERSE_DOMAIN).build();
    assertEquals("https://custom-dataflow.example.com:8443/", client.getRootUrl());
    assertEquals("some/path/", client.getServicePath());
  }

  @Test
  public void testApiRootUrlOverridesUniverseDomain() {
    options.setApiRootUrl("https://custom-dataflow.example.com/");
    Dataflow client =
        DataflowTransport.newDataflowClient(options).setUniverseDomain(UNIVERSE_DOMAIN).build();
    assertEquals("https://custom-dataflow.example.com/", client.getBaseUrl());
  }

  @Test
  public void testApiRootUrlWithRelativeDataflowEndpoint() {
    options.setApiRootUrl("https://custom-dataflow.example.com/");
    options.setDataflowEndpoint("v0neverExisted");
    Dataflow client = DataflowTransport.newDataflowClient(options).build();
    assertEquals("https://custom-dataflow.example.com/v0neverExisted/", client.getBaseUrl());
  }

  @Test
  public void testExplicitDefaultApiRootUrlIsNotOverridden() {
    options.setApiRootUrl(Dataflow.DEFAULT_ROOT_URL);
    Dataflow client =
        DataflowTransport.newDataflowClient(options).setUniverseDomain(UNIVERSE_DOMAIN).build();
    assertEquals(Dataflow.DEFAULT_ROOT_URL, client.getBaseUrl());
  }
}
