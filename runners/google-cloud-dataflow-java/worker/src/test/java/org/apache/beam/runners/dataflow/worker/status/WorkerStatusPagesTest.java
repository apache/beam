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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.util.function.BooleanSupplier;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link WorkerStatusPages}. */
@RunWith(JUnit4.class)
public class WorkerStatusPagesTest {

  private final Server server = new Server();
  private final LocalConnector connector = new LocalConnector(server);
  @Mock private MemoryMonitor mockMemoryMonitor;
  private final BooleanSupplier mockHealthyIndicator = () -> true;
  private WorkerStatusPages wsp;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    wsp = new WorkerStatusPages(server, mockMemoryMonitor, mockHealthyIndicator);
    server.addConnector(connector);
    wsp.start();
  }

  @After
  public void tearDown() throws Exception {
    wsp.stop();
  }

  @Test
  public void testThreadz() throws Exception {
    String response = getPage("/threadz");
    assertThat(response, containsString("HTTP/1.1 200 OK"));
    assertThat(
        "Test method should appear in stack trace",
        response,
        containsString("WorkerStatusPagesTest.testThreadz"));
  }

  @Test
  public void testHealthzHealthy() throws Exception {
    String response = getPage("/healthz");
    assertThat(response, containsString("HTTP/1.1 200 OK"));
    assertThat(response, containsString("ok"));
  }

  @Test
  public void testHealthzUnhealthy() throws Exception {
    // set up WorkerStatusPages that respond unhealthy status on "healthz"
    wsp.stop();
    wsp = new WorkerStatusPages(server, mockMemoryMonitor, () -> false);
    wsp.start();

    String response = getPage("/healthz");
    assertThat(response, containsString("HTTP/1.1 500 Server Error"));
    assertThat(response, containsString("internal server error"));
  }

  @Test
  public void testUnknownHandler() throws Exception {
    String response = getPage("/missinghandlerz");
    assertThat(response, containsString("HTTP/1.1 302 Found"));
    assertThat(response, containsString("Location: http://localhost/statusz"));
  }

  private String getPage(String requestURL) throws Exception {
    String request = String.format("GET %s HTTP/1.1\nhost: localhost\n\n", requestURL);
    return connector.getResponses(request);
  }
}
