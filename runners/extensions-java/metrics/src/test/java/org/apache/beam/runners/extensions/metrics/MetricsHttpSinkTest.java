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
package org.apache.beam.runners.extensions.metrics;

import static org.junit.Assert.assertEquals;

import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test class for MetricsHttpSink. */
public class MetricsHttpSinkTest {
  private static int port;
  private static List<String> messages = new ArrayList<>();
  private static HttpServer httpServer;
  private static CountDownLatch countDownLatch;

  @BeforeClass
  public static void beforeClass() throws IOException {
    // get free local port
    ServerSocket serverSocket = new ServerSocket(0);
    port = serverSocket.getLocalPort();
    serverSocket.close();
    httpServer = HttpServer.create(new InetSocketAddress(port), 0);
    httpServer
        .createContext("/")
        .setHandler(
            httpExchange -> {
              try (final BufferedReader in =
                  new BufferedReader(
                      new InputStreamReader(
                          httpExchange.getRequestBody(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                  messages.add(line);
                }
              }
              httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0L);
              httpExchange.close();
              countDownLatch.countDown();
            });
    httpServer.start();
  }

  @Before
  public void before() {
    messages.clear();
  }

  @Test
  public void testWriteMetricsWithCommittedSupported() throws Exception {
    MetricQueryResults metricQueryResults = new CustomMetricQueryResults(true);
    MetricsOptions pipelineOptions = PipelineOptionsFactory.create().as(MetricsOptions.class);
    pipelineOptions.setMetricsHttpSinkUrl(String.format("http://localhost:%s", port));
    MetricsHttpSink metricsHttpSink = new MetricsHttpSink(pipelineOptions);
    countDownLatch = new CountDownLatch(1);
    metricsHttpSink.writeMetrics(metricQueryResults);
    countDownLatch.await();
    String expected =
        "{\"counters\":[{\"attempted\":20,\"committed\":10,\"name\":{\"name\":\"n1\","
            + "\"namespace\":\"ns1\"},\"step\":\"s1\"}],\"distributions\":[{\"attempted\":"
            + "{\"count\":4,\"max\":9,\"mean\":6.25,\"min\":3,\"sum\":25},\"committed\":"
            + "{\"count\":2,\"max\":8,\"mean\":5.0,\"min\":5,\"sum\":10},\"name\":{\"name\":\"n2\","
            + "\"namespace\":\"ns1\"},\"step\":\"s2\"}],\"gauges\":[{\"attempted\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":120},\"committed\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":100},\"name\":{\"name\":\"n3\",\"namespace\":"
            + "\"ns1\"},\"step\":\"s3\"}],\"perWorkerHistograms\":[],\"stringSets\":[{\"attempted\":{\"stringSet\":[\"cd"
            + "\"]},\"committed\":{\"stringSet\":[\"ab\"]},\"name\":{\"name\":\"n3\","
            + "\"namespace\":\"ns1\"},\"step\":\"s3\"}]}";
    assertEquals("Wrong number of messages sent to HTTP server", 1, messages.size());
    assertEquals("Wrong messages sent to HTTP server", expected, messages.get(0));
  }

  @Test
  public void testWriteMetricsWithCommittedUnSupported() throws Exception {
    MetricQueryResults metricQueryResults = new CustomMetricQueryResults(false);
    MetricsOptions pipelineOptions = PipelineOptionsFactory.create().as(MetricsOptions.class);
    pipelineOptions.setMetricsHttpSinkUrl(String.format("http://localhost:%s", port));
    MetricsHttpSink metricsHttpSink = new MetricsHttpSink(pipelineOptions);
    countDownLatch = new CountDownLatch(1);
    metricsHttpSink.writeMetrics(metricQueryResults);
    countDownLatch.await();
    String expected =
        "{\"counters\":[{\"attempted\":20,\"name\":{\"name\":\"n1\","
            + "\"namespace\":\"ns1\"},\"step\":\"s1\"}],\"distributions\":[{\"attempted\":"
            + "{\"count\":4,\"max\":9,\"mean\":6.25,\"min\":3,\"sum\":25},\"name\":{\"name\":\"n2\""
            + ",\"namespace\":\"ns1\"},\"step\":\"s2\"}],\"gauges\":[{\"attempted\":{\"timestamp\":"
            + "\"1970-01-05T00:04:22.800Z\",\"value\":120},\"name\":{\"name\":\"n3\",\"namespace\":"
            + "\"ns1\"},\"step\":\"s3\"}],\"perWorkerHistograms\":[],\"stringSets\":[{\"attempted\":{\"stringSet\":[\"cd\"]},"
            + "\"name\":{\"name\":\"n3\",\"namespace\":\"ns1\"},\"step\":\"s3\"}]}";
    assertEquals("Wrong number of messages sent to HTTP server", 1, messages.size());
    assertEquals("Wrong messages sent to HTTP server", expected, messages.get(0));
  }

  @AfterClass
  public static void after() {
    httpServer.stop(0);
  }
}
