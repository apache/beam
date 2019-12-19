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
package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.getEsIndex;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.TestPipeline;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link ElasticsearchIO} version 2.x. */
@RunWith(JUnit4.class)
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String ES_IP = "127.0.0.1";
  private static final int MAX_STARTUP_WAITING_TIME_MSEC = 5000;
  private static int esHttpPort;
  private static Node node;
  private static RestClient restClient;
  private static ConnectionConfiguration connectionConfiguration;
  // cannot use inheritance because ES5 test already extends ESIntegTestCase.
  private static ElasticsearchIOTestCommon elasticsearchIOTestCommon;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws IOException {
    esHttpPort = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting embedded Elasticsearch instance ({})", esHttpPort);
    Settings.Builder settingsBuilder =
        Settings.settingsBuilder()
            .put("cluster.name", "beam")
            .put("http.enabled", "true")
            .put("node.data", "true")
            .put("path.data", TEMPORARY_FOLDER.getRoot().getPath())
            .put("path.home", TEMPORARY_FOLDER.getRoot().getPath())
            .put("node.name", "beam")
            .put("network.host", ES_IP)
            .put("http.port", esHttpPort)
            .put("index.store.stats_refresh_interval", 0)
            // had problems with some jdk, embedded ES was too slow for bulk insertion,
            // and queue of 50 was full. No pb with real ES instance (cf testWrite integration test)
            .put("threadpool.bulk.queue_size", 400);
    node = new Node(settingsBuilder.build());
    LOG.info("Elasticsearch node created");
    node.start();
    connectionConfiguration =
        ConnectionConfiguration.create(
                new String[] {"http://" + ES_IP + ":" + esHttpPort}, getEsIndex(), ES_TYPE)
            .withSocketTimeout(120000)
            .withConnectTimeout(5000);
    restClient = connectionConfiguration.createClient();
    elasticsearchIOTestCommon =
        new ElasticsearchIOTestCommon(connectionConfiguration, restClient, false);
    int waitingTime = 0;
    int healthCheckFrequency = 500;
    Request request = new Request("HEAD", "/");
    while ((waitingTime < MAX_STARTUP_WAITING_TIME_MSEC)
        && restClient.performRequest(request).getStatusLine().getStatusCode() != 200) {
      try {
        Thread.sleep(healthCheckFrequency);
        waitingTime += healthCheckFrequency;
      } catch (InterruptedException e) {
        LOG.warn(
            "Waiting thread was interrupted while waiting for connection to Elasticsearch to be available");
      }
    }
    if (waitingTime >= MAX_STARTUP_WAITING_TIME_MSEC) {
      throw new IOException("Max startup waiting for embedded Elasticsearch to start was exceeded");
    }
  }

  @AfterClass
  public static void afterClass() throws IOException {
    restClient.close();
    node.close();
  }

  @Before
  public void before() throws Exception {
    ElasticsearchIOTestUtils.deleteIndex(connectionConfiguration, restClient);
  }

  @Test
  public void testSizes() throws Exception {
    elasticsearchIOTestCommon.testSizes();
  }

  @Test
  public void testRead() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQueryString() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithQueryString();
  }

  @Test
  public void testReadWithQueryValueProvider() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithQueryValueProvider();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWrite();
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testWriteWithErrors() throws Exception {
    elasticsearchIOTestCommon.setExpectedException(expectedException);
    elasticsearchIOTestCommon.testWriteWithErrors();
  }

  @Test
  public void testWriteWithMaxBatchSize() throws Exception {
    elasticsearchIOTestCommon.testWriteWithMaxBatchSize();
  }

  @Test
  public void testWriteWithMaxBatchSizeBytes() throws Exception {
    elasticsearchIOTestCommon.testWriteWithMaxBatchSizeBytes();
  }

  @Test
  public void testSplit() throws Exception {
    elasticsearchIOTestCommon.testSplit(0);
  }

  @Test
  public void testWriteWithIdFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithIdFn();
  }

  @Test
  public void testWriteWithIndexFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithIndexFn();
  }

  @Test
  public void testWriteWithTypeFn() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithTypeFn2x5x();
  }

  @Test
  public void testWriteFullAddressing() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithFullAddressing();
  }

  @Test
  public void testWritePartialUpdate() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWritePartialUpdate();
  }

  @Test
  public void testReadWithMetadata() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithMetadata();
  }

  @Test
  public void testDefaultRetryPredicate() throws IOException {
    elasticsearchIOTestCommon.testDefaultRetryPredicate(restClient);
  }

  @Test
  public void testWriteRetry() throws Throwable {
    elasticsearchIOTestCommon.setExpectedException(expectedException);
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteRetry();
  }

  @Test
  public void testWriteRetryValidRequest() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteRetryValidRequest();
  }
}
