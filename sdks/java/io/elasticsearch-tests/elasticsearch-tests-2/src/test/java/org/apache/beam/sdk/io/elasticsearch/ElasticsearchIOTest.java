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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.ES_TYPE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestCommon.NUM_DOCS_UTESTS;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
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

  private static Node node;
  private static RestClient restClient;
  private static ConnectionConfiguration connectionConfiguration;
  //cannot use inheritance because ES5 test already extends ESIntegTestCase.
  private static ElasticsearchIOTestCommon elasticsearchIOTestCommon;

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int esHttpPort = serverSocket.getLocalPort();
    serverSocket.close();
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
    connectionConfiguration = ConnectionConfiguration
        .create(new String[] { "http://" + ES_IP + ":" + esHttpPort }, ES_INDEX, ES_TYPE);
    restClient = connectionConfiguration.createClient();
    elasticsearchIOTestCommon = new ElasticsearchIOTestCommon(connectionConfiguration, restClient,
        false);
  }

  @AfterClass
  public static void afterClass() throws IOException{
    restClient.close();
    node.close();
  }

  @Before
  public void before() throws Exception {
    ElasticSearchIOTestUtils.deleteIndex(connectionConfiguration, restClient);
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
  public void testReadWithQuery() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testReadWithQuery();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWrite();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
    ElasticSearchIOTestUtils
        .insertTestDocuments(connectionConfiguration, NUM_DOCS_UTESTS, restClient);
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
        null);
    //desiredBundleSize is ignored because in ES 2.x there is no way to split shards. So we get
    // as many bundles as ES shards and bundle size is shard size
    int desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    //this is the number of ES shards
    // (By default, each index in Elasticsearch is allocated 5 primary shards)
    int expectedNumSources = 5;
    assertEquals("Wrong number of splits", expectedNumSources, splits.size());
    int emptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).isEmpty()) {
        emptySplits += 1;
      }
    }
    assertThat(
        "There are too many empty splits, parallelism is sub-optimal",
        emptySplits,
        lessThan((int) (ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE * splits.size())));
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
    elasticsearchIOTestCommon.testWriteWithTypeFn();
  }

  @Test
  public void testWriteFullAddressing() throws Exception {
    elasticsearchIOTestCommon.setPipeline(pipeline);
    elasticsearchIOTestCommon.testWriteWithFullAddressing();
  }
}
