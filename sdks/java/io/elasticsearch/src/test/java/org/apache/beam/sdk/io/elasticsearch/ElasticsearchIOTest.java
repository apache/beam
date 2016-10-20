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
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test on {@link ElasticsearchIO}.
 */
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String DATA_DIRECTORY = "target/elasticsearch";
  public static final String ES_INDEX = "beam";
  public static final String ES_TYPE = "test";
  private static final String ES_IP = "127.0.0.1";
  private static final long NB_DOCS = 400L;

  private static int esHttpPort;

  private static transient Node node;

  @BeforeClass
  public static void beforeClass() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    esHttpPort = serverSocket.getLocalPort();
    serverSocket.close();

    FileUtils.deleteDirectory(new File(DATA_DIRECTORY));
    LOGGER.info("Starting embedded Elasticsearch instance ({})", esHttpPort);
    Settings.Builder settingsBuilder =
        Settings.settingsBuilder()
            .put("cluster.name", "beam")
            .put("http.enabled", "true")
            .put("node.data", "true")
            .put("path.data", DATA_DIRECTORY)
            .put("path.home", DATA_DIRECTORY)
            .put("node.name", "beam")
            .put("network.host", ES_IP)
            .put("http.port", esHttpPort)
            .put("index.store.stats_refresh_interval", 0);
    node = NodeBuilder.nodeBuilder().settings(settingsBuilder).build();
    LOGGER.info("Elasticsearch node created");
    if (node != null) {
      node.start();
    }
  }

  @Before
  public void before() throws Exception {
    IndicesAdminClient indices = node.client().admin().indices();
    IndicesExistsResponse indicesExistsResponse =
        indices.exists(new IndicesExistsRequest(ES_INDEX)).get();
    if (indicesExistsResponse.isExists()) {
      indices.prepareDelete(ES_INDEX).get();
    }
  }

  private void sampleIndex(long nbDocs) throws Exception {
    Client client = node.client();
    final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk().setRefresh(true);

    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    for (int i = 0; i < nbDocs; i++) {
      int index = i % scientists.length;
      String source = String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i);
      bulkRequestBuilder.add(client.prepareIndex(ES_INDEX, ES_TYPE, null).setSource(source));
    }
    final BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      throw new IOException("Cannot insert samples in index " + ES_INDEX);
    }
  }

  @Test
  public void testSizes() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    String[] addresses = {"http://" + ES_IP + ":" + esHttpPort};
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(
            ElasticsearchIO.ConnectionConfiguration
                .create(addresses, ES_INDEX, ES_TYPE));
    BoundedElasticsearchSource initialSource =
        new BoundedElasticsearchSource(read, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOGGER.info("Estimated size: {}", estimatedSize);
    assertTrue("Wrong estimated size", estimatedSize > 40000);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws Exception {
    sampleIndex(NB_DOCS);

    TestPipeline pipeline = TestPipeline.create();

    String[] addresses = { "http://" + ES_IP + ":" + esHttpPort };
    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withConnectionConfiguration(
            ElasticsearchIO.ConnectionConfiguration
                .create(addresses, ES_INDEX, ES_TYPE))
            .withScrollKeepalive("5m"));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadWithQuery() throws Exception {
    sampleIndex(NB_DOCS);

    String query = "{\n"
        + "  \"query\": {\n"
        + "  \"match\" : {\n"
        + "    \"scientist\" : {\n"
        + "      \"query\" : \"Einstein\",\n"
        + "      \"type\" : \"boolean\"\n"
        + "    }\n"
        + "  }\n"
        + "  }\n"
        + "}";

    Pipeline pipeline = TestPipeline.create();

    String[] addresses = {"http://" + ES_IP + ":" + esHttpPort};
    PCollection<String> output = pipeline.apply(
        ElasticsearchIO.read().withConnectionConfiguration(
            ElasticsearchIO.ConnectionConfiguration
                .create(addresses, ES_INDEX, ES_TYPE))
            .withQuery(query));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NB_DOCS / 10);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] addresses = {"http://" + ES_IP + ":" + esHttpPort};
    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write().withConnectionConfiguration(
            ElasticsearchIO.ConnectionConfiguration
                .create(addresses, ES_INDEX, ES_TYPE)));
    pipeline.run();

    // upgrade
    node.client().admin().indices().upgrade(new UpgradeRequest()).actionGet();

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteWithBatchSizes() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    String[] addresses = {"http://" + ES_IP + ":" + esHttpPort};
    String[] scientists =
        { "Einstein", "Darwin", "Copernicus", "Pasteur", "Curie", "Faraday", "Newton", "Bohr",
            "Galilei", "Maxwell" };
    ArrayList<String> data = new ArrayList<>();
    for (int i = 0; i < NB_DOCS; i++) {
      int index = i % scientists.length;
      data.add(String.format("{\"scientist\":\"%s\", \"id\":%d}", scientists[index], i));
    }
    PDone collection = pipeline.apply(Create.of(data)).apply(
        ElasticsearchIO.write()
            .withConnectionConfiguration(ElasticsearchIO.ConnectionConfiguration
                                             .create(addresses, ES_INDEX, ES_TYPE))
            .withBatchSize(NB_DOCS / 2)
            .withBatchSizeMegaBytes(1));

    pipeline.run();

    // upgrade
    node.client().admin().indices().upgrade(new UpgradeRequest()).actionGet();

    SearchResponse response = node.client().prepareSearch().execute().actionGet(5000);
    assertEquals(NB_DOCS, response.getHits().getTotalHits());

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    response = node.client().prepareSearch().setQuery(queryBuilder).execute().actionGet();
    assertEquals(NB_DOCS / 10, response.getHits().getTotalHits());
  }

  @Test
  public void testSplitIntoBundles() throws Exception {
    sampleIndex(NB_DOCS);
    PipelineOptions options = PipelineOptionsFactory.create();
    String[] addresses = {"http://" + ES_IP + ":" + esHttpPort};
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(
            ElasticsearchIO.ConnectionConfiguration
                .create(addresses, ES_INDEX, ES_TYPE));
    BoundedElasticsearchSource initialSource =
        new BoundedElasticsearchSource(read, null);
    //desiredBundleSize is ignored because in ES 2.x there is no way to split shards. So we get
    // as many bundles as ES shards and bundle size is shard size
    int desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits = initialSource.splitIntoBundles(
        desiredBundleSizeBytes, options);
    SourceTestUtils.
        assertSourcesEqualReferenceSource(initialSource, splits, options);
    //this is the number of ES shards
    int expectedNbSplits = 5;
    assertEquals(expectedNbSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNbSplits, nonEmptySplits);
  }

  @AfterClass
  public static void afterClass() {
    if (node != null) {
      node.close();
    }
  }

}
