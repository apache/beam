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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.hamcrest.CustomMatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on {@link ElasticsearchIO}. */
@RunWith(JUnit4.class)
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String ES_INDEX = "beam";
  private static final String ES_TYPE = "test";
  private static final String ES_IP = "127.0.0.1";
  private static final long NUM_DOCS = 400L;
  private static final int NUM_SCIENTISTS = 10;
  private static final long BATCH_SIZE = 200L;
  private static final long AVERAGE_DOC_SIZE = 25L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  private boolean indexDeleted = false;
  private boolean waitForIndexDeletion = true;

  private static int esHttpPort;
  private static transient Node node;
  private static ElasticsearchIO.ConnectionConfiguration connectionConfiguration;

  @ClassRule public static TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    esHttpPort = serverSocket.getLocalPort();
    serverSocket.close();
    connectionConfiguration =
        ElasticsearchIO.ConnectionConfiguration.create(
            new String[] {"http://" + ES_IP + ":" + esHttpPort}, ES_INDEX, ES_TYPE);
    LOGGER.info("Starting embedded Elasticsearch instance ({})", esHttpPort);
    Settings.Builder settingsBuilder =
        Settings.settingsBuilder()
            .put("cluster.name", "beam")
            .put("http.enabled", "true")
            .put("node.data", "true")
            .put("path.data", folder.getRoot().getPath())
            .put("path.home", folder.getRoot().getPath())
            .put("node.name", "beam")
            .put("network.host", ES_IP)
            .put("http.port", esHttpPort)
            .put("index.store.stats_refresh_interval", 0)
            // had problems with some jdk, embedded ES was too slow for bulk insertion,
            // and queue of 50 was full. No pb with real ES instance (cf testWrite integration test)
            .put("threadpool.bulk.queue_size", 100);
    node = NodeBuilder.nodeBuilder().settings(settingsBuilder).build();
    LOGGER.info("Elasticsearch node created");
    node.start();
  }

  @AfterClass
  public static void afterClass() {
    node.close();
  }

  @Before
  public void before() throws Exception {
    IndicesAdminClient indices = node.client().admin().indices();
    IndicesExistsResponse indicesExistsResponse =
        indices.exists(new IndicesExistsRequest(ES_INDEX)).get();
    if (indicesExistsResponse.isExists()) {
      indices.prepareClose(ES_INDEX).get();
      // delete index is an asynchronous request, neither refresh or upgrade
      // delete all docs before starting tests. WaitForYellow() and delete directory are too slow,
      // so block thread until it is done (make it synchronous!!!)
      indices.delete(
          Requests.deleteIndexRequest(ES_INDEX),
          new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
              waitForIndexDeletion = false;
              indexDeleted = true;
            }

            @Override
            public void onFailure(Throwable throwable) {
              waitForIndexDeletion = false;
              indexDeleted = false;
            }
          });
      while (waitForIndexDeletion) {
        Thread.sleep(100);
      }
      if (!indexDeleted) {
        throw new IOException("Failed to delete index " + ES_INDEX);
      }
    }
  }

  @Test
  public void testSizes() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, node.client());
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOGGER.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE * NUM_DOCS));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testRead() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, node.client());

    TestPipeline pipeline = TestPipeline.create();

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                //set to default value, usefull just to test parameter passing.
                .withScrollKeepalive("5m")
                //set to default value, usefull just to test parameter passing.
                .withBatchSize(100L));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NUM_DOCS);
    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testReadWithQuery() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, node.client());

    String query =
        "{\n"
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

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .withQuery(query));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(NUM_DOCS / NUM_SCIENTISTS);
    pipeline.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWrite() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchIOTestUtils.upgradeIndexAndGetCurrentNumDocs(ES_INDEX, ES_TYPE, node.client());
    assertEquals(NUM_DOCS, currentNumDocs);

    QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("Einstein").field("scientist");
    SearchResponse searchResponse =
        node.client()
            .prepareSearch(ES_INDEX)
            .setTypes(ES_TYPE)
            .setQuery(queryBuilder)
            .execute()
            .actionGet();
    assertEquals(NUM_DOCS / NUM_SCIENTISTS, searchResponse.getHits().getTotalHits());
  }

  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testWriteWithErrors() throws Exception {
    ElasticsearchIO.Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    ElasticsearchIO.Write.WriterFn writerFn = new ElasticsearchIO.Write.WriterFn(write);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    DoFnTester<String, Void> fnTester = DoFnTester.of(writerFn);

    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);
    exception.expect(isA(IOException.class));
    exception.expectMessage(
        new CustomMatcher<String>("RegExp matcher") {
          @Override
          public boolean matches(Object o) {
            String message = (String) o;
            return message.matches(
                "(?is).*Error writing to Elasticsearch, some elements could not be inserted"
                    + ".*Document id.* failed to parse \\(mapper_parsing_exception\\).*Caused by: "
                    + "Unexpected character.* was expecting a colon to separate field name and "
                    + "value.*Document id.* failed to parse \\(mapper_parsing_exception\\).*"
                    + "Caused by: Unexpected character.* was expecting a colon to separate field "
                    + "name and value.*");
          }
        });
    // inserts into Elasticsearch
    fnTester.processBundle(input);
  }

  @Test
  public void testWriteWithMaxBatchSize() throws Exception {
    ElasticsearchIO.Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    ElasticsearchIO.Write.WriterFn writerFn = new ElasticsearchIO.Write.WriterFn(write);
    DoFnTester<String, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);
    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    long numDocsProcessed = 0;
    long numDocsInserted = 0;
    for (String document : input) {
      fnTester.processElement(document);
      numDocsProcessed++;
      // test every 100 docs to avoid overloading ES
      if ((numDocsProcessed % 100) == 0) {
        // force the index to upgrade after inserting for the inserted docs
        // to be searchable immediately
        long currentNumDocs =
            ElasticSearchIOTestUtils.upgradeIndexAndGetCurrentNumDocs(
                ES_INDEX, ES_TYPE, node.client());
        if ((numDocsProcessed % BATCH_SIZE) == 0) {
          /* bundle end */
          assertEquals(
              "we are at the end of a bundle, we should have inserted all processed documents",
              numDocsProcessed,
              currentNumDocs);
          numDocsInserted = currentNumDocs;
        } else {
          /* not bundle end */
          assertEquals(
              "we are not at the end of a bundle, we should have inserted no more documents",
              numDocsInserted,
              currentNumDocs);
        }
      }
    }
  }

  @Test
  public void testWriteWithMaxBatchSizeBytes() throws Exception {
    ElasticsearchIO.Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSizeBytes(BATCH_SIZE_BYTES);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    ElasticsearchIO.Write.WriterFn writerFn = new ElasticsearchIO.Write.WriterFn(write);
    DoFnTester<String, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);
    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    long numDocsProcessed = 0;
    long sizeProcessed = 0;
    long numDocsInserted = 0;
    long batchInserted = 0;
    for (String document : input) {
      fnTester.processElement(document);
      numDocsProcessed++;
      sizeProcessed += document.getBytes().length;
      // test every 40 docs to avoid overloading ES
      if ((numDocsProcessed % 40) == 0) {
        // force the index to upgrade after inserting for the inserted docs
        // to be searchable immediately
        long currentNumDocs =
            ElasticSearchIOTestUtils.upgradeIndexAndGetCurrentNumDocs(
                ES_INDEX, ES_TYPE, node.client());
        if (sizeProcessed / BATCH_SIZE_BYTES > batchInserted) {
          /* bundle end */
          assertThat(
              "we have passed a bundle size, we should have inserted some documents",
              currentNumDocs,
              greaterThan(numDocsInserted));
          numDocsInserted = currentNumDocs;
          batchInserted = (sizeProcessed / BATCH_SIZE_BYTES);
        } else {
          /* not bundle end */
          assertEquals(
              "we are not at the end of a bundle, we should have inserted no more documents",
              numDocsInserted,
              currentNumDocs);
        }
      }
    }
  }

  @Test
  public void testSplitIntoBundles() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, node.client());
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null);
    //desiredBundleSize is ignored because in ES 2.x there is no way to split shards. So we get
    // as many bundles as ES shards and bundle size is shard size
    int desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits =
        initialSource.splitIntoBundles(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    //this is the number of ES shards
    // (By default, each index in Elasticsearch is allocated 5 primary shards)
    int expectedNumSplits = 5;
    assertEquals(expectedNumSplits, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    assertEquals("Wrong number of empty splits", expectedNumSplits, nonEmptySplits);
  }
}
