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
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.hamcrest.CustomMatcher;
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

/** Tests for {@link ElasticsearchIO}. */
@RunWith(JUnit4.class)
public class ElasticsearchIOTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTest.class);

  private static final String ES_INDEX = "beam";
  private static final String ES_TYPE = "test";
  private static final String ES_IP = "127.0.0.1";
  private static final long NUM_DOCS = 400L;
  private static final int NUM_SCIENTISTS = 10;
  private static final long BATCH_SIZE = 200L;
  private static final long AVERAGE_DOC_SIZE = 25L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  private static Node node;
  private static RestClient restClient;
  private static ElasticsearchIO.ConnectionConfiguration connectionConfiguration;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
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
            .put("threadpool.bulk.queue_size", 100);
    node = new Node(settingsBuilder.build());
    LOG.info("Elasticsearch node created");
    node.start();
    connectionConfiguration =
      ElasticsearchIO.ConnectionConfiguration.create(
        new String[] {"http://" + ES_IP + ":" + esHttpPort}, ES_INDEX, ES_TYPE);
    restClient = connectionConfiguration.createClient();
  }

  @AfterClass
  public static void afterClass() throws IOException{
    restClient.close();
    node.close();
  }

  @Before
  public void before() throws Exception {
    ElasticSearchIOTestUtils.deleteIndex(ES_INDEX, restClient);
  }

  @Test
  public void testSizes() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, restClient);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE * NUM_DOCS));
  }

  @Test
  public void testRead() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, restClient);

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                //set to default value, useful just to test parameter passing.
                .withScrollKeepalive("5m")
                //set to default value, useful just to test parameter passing.
                .withBatchSize(100L));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(NUM_DOCS);
    pipeline.run();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, restClient);

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
  public void testWrite() throws Exception {
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs(ES_INDEX, ES_TYPE, restClient);
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
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    DoFnTester<String, Void> fnTester = DoFnTester.of(new ElasticsearchIO.Write.WriteFn(write));

    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            NUM_DOCS, ElasticSearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);
    exception.expect(isA(IOException.class));
    exception.expectMessage(
        new CustomMatcher<String>("RegExp matcher") {
          @Override
          public boolean matches(Object o) {
            String message = (String) o;
            // This regexp tests that 2 malformed documents are actually in error
            // and that the message contains their IDs.
            // It also ensures that root reason, root error type,
            // caused by reason and caused by error type are present in message.
            // To avoid flakiness of the test in case of Elasticsearch error message change,
            // only "failed to parse" root reason is matched,
            // the other messages are matched using .+
            return message.matches(
                "(?is).*Error writing to Elasticsearch, some elements could not be inserted"
                    + ".*Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*"
                    + "Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*");
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
    DoFnTester<String, Void> fnTester = DoFnTester.of(new ElasticsearchIO.Write.WriteFn(write));
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
        long currentNumDocs = ElasticSearchIOTestUtils
            .refreshIndexAndGetCurrentNumDocs(ES_INDEX, ES_TYPE, restClient);
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
    DoFnTester<String, Void> fnTester = DoFnTester.of(new ElasticsearchIO.Write.WriteFn(write));
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
            ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs(
                ES_INDEX, ES_TYPE, restClient);
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
  public void testSplit() throws Exception {
    ElasticSearchIOTestUtils.insertTestDocuments(ES_INDEX, ES_TYPE, NUM_DOCS, restClient);
    PipelineOptions options = PipelineOptionsFactory.create();
    ElasticsearchIO.Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null);
    //desiredBundleSize is ignored because in ES 2.x there is no way to split shards. So we get
    // as many bundles as ES shards and bundle size is shard size
    int desiredBundleSizeBytes = 0;
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
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
