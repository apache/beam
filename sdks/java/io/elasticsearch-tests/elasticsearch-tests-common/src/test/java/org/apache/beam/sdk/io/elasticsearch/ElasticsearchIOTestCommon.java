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
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.DEFAULT_RETRY_PREDICATE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.getBackendVersion;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.FAMOUS_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.NUM_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.countByMatch;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.countByScientistName;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.refreshIndexAndGetCurrentNumDocs;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.DefaultRetryPredicate;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.RetryPredicate;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.hamcrest.CustomMatcher;
import org.joda.time.Duration;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common test class for {@link ElasticsearchIO}. */
class ElasticsearchIOTestCommon implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTestCommon.class);

  private static final RetryPredicate CUSTOM_RETRY_PREDICATE = new DefaultRetryPredicate(400);

  private static final int EXPECTED_RETRIES = 2;
  private static final int MAX_ATTEMPTS = 3;
  private static final String[] BAD_FORMATTED_DOC = {"{ \"x\" :a,\"y\":\"ab\" }"};
  private static final String OK_REQUEST =
      "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"doc\", \"_id\" : \"1\" } }\n"
          + "{ \"field1\" : 1 }\n";
  private static final String BAD_REQUEST =
      "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"doc\", \"_id\" : \"1\" } }\n"
          + "{ \"field1\" : @ }\n";

  static String getEsIndex() {
    return "beam" + Thread.currentThread().getId();
  }

  static final String ES_TYPE = "test";
  static final long NUM_DOCS_UTESTS = 400L;
  static final long NUM_DOCS_ITESTS = 50000L;
  static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final long AVERAGE_DOC_SIZE = 25L;

  private static final long BATCH_SIZE = 200L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  public static final String UPDATE_INDEX = "partial_update";
  public static final String UPDATE_TYPE = "test";

  private final long numDocs;
  private final ConnectionConfiguration connectionConfiguration;
  private final RestClient restClient;
  private final boolean useAsITests;

  private TestPipeline pipeline;
  private ExpectedException expectedException;

  ElasticsearchIOTestCommon(
      ConnectionConfiguration connectionConfiguration, RestClient restClient, boolean useAsITests) {
    this.connectionConfiguration = connectionConfiguration;
    this.restClient = restClient;
    this.numDocs = useAsITests ? NUM_DOCS_ITESTS : NUM_DOCS_UTESTS;
    this.useAsITests = useAsITests;
  }

  // lazy init of the test rules (cannot be static)
  void setPipeline(TestPipeline pipeline) {
    this.pipeline = pipeline;
  }

  void setExpectedException(ExpectedException expectedException) {
    this.expectedException = expectedException;
  }

  void testSplit(final int desiredBundleSizeBytes) throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read = ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource =
        new BoundedElasticsearchSource(read, null, null, null);
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    long indexSize = BoundedElasticsearchSource.estimateIndexSize(connectionConfiguration);

    int expectedNumSources;
    if (desiredBundleSizeBytes == 0) {
      // desiredBundleSize is ignored because in ES 2.x there is no way to split shards.
      // 5 is the number of ES shards
      // (By default, each index in Elasticsearch is allocated 5 primary shards)
      expectedNumSources = 5;
    } else {
      float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
      expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
    }
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

  void testSizes() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read = ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource =
        new BoundedElasticsearchSource(read, null, null, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE * numDocs));
  }

  void testRead() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                // set to default value, useful just to test parameter passing.
                .withScrollKeepalive("5m")
                // set to default value, useful just to test parameter passing.
                .withBatchSize(100L));
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQueryString() throws Exception {
    testReadWithQueryInternal(Read::withQuery);
  }

  void testReadWithQueryValueProvider() throws Exception {
    testReadWithQueryInternal(
        (read, query) -> read.withQuery(ValueProvider.StaticValueProvider.of(query)));
  }

  private void testReadWithQueryInternal(BiFunction<Read, String, Read> queryConfigurer)
      throws IOException {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    String query =
        "{\n"
            + "  \"query\": {\n"
            + "  \"match\" : {\n"
            + "    \"scientist\" : {\n"
            + "      \"query\" : \"Einstein\"\n"
            + "    }\n"
            + "  }\n"
            + "  }\n"
            + "}";

    Read read = ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);

    read = queryConfigurer.apply(read, query);

    PCollection<String> output = pipeline.apply(read);

    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo(numDocs / NUM_SCIENTISTS);
    pipeline.run();
  }

  /** Test reading metadata by reading back the id of a document after writing it. */
  void testReadWithMetadata() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, 1, restClient);
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .withMetadata());
    PAssert.that(output).satisfies(new ContainsStringCheckerFn("\"_id\":\"0\""));
    pipeline.run();
  }

  void testWrite() throws Exception {
    Write write = ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration);
    executeWriteTest(write);
  }

  void testWriteWithErrors() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    List<String> input =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);
    expectedException.expect(isA(IOException.class));
    expectedException.expectMessage(
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
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      // inserts into Elasticsearch
      fnTester.processBundle(input);
    }
  }

  void testWriteWithMaxBatchSize() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      List<String> input =
          ElasticsearchIOTestUtils.createDocuments(
              numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
              refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
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
  }

  void testWriteWithMaxBatchSizeBytes() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSizeBytes(BATCH_SIZE_BYTES);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<String, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      List<String> input =
          ElasticsearchIOTestUtils.createDocuments(
              numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
      long numDocsProcessed = 0;
      long sizeProcessed = 0;
      long numDocsInserted = 0;
      long batchInserted = 0;
      for (String document : input) {
        fnTester.processElement(document);
        numDocsProcessed++;
        sizeProcessed += document.getBytes(StandardCharsets.UTF_8).length;
        // test every 40 docs to avoid overloading ES
        if ((numDocsProcessed % 40) == 0) {
          // force the index to upgrade after inserting for the inserted docs
          // to be searchable immediately
          long currentNumDocs =
              refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
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
  }

  /** Extracts the name field from the JSON document. */
  private static class ExtractValueFn implements Write.FieldValueExtractFn {
    private final String fieldName;

    private ExtractValueFn(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String apply(JsonNode input) {
      return input.path(fieldName).asText();
    }
  }

  /**
   * Tests that when using the scientist name as the document identifier only as many documents as
   * scientists are created, since subsequent calls with the same name invoke updates.
   */
  void testWriteWithIdFn() throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("scientist")));
    pipeline.run();

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(NUM_SCIENTISTS, currentNumDocs);

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein");
    assertEquals(1, count);
  }

  /**
   * Tests that documents are dynamically routed to different indexes and not the one specified in
   * the configuration. Documents should be routed to an index named the same as the scientist in
   * the document. Multiple indexes adds significant work to the ES server and even passing moderate
   * number of docs can overload the bulk queue and workers. The post explains more
   * https://www.elastic.co/blog/why-am-i-seeing-bulk-rejections-in-my-elasticsearch-cluster.
   * Therefore limit to a small number of docs to test routing behavior only.
   */
  void testWriteWithIndexFn() throws Exception {
    long docsPerScientist = 10; // very conservative
    long adjustedNumDocs = docsPerScientist * FAMOUS_SCIENTISTS.length;

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            adjustedNumDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIndexFn(new ExtractValueFn("scientist")));
    pipeline.run();

    // verify counts on each index
    for (String scientist : FAMOUS_SCIENTISTS) {
      String index = scientist.toLowerCase();
      long count =
          refreshIndexAndGetCurrentNumDocs(
              restClient,
              index,
              connectionConfiguration.getType(),
              getBackendVersion(connectionConfiguration));
      assertEquals(scientist + " index holds incorrect count", docsPerScientist, count);
    }
  }

  /** Returns TYPE_0 or TYPE_1 based on the modulo 2 of the hash of the named field. */
  static class Modulo2ValueFn implements Write.FieldValueExtractFn {
    private final String fieldName;

    Modulo2ValueFn(String fieldName) {
      this.fieldName = fieldName;
    }

    @Override
    public String apply(JsonNode input) {
      return "TYPE_" + input.path(fieldName).asText().hashCode() % 2;
    }
  }

  /**
   * Tests that documents are dynamically routed to different types and not the type that is given
   * in the configuration. Documents should be routed to the a type of type_0 or type_1 using a
   * modulo approach of the explicit id.
   *
   * <p>This test does not work with ES 6 because ES 6 does not allow one mapping has more than 1
   * type
   */
  void testWriteWithTypeFn2x5x() throws Exception {
    // defensive coding: this test requires an even number of docs
    long adjustedNumDocs = (numDocs & 1) == 0 ? numDocs : numDocs + 1;

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            adjustedNumDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withTypeFn(new Modulo2ValueFn("id")));
    pipeline.run();

    for (int i = 0; i < 2; i++) {
      String type = "TYPE_" + i;
      long count =
          refreshIndexAndGetCurrentNumDocs(
              restClient,
              connectionConfiguration.getIndex(),
              type,
              getBackendVersion(connectionConfiguration));
      assertEquals(type + " holds incorrect count", adjustedNumDocs / 2, count);
    }
  }

  /**
   * Tests that documents are correctly routed when index, type and document ID functions are
   * provided to overwrite the defaults of using the configuration and auto-generation of the
   * document IDs by Elasticsearch. The scientist name is used for the index, type and document ID.
   * As a result there should be only a single document in each index/type.
   */
  void testWriteWithFullAddressing() throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("id"))
                .withIndexFn(new ExtractValueFn("scientist"))
                .withTypeFn(new Modulo2ValueFn("scientist")));
    pipeline.run();

    for (String scientist : FAMOUS_SCIENTISTS) {
      String index = scientist.toLowerCase();
      for (int i = 0; i < 2; i++) {
        String type = "TYPE_" + scientist.hashCode() % 2;
        long count =
            refreshIndexAndGetCurrentNumDocs(
                restClient, index, type, getBackendVersion(connectionConfiguration));
        assertEquals("Incorrect count for " + index + "/" + type, numDocs / NUM_SCIENTISTS, count);
      }
    }
  }

  /**
   * Tests partial updates by adding a group field to each document in the standard test set. The
   * group field is populated as the modulo 2 of the document id allowing for a test to ensure the
   * documents are split into 2 groups.
   */
  void testWritePartialUpdate() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    // defensive coding to ensure our initial state is as expected

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    // partial documents containing the ID and group only
    List<String> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      data.add(String.format("{\"id\" : %s, \"group\" : %s}", i, i % 2));
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("id"))
                .withUsePartialUpdate(true));
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(numDocs, currentNumDocs);
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restClient, "Einstein"));

    // Partial update assertions
    assertEquals(numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "0"));
    assertEquals(numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "1"));
  }

  /**
   * Function for checking if any string in iterable contains expected substring. Fails if no match
   * is found.
   */
  private static class ContainsStringCheckerFn
      implements SerializableFunction<Iterable<String>, Void> {

    private String expectedSubString;

    ContainsStringCheckerFn(String expectedSubString) {
      this.expectedSubString = expectedSubString;
    }

    @Override
    public Void apply(Iterable<String> input) {
      for (String s : input) {
        if (s.contains(expectedSubString)) {
          return null;
        }
      }
      fail("No string found containing " + expectedSubString);
      return null;
    }
  }

  /** Test that the default predicate correctly parses chosen error code. */
  void testDefaultRetryPredicate(RestClient restClient) throws IOException {

    HttpEntity entity1 = new NStringEntity(BAD_REQUEST, ContentType.APPLICATION_JSON);
    Request request = new Request("POST", "/_bulk");
    request.addParameters(Collections.emptyMap());
    request.setEntity(entity1);
    Response response1 = restClient.performRequest(request);
    assertTrue(CUSTOM_RETRY_PREDICATE.test(response1.getEntity()));

    HttpEntity entity2 = new NStringEntity(OK_REQUEST, ContentType.APPLICATION_JSON);
    request.setEntity(entity2);
    Response response2 = restClient.performRequest(request);
    assertFalse(DEFAULT_RETRY_PREDICATE.test(response2.getEntity()));
  }

  /**
   * Test that retries are invoked when Elasticsearch returns a specific error code. We invoke this
   * by issuing corrupt data and retrying on the `400` error code. Normal behaviour is to retry on
   * `429` only but that is difficult to simulate reliably. The logger is used to verify expected
   * behavior.
   */
  void testWriteRetry() throws Throwable {
    expectedException.expectCause(isA(IOException.class));
    // max attempt is 3, but retry is 2 which excludes 1st attempt when error was identified and
    // retry started.
    expectedException.expectMessage(
        String.format(ElasticsearchIO.Write.WriteFn.RETRY_FAILED_LOG, EXPECTED_RETRIES));

    ElasticsearchIO.Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withRetryConfiguration(
                ElasticsearchIO.RetryConfiguration.create(MAX_ATTEMPTS, Duration.millis(35000))
                    .withRetryPredicate(CUSTOM_RETRY_PREDICATE));
    pipeline.apply(Create.of(Arrays.asList(BAD_FORMATTED_DOC))).apply(write);

    pipeline.run();
  }

  void testWriteRetryValidRequest() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withRetryConfiguration(
                ElasticsearchIO.RetryConfiguration.create(MAX_ATTEMPTS, Duration.millis(35000))
                    .withRetryPredicate(CUSTOM_RETRY_PREDICATE));
    executeWriteTest(write);
  }

  private void executeWriteTest(ElasticsearchIO.Write write) throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein");
    assertEquals(numDocs / NUM_SCIENTISTS, count);
  }

  /**
   * Tests deletion of documents from Elasticsearch index. Documents with odd integer as id are
   * deleted and those with even integer are partially updated. Documents to be deleted needs to
   * have criteria within the fields of the document.
   */
  void testWriteWithIsDeletedFnWithPartialUpdates() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    // partial documents containing the ID and group only
    List<String> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      // Scientist names at odd index to be deleted.
      data.add(String.format("{\"id\" : %s, \"is_deleted\": %b}", i, i % 2 == 1));
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("id"))
                .withUsePartialUpdate(true)
                .withIsDeleteFn(doc -> doc.get("is_deleted").asBoolean()));
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restClient, "Einstein"));

    // Check if documents are deleted as expected
    assertEquals(numDocs / 2, currentNumDocs);
    assertEquals(0, countByScientistName(connectionConfiguration, restClient, "Darwin"));
  }

  /**
   * Tests deletion of documents from Elasticsearch index. Documents with odd integer as id are
   * deleted. Documents to be deleted needs to have criteria within the fields of the document.
   */
  void testWriteWithIsDeletedFnWithoutPartialUpdate() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    // partial documents containing the ID and group only
    List<String> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      // Scientist names at odd index to be deleted.
      if (i % 2 == 1) {
        data.add(String.format("{\"id\" : %s, \"is_deleted\": %b}", i, true));
      }
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("id"))
                .withIsDeleteFn(doc -> doc.get("is_deleted").asBoolean()));
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restClient, "Einstein"));

    // Check if documents are deleted as expected
    assertEquals(numDocs / 2, currentNumDocs);
    assertEquals(0, countByScientistName(connectionConfiguration, restClient, "Darwin"));
  }

  /**
   * Tests IllegalArgumentException while deletion of documents from Elasticsearch index. While
   * using withIsDeleteFn(), it is mandatory to define the document using withIdFn().
   */
  void testInvalidWriteWithIsDeletedFn() throws Throwable {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }
    expectedException.expectCause(isA(IllegalArgumentException.class));

    // partial documents containing the ID and group only
    List<String> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      data.add(String.format("{\"id\" : %s, \"is_deleted\": true}", i));
    }

    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIndexFn(doc -> "test")
                .withIsDeleteFn(doc -> doc.get("is_deleted").asBoolean()));
    pipeline.run();
  }
}
