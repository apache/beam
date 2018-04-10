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

import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.FAMOUS_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.NUM_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.countByScientistName;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.client.RestClient;
import org.hamcrest.CustomMatcher;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common test class for {@link ElasticsearchIO}. */
class ElasticsearchIOTestCommon implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTestCommon.class);

  static final String ES_INDEX = "beam";
  static final String ES_TYPE = "test";
  static final long NUM_DOCS_UTESTS = 400L;
  static final long NUM_DOCS_ITESTS = 50000L;
  static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final long AVERAGE_DOC_SIZE = 25L;


  private static final long BATCH_SIZE = 200L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  private long numDocs;
  private ConnectionConfiguration connectionConfiguration;
  private RestClient restClient;
  private boolean useAsITests;

  private TestPipeline pipeline;
  private ExpectedException expectedException;

  ElasticsearchIOTestCommon(ConnectionConfiguration connectionConfiguration, RestClient restClient,
      boolean useAsITests) {
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

  void testSizes() throws Exception {
    if (!useAsITests) {
      ElasticSearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null,
        null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE * numDocs));
  }


  void testRead() throws Exception {
    if (!useAsITests) {
      ElasticSearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                //set to default value, useful just to test parameter passing.
                .withScrollKeepalive("5m")
                //set to default value, useful just to test parameter passing.
                .withBatchSize(100L));
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQuery() throws Exception {
    if (!useAsITests) {
      ElasticSearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

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
    PAssert.thatSingleton(output.apply("Count", Count.globally()))
        .isEqualTo(numDocs / NUM_SCIENTISTS);
    pipeline.run();
  }

  void testWrite() throws Exception {
    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
      pipeline
        .apply(Create.of(data))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
            refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein");
    assertEquals(numDocs / NUM_SCIENTISTS, count);
  }

  void testWriteWithErrors() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);
    List<String> input =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);
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
          ElasticSearchIOTestUtils.createDocuments(
              numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
          ElasticSearchIOTestUtils.createDocuments(
              numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withIdFn(new ExtractValueFn("scientist")));
    pipeline.run();

    long currentNumDocs =
        refreshIndexAndGetCurrentNumDocs(
            connectionConfiguration, restClient);
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
        ElasticSearchIOTestUtils.createDocuments(
            adjustedNumDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
          refreshIndexAndGetCurrentNumDocs(restClient, index, connectionConfiguration.getType());
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
   */
  void testWriteWithTypeFn() throws Exception {
    // defensive coding: this test requires an even number of docs
    long adjustedNumDocs = (numDocs & 1) == 0 ? numDocs : numDocs + 1;

    List<String> data =
        ElasticSearchIOTestUtils.createDocuments(
            adjustedNumDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
              restClient, connectionConfiguration.getIndex(), type);
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
        ElasticSearchIOTestUtils.createDocuments(
                numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
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
                refreshIndexAndGetCurrentNumDocs(restClient, index, type);
        assertEquals("Incorrect count for " + index + "/" + type, numDocs / NUM_SCIENTISTS, count);
      }

    }
  }
}
