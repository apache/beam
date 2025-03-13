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
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BulkIO;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.DocToBulk;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.DEFAULT_RETRY_PREDICATE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.getBackendVersion;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.FAMOUS_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.INVALID_DOCS_IDS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.INVALID_LONG_ID;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.NUM_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.SCRIPT_SOURCE;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.countByMatch;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.countByScientistName;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.flushAndRefreshAllIndices;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.insertTestDocuments;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.mapToInputId;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.mapToInputIdString;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.refreshIndexAndGetCurrentNumDocs;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BulkIO.StatefulBatching;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Document;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.DocumentCoder;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.DefaultRetryPredicate;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.RetryConfiguration.RetryPredicate;
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIOTestUtils.InjectionMode;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common test class for {@link ElasticsearchIO}. */
@SuppressWarnings("nullness")
class ElasticsearchIOTestCommon {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTestCommon.class);

  private static final RetryPredicate CUSTOM_RETRY_PREDICATE = new DefaultRetryPredicate(400);

  private static final int EXPECTED_RETRIES = 2;
  private static final int MAX_ATTEMPTS = 3;
  private static final String[] BAD_FORMATTED_DOC = {"{ \"x\" :a,\"y\":\"ab\" }"};
  private static final String OK_REQUEST =
      "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"doc\", \"_id\" : \"1\" } }\n"
          + "{ \"field1\" : 1 }\n";
  private static final String OK_REQUEST_NO_TYPE =
      "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }\n" + "{ \"field1\" : 1 }\n";
  private static final String BAD_REQUEST =
      "{ \"index\" : { \"_index\" : \"test\", \"_type\" : \"doc\", \"_id\" : \"1\" } }\n"
          + "{ \"field1\" : @ }\n";
  private static final String BAD_REQUEST_NO_TYPE =
      "{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }\n" + "{ \"field1\" : @ }\n";

  static String getEsIndex() {
    return "beam" + Thread.currentThread().getId();
  }

  static final String ES_TYPE = "test";
  static final long NUM_DOCS_UTESTS = 40L;
  static final long NUM_DOCS_ITESTS = 50000L;
  static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final long AVERAGE_DOC_SIZE = 25L;

  private static final long BATCH_SIZE = 200L;
  private static final long BATCH_SIZE_BYTES = 2048L;

  public static final String UPDATE_INDEX = "partial_update";
  public static final String UPDATE_TYPE = "test";

  private final long numDocs;
  private final ConnectionConfiguration connectionConfiguration;
  final RestClient restClient;
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
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null);
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    long indexSize = BoundedElasticsearchSource.estimateIndexSize(connectionConfiguration);

    int expectedNumSources;
    float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
    expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
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
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read, null, null);
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

  /** Point in Time search is currently available for Elasticsearch version 8+. */
  void testReadPIT() throws Exception {
    if (!useAsITests) {
      ElasticsearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restClient);
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .withPointInTimeSearch());
    PAssert.thatSingleton(output.apply("Count", Count.globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQueryString() throws Exception {
    testReadWithQueryInternal(Read::withQuery, true);
  }

  void testReadWithQueryAndPIT() throws Exception {
    testReadWithQueryInternal(Read::withQuery, false);
  }

  void testReadWithQueryValueProvider() throws Exception {
    testReadWithQueryInternal(
        (read, query) -> read.withQuery(ValueProvider.StaticValueProvider.of(query)), true);
  }

  private void testReadWithQueryInternal(
      BiFunction<Read, String, Read> queryConfigurer, boolean useScrollAPI) throws IOException {
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
            + "   }\n"
            + "  }\n"
            + "}";

    Read read = ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);

    read = queryConfigurer.apply(read, query);

    read = useScrollAPI ? read : read.withPointInTimeSearch();

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

  /** Test that DocToBulk and BulkIO can be constructed and operate independently of Write. */
  void testDocToBulkAndBulkIO() throws Exception {
    DocToBulk docToBulk =
        ElasticsearchIO.docToBulk().withConnectionConfiguration(connectionConfiguration);
    BulkIO bulkIO = ElasticsearchIO.bulkIO().withConnectionConfiguration(connectionConfiguration);

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline.apply(Create.of(data)).apply(docToBulk).apply(bulkIO);
    pipeline.run();

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein", null);
    assertEquals(numDocs / NUM_SCIENTISTS, count);
  }

  void testWriteStateful() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withUseStatefulBatches(true);
    executeWriteTest(write);
  }

  List<Document> serializeDocs(ElasticsearchIO.Write write, List<String> jsonDocs)
      throws IOException {
    List<Document> serializedInput = new ArrayList<>();
    for (String doc : jsonDocs) {
      String bulkDoc =
          DocToBulk.createBulkApiEntity(
              write.getDocToBulk(), doc, getBackendVersion(connectionConfiguration));
      Document r =
          Document.create()
              .withInputDoc(doc)
              .withBulkDirective(bulkDoc)
              .withTimestamp(Instant.now());
      serializedInput.add(r);
    }
    return serializedInput;
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
    try (DoFnTester<Document, Document> fnTester =
        DoFnTester.of(new BulkIO.BulkIOBundleFn(write.getBulkIO()))) {
      // inserts into Elasticsearch
      fnTester.processBundle(serializeDocs(write, input));
    }
  }

  void testWriteWithErrorsReturned() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE)
            .withThrowWriteErrors(false);

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);

    PCollectionTuple outputs = pipeline.apply(Create.of(data)).apply(write);
    PCollection<Integer> success =
        outputs
            .get(Write.SUCCESSFUL_WRITES)
            .apply("Convert success to input ID", MapElements.via(mapToInputId));
    PCollection<Integer> fail =
        outputs
            .get(Write.FAILED_WRITES)
            .apply("Convert fails to input ID", MapElements.via(mapToInputId));

    Set<Integer> successfulIds =
        IntStream.range(0, data.size()).boxed().collect(Collectors.toSet());
    successfulIds.removeAll(INVALID_DOCS_IDS);

    PAssert.that(success).containsInAnyOrder(successfulIds);
    PAssert.that(fail).containsInAnyOrder(INVALID_DOCS_IDS);

    pipeline.run();
  }

  void testWriteWithErrorsReturnedAllowedErrors() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE)
            .withThrowWriteErrors(false)
            .withAllowableResponseErrors(Collections.singleton("json_parse_exception"));

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);

    PCollectionTuple outputs = pipeline.apply(Create.of(data)).apply(write);
    PCollection<Integer> success =
        outputs
            .get(Write.SUCCESSFUL_WRITES)
            .apply("Convert success to input ID", MapElements.via(mapToInputId));
    PCollection<Integer> fail =
        outputs
            .get(Write.FAILED_WRITES)
            .apply("Convert fails to input ID", MapElements.via(mapToInputId));

    // Successful IDs should be all IDs, as we're explicitly telling the ES transform that we
    // want to ignore failures of a certain kind, therefore treat those failures as having been
    // successfully processed
    Set<Integer> successfulIds =
        IntStream.range(0, data.size()).boxed().collect(Collectors.toSet());

    PAssert.that(success).containsInAnyOrder(successfulIds);
    PAssert.that(fail).empty();

    pipeline.run();
  }

  void testWriteWithElasticClientResponseException() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(numDocs + 1)
            .withMaxBatchSizeBytes(
                Long.MAX_VALUE) // Max long number to make sure all docs are flushed in one batch.
            .withThrowWriteErrors(false)
            .withIdFn(new ExtractValueFn("id"))
            .withUseStatefulBatches(true);

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.INJECT_ONE_ID_TOO_LONG_DOC);

    PCollectionTuple outputs = pipeline.apply(Create.of(data)).apply(write);

    // The whole batch should fail and direct to tag FAILED_WRITES because of one invalid doc.
    PCollection<String> success =
        outputs
            .get(Write.SUCCESSFUL_WRITES)
            .apply("Convert success to input ID", MapElements.via(mapToInputIdString));

    PCollection<String> fail =
        outputs
            .get(Write.FAILED_WRITES)
            .apply("Convert fails to input ID", MapElements.via(mapToInputIdString));

    Set<String> failedIds =
        IntStream.range(0, data.size() - 1).mapToObj(String::valueOf).collect(Collectors.toSet());
    failedIds.add(INVALID_LONG_ID);
    PAssert.that(success).empty();
    PAssert.that(fail).containsInAnyOrder(failedIds);

    // Verify response item contains the corresponding error message.
    PAssert.that(outputs.get(Write.FAILED_WRITES))
        .satisfies(responseItemJsonSubstringValidator("java.io.IOException"));
    pipeline.run();
  }

  void testWriteWithAllowedErrors() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE)
            .withAllowableResponseErrors(Collections.singleton("json_parse_exception"));
    List<String> input =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS);

    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<Document, Document> fnTester =
        DoFnTester.of(new BulkIO.BulkIOBundleFn(write.getBulkIO()))) {
      // inserts into Elasticsearch
      fnTester.processBundle(serializeDocs(write, input));
    }
  }

  void testWriteWithMaxBatchSize() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxBatchSize(BATCH_SIZE);

    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<Document, Document> fnTester =
        DoFnTester.of(new BulkIO.BulkIOBundleFn(write.getBulkIO()))) {
      List<String> input =
          ElasticsearchIOTestUtils.createDocuments(
              numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

      List<Document> serializedInput = new ArrayList<>();
      for (String doc : input) {
        String bulkDoc =
            DocToBulk.createBulkApiEntity(
                write.getDocToBulk(), doc, getBackendVersion(connectionConfiguration));
        Document r =
            Document.create()
                .withInputDoc(doc)
                .withBulkDirective(bulkDoc)
                .withTimestamp(Instant.now());
        serializedInput.add(r);
      }
      long numDocsProcessed = 0;
      long numDocsInserted = 0;
      for (Document document : serializedInput) {
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
    try (DoFnTester<Document, Document> fnTester =
        DoFnTester.of(new BulkIO.BulkIOBundleFn(write.getBulkIO()))) {
      List<String> input =
          ElasticsearchIOTestUtils.createDocuments(
              numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
      List<Document> serializedInput = new ArrayList<>();
      for (String doc : input) {
        String bulkDoc =
            DocToBulk.createBulkApiEntity(
                write.getDocToBulk(), doc, getBackendVersion(connectionConfiguration));
        Document r =
            Document.create()
                .withInputDoc(doc)
                .withBulkDirective(bulkDoc)
                .withTimestamp(Instant.now());
        serializedInput.add(r);
      }
      long numDocsProcessed = 0;
      long sizeProcessed = 0;
      long numDocsInserted = 0;
      long batchInserted = 0;
      for (Document document : serializedInput) {
        fnTester.processElement(document);
        numDocsProcessed++;
        sizeProcessed += document.getBulkDirective().getBytes(StandardCharsets.UTF_8).length;
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
    private final String disambiguationName;

    private ExtractValueFn(String fieldName, String disambiguationName) {
      this.fieldName = fieldName;
      this.disambiguationName = disambiguationName;
    }

    private ExtractValueFn(String fieldName) {
      this.fieldName = fieldName;
      this.disambiguationName = null;
    }

    @Override
    public String apply(JsonNode input) {
      String output = input.path(fieldName).asText();
      if (disambiguationName != null) {
        output += disambiguationName;
      }
      return output;
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

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein", null);
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
    String disambiguation = "testWriteWithIndexFn".toLowerCase();
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
                .withIndexFn(new ExtractValueFn("scientist", disambiguation)));
    pipeline.run();

    // verify counts on each index
    for (String scientist : FAMOUS_SCIENTISTS) {
      // Note that tests run in parallel, so without disambiguation multiple tests might be
      // interacting with the index named after a particular scientist
      String index = scientist.toLowerCase() + disambiguation;
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
   * in the configuration. Documents should be routed to a type of type_0 or type_1 using a modulo
   * approach of the explicit id.
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
    // Note that tests run in parallel, so without disambiguation multiple tests might be
    // interacting with the index named after a particular scientist
    String disambiguation = "testWriteWithFullAddressing".toLowerCase();

    int backendVersion = getBackendVersion(restClient);

    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withIdFn(new ExtractValueFn("id"))
            .withIndexFn(new ExtractValueFn("scientist", disambiguation));

    if (backendVersion <= 7) {
      write = write.withTypeFn(new Modulo2ValueFn("scientist"));
    }

    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    for (String scientist : FAMOUS_SCIENTISTS) {
      String index = scientist.toLowerCase() + disambiguation;
      for (int i = 0; i < 2; i++) {
        String type = backendVersion <= 7 ? "TYPE_" + scientist.hashCode() % 2 : null;
        long count = refreshIndexAndGetCurrentNumDocs(restClient, index, type, backendVersion);
        assertEquals("Incorrect count for " + index + "/" + type, numDocs / NUM_SCIENTISTS, count);
      }
    }
  }

  /**
   * Tests that documents are correctly routed when routingFn function is provided to overwrite the
   * defaults of using the configuration and auto-generation of the document IDs by Elasticsearch.
   * The scientist name is used for routing. As a result there should be numDocs/NUM_SCIENTISTS in
   * each index.
   */
  void testWriteWithRouting() throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline
        .apply(Create.of(data))
        .apply(
            ElasticsearchIO.write()
                .withConnectionConfiguration(connectionConfiguration)
                .withRoutingFn(new ExtractValueFn("scientist")));
    pipeline.run();

    flushAndRefreshAllIndices(restClient);
    for (String scientist : FAMOUS_SCIENTISTS) {
      Map<String, String> urlParams = Collections.singletonMap("routing", scientist);

      assertEquals(
          numDocs / NUM_SCIENTISTS,
          countByScientistName(connectionConfiguration, restClient, scientist, urlParams));
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
        countByScientistName(connectionConfiguration, restClient, "Einstein", null));

    // Partial update assertions
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "0", null, null));
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "1", null, null));
  }

  void testWriteWithDocVersion() throws Exception {
    List<ObjectNode> jsonData =
        ElasticsearchIOTestUtils.createJsonDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

    List<String> data = new ArrayList<>();
    for (ObjectNode doc : jsonData) {
      doc.put("my_version", "1");
      data.add(doc.toString());
    }

    insertTestDocuments(connectionConfiguration, data, restClient);
    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);
    // Check that all docs have the same "my_version"
    assertEquals(
        numDocs,
        countByMatch(
            connectionConfiguration, restClient, "my_version", "1", null, KV.of(1, numDocs)));

    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withIdFn(new ExtractValueFn("id"))
            .withDocVersionFn(new ExtractValueFn("my_version"))
            .withDocVersionType("external");

    data = new ArrayList<>();
    for (ObjectNode doc : jsonData) {
      // Set version to larger number than originally set, and larger than next logical version
      // number set by default by ES.
      doc.put("my_version", "3");
      data.add(doc.toString());
    }

    // Test that documents with lower version are rejected, but rejections ignored when specified
    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    // my_version and doc version should have changed
    assertEquals(
        0,
        countByMatch(
            connectionConfiguration, restClient, "my_version", "1", null, KV.of(1, numDocs)));
    assertEquals(
        numDocs,
        countByMatch(
            connectionConfiguration, restClient, "my_version", "3", null, KV.of(3, numDocs)));
  }

  /**
   * Tests upsert script by adding a group field to each document in the standard test set. The
   * group field is populated as the modulo 2 of the document id allowing for a test to ensure the
   * documents are split into 2 groups.
   */
  void testWriteScriptedUpsert() throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withIdFn(new ExtractValueFn("id"))
            .withUpsertScript(SCRIPT_SOURCE);

    // Test that documents can be inserted/created by using withUpsertScript
    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    // defensive coding to ensure our initial state is as expected
    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    // check we have not unwittingly modified existing behaviour
    assertEquals(numDocs, currentNumDocs);
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restClient, "Einstein", null));

    // All docs should have have group = 0 added by the script upon creation
    assertEquals(
        numDocs, countByMatch(connectionConfiguration, restClient, "group", "0", null, null));

    // Run the same data again. This time, because all docs exist in the index already, scripted
    // updates should happen rather than scripted inserts.
    pipeline.apply(Create.of(data)).apply(write);
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(numDocs, currentNumDocs);
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restClient, "Einstein", null));

    // The script will set either 0 or 1 for the group value on update operations
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "0", null, null));
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restClient, "group", "1", null, null));
  }

  void testMaxParallelRequestsPerWindow() throws Exception {
    List<Document> data =
        ElasticsearchIOTestUtils.createDocuments(
                numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS)
            .stream()
            .map(doc -> Document.create().withInputDoc(doc).withTimestamp(Instant.now()))
            .collect(Collectors.toList());

    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withMaxParallelRequestsPerWindow(1);

    PCollection<KV<Integer, Iterable<Document>>> batches =
        pipeline.apply(Create.of(data)).apply(StatefulBatching.fromSpec(write.getBulkIO()));

    PCollection<Integer> keyValues =
        batches.apply(
            MapElements.into(integers())
                .via((SerializableFunction<KV<Integer, Iterable<Document>>, Integer>) KV::getKey));

    // Number of unique keys produced should be number of maxParallelRequestsPerWindow * numWindows
    // There is only 1 request (key) per window, and 1 (global) window ie. one key total where
    // key value is 0
    PAssert.that(keyValues).containsInAnyOrder(0);

    PAssert.that(batches).satisfies(new AssertThatHasExpectedContents(0, data));

    pipeline.run();
  }

  void testDocumentCoder() throws Exception {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(numDocs, InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

    int randomNum = ThreadLocalRandom.current().nextInt(0, data.size());
    Instant now = Instant.now();
    Write write = ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration);
    Document expected =
        serializeDocs(write, data)
            .get(randomNum)
            .withTimestamp(now)
            .withHasError(randomNum % 2 == 0);

    PipedInputStream in = new PipedInputStream();
    PipedOutputStream out = new PipedOutputStream(in);
    DocumentCoder coder = DocumentCoder.of();
    coder.encode(expected, out);
    Document actual = coder.decode(in);

    assertEquals(expected, actual);
  }

  void testPipelineDone() throws Exception {
    Write write = ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration);
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);
    pipeline.apply(Create.of(data)).apply(write);

    assertEquals(State.DONE, pipeline.run().waitUntilFinish());
  }

  private static class AssertThatHasExpectedContents
      implements SerializableFunction<Iterable<KV<Integer, Iterable<Document>>>, Void> {

    private final int key;
    private final List<Document> expectedContents;

    AssertThatHasExpectedContents(int key, List<Document> expected) {
      this.key = key;
      this.expectedContents = expected;
    }

    @Override
    public Void apply(Iterable<KV<Integer, Iterable<Document>>> actual) {
      assertThat(
          actual,
          IsIterableContainingInAnyOrder.containsInAnyOrder(
              KvMatcher.isKv(
                  is(key),
                  IsIterableContainingInAnyOrder.containsInAnyOrder(expectedContents.toArray()))));
      return null;
    }
  }

  public static class KvMatcher<K, V> extends TypeSafeMatcher<KV<? extends K, ? extends V>> {
    final Matcher<? super K> keyMatcher;
    final Matcher<? super V> valueMatcher;

    public static <K, V> KvMatcher<K, V> isKv(Matcher<K> keyMatcher, Matcher<V> valueMatcher) {
      return new KvMatcher<>(keyMatcher, valueMatcher);
    }

    public KvMatcher(Matcher<? super K> keyMatcher, Matcher<? super V> valueMatcher) {
      this.keyMatcher = keyMatcher;
      this.valueMatcher = valueMatcher;
    }

    @Override
    public boolean matchesSafely(KV<? extends K, ? extends V> kv) {
      return keyMatcher.matches(kv.getKey()) && valueMatcher.matches(kv.getValue());
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("a KV(")
          .appendValue(keyMatcher)
          .appendText(", ")
          .appendValue(valueMatcher)
          .appendText(")");
    }
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
    HttpEntity entity1, entity2;

    if (getBackendVersion(restClient) > 7) {
      entity1 = new NStringEntity(BAD_REQUEST_NO_TYPE, ContentType.APPLICATION_JSON);
      entity2 = new NStringEntity(OK_REQUEST_NO_TYPE, ContentType.APPLICATION_JSON);
    } else {
      entity1 = new NStringEntity(BAD_REQUEST, ContentType.APPLICATION_JSON);
      entity2 = new NStringEntity(OK_REQUEST, ContentType.APPLICATION_JSON);
    }

    Request request = new Request("POST", "/_bulk");
    request.addParameters(Collections.emptyMap());
    request.setEntity(entity1);
    Response response1 = restClient.performRequest(request);
    assertTrue(CUSTOM_RETRY_PREDICATE.test(response1.getEntity()));

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
        String.format(ElasticsearchIO.BulkIO.RETRY_FAILED_LOG, EXPECTED_RETRIES));

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

  void testWriteAppendOnly() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withIdFn(new ExtractValueFn("id"))
            .withAppendOnly(true);
    executeWriteTest(write);
  }

  void testWriteAppendOnlyDeleteNotAllowed() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withIdFn(new ExtractValueFn("id"))
            .withAppendOnly(true)
            .withIsDeleteFn(doc -> true);
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

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein", null);
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
      data.add(String.format("{\"id\" : %s, \"is_deleted\": %b}", i, i % 2 != 0));
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
        countByScientistName(connectionConfiguration, restClient, "Einstein", null));

    // Check if documents are deleted as expected
    assertEquals(numDocs / 2, currentNumDocs);
    assertEquals(0, countByScientistName(connectionConfiguration, restClient, "Darwin", null));
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
      if (i % 2 != 0) {
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
        countByScientistName(connectionConfiguration, restClient, "Einstein", null));

    // Check if documents are deleted as expected
    assertEquals(numDocs / 2, currentNumDocs);
    assertEquals(0, countByScientistName(connectionConfiguration, restClient, "Darwin", null));
  }

  void testValidSSLAndUsernameConfiguration(String filePath) throws Exception {
    ConnectionConfiguration configWithSsl =
        connectionConfiguration
            .withUsername("username")
            .withPassword("password")
            .withKeystorePassword("qwerty")
            .withKeystorePath(filePath);

    RestClient restClient = configWithSsl.createClient();
    Assert.assertNotNull("rest client should not be null", restClient);
  }

  void testWriteWindowPreservation() throws IOException {
    List<String> data =
        ElasticsearchIOTestUtils.createDocuments(
            numDocs, ElasticsearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS);

    int step = 1;
    int windowSize = 5;
    long numExpectedWindows = numDocs / windowSize;
    Duration windowDuration = Duration.standardSeconds(windowSize);
    Duration stepDuration = Duration.standardSeconds(step);
    Duration offset = Duration.ZERO;
    TestStream.Builder<String> docsBuilder =
        TestStream.create(StringUtf8Coder.of()).advanceWatermarkTo(new Instant(0));

    for (String doc : data) {
      docsBuilder = docsBuilder.addElements(TimestampedValue.of(doc, new Instant(0).plus(offset)));
      offset = offset.plus(stepDuration);
    }

    TestStream<String> docs = docsBuilder.advanceWatermarkToInfinity();

    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withUseStatefulBatches(true)
            // Ensure that max batch will be hit before all buffered elements from
            // GroupIntoBatches are processed in a single bundle so that both ProcessContext and
            // FinishBundleContext flush code paths are exercised
            .withMaxBatchSize(numDocs / 2);

    PCollection<Document> successfulWrites =
        pipeline
            .apply(docs)
            .apply(Window.into(FixedWindows.of(windowDuration)))
            .apply(write)
            .get(Write.SUCCESSFUL_WRITES);

    for (int i = 0; i < numExpectedWindows; i++) {
      PAssert.that(successfulWrites)
          .inWindow(
              new IntervalWindow(
                  new Instant(0).plus(windowDuration.multipliedBy(i)),
                  new Instant(0).plus(windowDuration.multipliedBy(i + 1))))
          .satisfies(
              windowPreservationValidator(windowSize, i * windowSize, (i + 1) * windowSize - 1));
    }

    pipeline.run();

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restClient);
    assertEquals(numDocs, currentNumDocs);

    int count = countByScientistName(connectionConfiguration, restClient, "Einstein", null);
    assertEquals(numDocs / NUM_SCIENTISTS, count);
  }

  SerializableFunction<Iterable<Document>, Void> windowPreservationValidator(
      int expectedNumDocs, int idRangeStart, int idRangeEnd) {
    JsonMapper mapper = new JsonMapper();
    Set<Integer> range =
        IntStream.rangeClosed(idRangeStart, idRangeEnd).boxed().collect(Collectors.toSet());

    return input -> {
      assertEquals(expectedNumDocs, Iterables.size(input));

      for (Document d : input) {
        try {
          ObjectNode doc = (ObjectNode) mapper.readTree(d.getInputDoc());
          assertTrue(range.contains(doc.get("id").asInt()));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    };
  }

  SerializableFunction<Iterable<Document>, Void> responseItemJsonSubstringValidator(
      String responseItemSubstring) {
    return input -> {
      for (Document d : input) {
        assertTrue(d.getResponseItemJson().contains(responseItemSubstring));
      }
      return null;
    };
  }
}
