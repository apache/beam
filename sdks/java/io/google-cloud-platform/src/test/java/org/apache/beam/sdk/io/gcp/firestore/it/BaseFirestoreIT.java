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
package org.apache.beam.sdk.io.gcp.firestore.it;

import static org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.chunkUpDocIds;
import static org.junit.Assert.assertEquals;

import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.Write;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.CleanupMode;
import org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.DataLayout;
import org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.DocumentGenerator;
import org.apache.beam.sdk.io.gcp.firestore.it.FirestoreTestingHelper.TestDataLayoutHint;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

@SuppressWarnings({
  "initialization.fields.uninitialized",
  "initialization.static.fields.uninitialized"
}) // fields are managed via #beforeClass & #setup
abstract class BaseFirestoreIT {

  protected static final int NUM_ITEMS_TO_GENERATE =
      768; // more than one read page and one write page

  @Rule(order = 1)
  public final TestName testName = new TestName();

  @Rule(order = 2)
  public final FirestoreTestingHelper helper = new FirestoreTestingHelper(CleanupMode.ALWAYS);

  @Rule(order = 3)
  public final TestPipeline testPipeline = TestPipeline.create();

  @Rule(order = 4)
  public final TestPipeline testPipeline2 = TestPipeline.create();

  protected static final RpcQosOptions RPC_QOS_OPTIONS =
      RpcQosOptions.defaultOptions()
          .toBuilder()
          .withMaxAttempts(1)
          .withHintMaxNumWorkers(1)
          .build();

  protected static String project;

  @Before
  public void setup() {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  }

  private static Instant toWriteTime(WriteResult result) {
    return Instant.ofEpochMilli(
        result.getUpdateTime().getSeconds() * 1000
            + result.getUpdateTime().getNanos() / 1000000
            // updateTime is microseconds precision, joda Instant is milliseconds precision. We want
            // to return an Instant that's no less than updateTime.
            + 1 * (result.getUpdateTime().getNanos() % 1000000 == 0 ? 0 : 1));
  }

  private static Instant toMaxWriteTime(List<WriteResult> results) {
    return results.stream().map(BaseFirestoreIT::toWriteTime).max(Instant::compareTo).get();
  }

  private Instant writeBatches(List<String> collectionIds) throws Exception {
    List<WriteResult> results =
        ApiFutures.transform(
                ApiFutures.allAsList(
                    chunkUpDocIds(collectionIds)
                        .map(
                            chunk -> {
                              WriteBatch batch = helper.getFs().batch();
                              chunk.stream()
                                  .map(col -> helper.getBaseDocument().collection(col).document())
                                  .forEach(ref -> batch.set(ref, ImmutableMap.of("foo", "bar")));
                              return batch.commit();
                            })
                        .collect(Collectors.toList())),
                FirestoreTestingHelper.flattenListList(),
                MoreExecutors.directExecutor())
            .get(10, TimeUnit.SECONDS);
    return toMaxWriteTime(results);
  }

  @Test
  @TestDataLayoutHint(DataLayout.Deep)
  public final void listCollections() throws Exception {
    // verification and cleanup of nested collections is much slower because each document
    // requires an rpc to find its collections, instead of using the usual size, use 20
    // to keep the test quick
    List<String> collectionIds =
        IntStream.rangeClosed(1, 20).mapToObj(i -> helper.colId()).collect(Collectors.toList());
    Instant readTime = writeBatches(collectionIds);
    Thread.sleep(5);

    List<String> moreCollectionIds =
        IntStream.rangeClosed(21, 30).mapToObj(i -> helper.colId()).collect(Collectors.toList());
    writeBatches(moreCollectionIds);

    List<String> allCollectionIds =
        Stream.of(collectionIds, moreCollectionIds)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    // Reading from 'current' should get all collection IDs written in both batches
    PCollection<String> actualCollectionIds =
        testPipeline
            .apply(Create.of(""))
            .apply(getListCollectionIdsPTransform(testName.getMethodName()))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .listCollectionIds()
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build());

    PAssert.that(actualCollectionIds).containsInAnyOrder(allCollectionIds);
    testPipeline.run(TestPipeline.testingPipelineOptions());

    // Reading from readTime should only get collection IDs written in the batch before readTime.
    PCollection<String> actualCollectionIdsAtReadTime =
        testPipeline2
            .apply(Create.of(""))
            .apply(getListCollectionIdsPTransform(testName.getMethodName()))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .listCollectionIds()
                    .withReadTime(readTime)
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build());
    PAssert.that(actualCollectionIdsAtReadTime).containsInAnyOrder(collectionIds);
    testPipeline2.run(TestPipeline.testingPipelineOptions());
  }

  @Test
  public final void listDocuments() throws Exception {
    DocumentGenerator documentGenerator = helper.documentGenerator(NUM_ITEMS_TO_GENERATE, "a");
    Instant readTime =
        toMaxWriteTime(documentGenerator.generateDocuments().get(10, TimeUnit.SECONDS));
    Thread.sleep(5);

    DocumentGenerator moreDocumentGenerator =
        helper.documentGenerator(NUM_ITEMS_TO_GENERATE + 1, NUM_ITEMS_TO_GENERATE + 10, "a");
    moreDocumentGenerator.generateDocuments().get(10, TimeUnit.SECONDS);

    List<String> allDocumentPaths =
        Stream.of(
                documentGenerator.expectedDocumentPaths(),
                moreDocumentGenerator.expectedDocumentPaths())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    // Reading from 'current' should get all the documents written.
    PCollection<String> listDocumentPaths =
        testPipeline
            .apply(Create.of("a"))
            .apply(getListDocumentsPTransform(testName.getMethodName()))
            .apply(
                FirestoreIO.v1().read().listDocuments().withRpcQosOptions(RPC_QOS_OPTIONS).build())
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPaths).containsInAnyOrder(allDocumentPaths);
    testPipeline.run(TestPipeline.testingPipelineOptions());

    // Reading from readTime should only get the documents written before readTime.
    PCollection<String> listDocumentPathsAtReadTime =
        testPipeline2
            .apply(Create.of("a"))
            .apply(getListDocumentsPTransform(testName.getMethodName()))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .listDocuments()
                    .withReadTime(readTime)
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build())
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPathsAtReadTime)
        .containsInAnyOrder(documentGenerator.expectedDocumentPaths());
    testPipeline2.run(TestPipeline.testingPipelineOptions());
  }

  @Test
  public final void runQuery() throws Exception {
    String collectionId = "a";
    DocumentGenerator documentGenerator =
        helper.documentGenerator(NUM_ITEMS_TO_GENERATE, collectionId, /* addBazDoc = */ true);
    Instant readTime =
        toMaxWriteTime(documentGenerator.generateDocuments().get(10, TimeUnit.SECONDS));
    Thread.sleep(5);

    DocumentGenerator moreDocumentGenerator =
        helper.documentGenerator(
            NUM_ITEMS_TO_GENERATE + 1, NUM_ITEMS_TO_GENERATE + 10, collectionId, true);
    moreDocumentGenerator.generateDocuments().get(10, TimeUnit.SECONDS);

    List<String> allDocumentPaths =
        Stream.of(
                documentGenerator.expectedDocumentPaths(),
                moreDocumentGenerator.expectedDocumentPaths())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    // Reading from 'current' should get all the documents written.
    PCollection<String> listDocumentPaths =
        testPipeline
            .apply(Create.of(collectionId))
            .apply(getRunQueryPTransform(testName.getMethodName()))
            .apply(FirestoreIO.v1().read().runQuery().withRpcQosOptions(RPC_QOS_OPTIONS).build())
            .apply(ParDo.of(new RunQueryResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPaths).containsInAnyOrder(allDocumentPaths);
    testPipeline.run(TestPipeline.testingPipelineOptions());

    // Reading from readTime should only get the documents written before readTime.
    PCollection<String> listDocumentPathsAtReadTime =
        testPipeline2
            .apply(Create.of(collectionId))
            .apply(getRunQueryPTransform(testName.getMethodName()))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .runQuery()
                    .withReadTime(readTime)
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build())
            .apply(ParDo.of(new RunQueryResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPathsAtReadTime)
        .containsInAnyOrder(documentGenerator.expectedDocumentPaths());
    testPipeline2.run(TestPipeline.testingPipelineOptions());
  }

  @Test
  public final void partitionQuery() throws Exception {
    String collectionGroupId = UUID.randomUUID().toString();
    // currently firestore will only generate a partition every 128 documents, so generate enough
    // documents to get 2 cursors returned, resulting in 3 partitions
    int partitionCount = 3;
    int documentCount = (partitionCount * 128) - 1;

    // create some documents for listing and asserting in the test
    DocumentGenerator documentGenerator =
        helper.documentGenerator(documentCount, collectionGroupId);
    Instant readTime =
        toMaxWriteTime(documentGenerator.generateDocuments().get(10, TimeUnit.SECONDS));
    Thread.sleep(5);

    DocumentGenerator moreDocumentGenerator =
        helper.documentGenerator(documentCount + 1, documentCount + 2 * 128, collectionGroupId);
    moreDocumentGenerator.generateDocuments().get(10, TimeUnit.SECONDS);

    List<String> allDocumentPaths =
        Stream.of(
                documentGenerator.expectedDocumentPaths(),
                moreDocumentGenerator.expectedDocumentPaths())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    // Reading from 'current' should get all the documents written.
    PCollection<String> listDocumentPaths =
        testPipeline
            .apply(Create.of(collectionGroupId))
            .apply(getPartitionQueryPTransform(testName.getMethodName(), partitionCount))
            .apply(FirestoreIO.v1().read().partitionQuery().withNameOnlyQuery().build())
            .apply(FirestoreIO.v1().read().runQuery().build())
            .apply(ParDo.of(new RunQueryResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPaths).containsInAnyOrder(allDocumentPaths);
    testPipeline.run(TestPipeline.testingPipelineOptions());

    // Reading from readTime should only get the documents written before readTime.
    PCollection<String> listDocumentPathsAtReadTime =
        testPipeline2
            .apply(Create.of(collectionGroupId))
            .apply(getPartitionQueryPTransform(testName.getMethodName(), partitionCount))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .partitionQuery()
                    .withReadTime(readTime)
                    .withNameOnlyQuery()
                    .build())
            .apply(FirestoreIO.v1().read().runQuery().withReadTime(readTime).build())
            .apply(ParDo.of(new RunQueryResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPathsAtReadTime)
        .containsInAnyOrder(documentGenerator.expectedDocumentPaths());
    testPipeline2.run(TestPipeline.testingPipelineOptions());
  }

  @Test
  public final void batchGet() throws Exception {
    String collectionId = "a";
    DocumentGenerator documentGenerator =
        helper.documentGenerator(NUM_ITEMS_TO_GENERATE, collectionId);
    Instant readTime =
        toMaxWriteTime(documentGenerator.generateDocuments().get(10, TimeUnit.SECONDS));

    DocumentGenerator moreDocumentGenerator =
        helper.documentGenerator(
            NUM_ITEMS_TO_GENERATE + 1, NUM_ITEMS_TO_GENERATE + 10, collectionId);
    moreDocumentGenerator.generateDocuments().get(10, TimeUnit.SECONDS);

    List<String> allDocumentIds =
        Stream.of(documentGenerator.getDocumentIds(), moreDocumentGenerator.getDocumentIds())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    List<String> allDocumentPaths =
        Stream.of(
                documentGenerator.expectedDocumentPaths(),
                moreDocumentGenerator.expectedDocumentPaths())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    // Reading from 'current' should get all the documents written.
    PCollection<String> listDocumentPaths =
        testPipeline
            .apply(Create.of(Collections.singletonList(allDocumentIds)))
            .apply(getBatchGetDocumentsPTransform(testName.getMethodName(), collectionId))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .batchGetDocuments()
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build())
            .apply(Filter.by(BatchGetDocumentsResponse::hasFound))
            .apply(ParDo.of(new BatchGetDocumentsResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPaths).containsInAnyOrder(allDocumentPaths);
    testPipeline.run(TestPipeline.testingPipelineOptions());

    // Reading from readTime should only get the documents written before readTime.
    PCollection<String> listDocumentPathsAtReadTime =
        testPipeline2
            .apply(Create.of(Collections.singletonList(allDocumentIds)))
            .apply(getBatchGetDocumentsPTransform(testName.getMethodName(), collectionId))
            .apply(
                FirestoreIO.v1()
                    .read()
                    .batchGetDocuments()
                    .withReadTime(readTime)
                    .withRpcQosOptions(RPC_QOS_OPTIONS)
                    .build())
            .apply(Filter.by(BatchGetDocumentsResponse::hasFound))
            .apply(ParDo.of(new BatchGetDocumentsResponseToDocument()))
            .apply(ParDo.of(new DocumentToName()));

    PAssert.that(listDocumentPathsAtReadTime)
        .containsInAnyOrder(documentGenerator.expectedDocumentPaths());
    testPipeline2.run(TestPipeline.testingPipelineOptions());
  }

  @Test
  public final void write() {
    String collectionId = "a";
    runWriteTest(getWritePTransform(testName.getMethodName(), collectionId), collectionId);
  }

  protected abstract PTransform<PCollection<String>, PCollection<ListCollectionIdsRequest>>
      getListCollectionIdsPTransform(String testMethodName);

  protected abstract PTransform<PCollection<String>, PCollection<ListDocumentsRequest>>
      getListDocumentsPTransform(String testMethodName);

  protected abstract PTransform<PCollection<List<String>>, PCollection<BatchGetDocumentsRequest>>
      getBatchGetDocumentsPTransform(String testMethodName, String collectionId);

  protected abstract PTransform<PCollection<String>, PCollection<RunQueryRequest>>
      getRunQueryPTransform(String testMethodName);

  protected abstract PTransform<PCollection<String>, PCollection<PartitionQueryRequest>>
      getPartitionQueryPTransform(String testMethodName, int partitionCount);

  protected abstract PTransform<PCollection<List<String>>, PCollection<Write>> getWritePTransform(
      String testMethodName, String collectionId);

  protected final void runWriteTest(
      PTransform<PCollection<List<String>>, PCollection<Write>> createWrite, String collectionId) {
    List<String> documentIds =
        IntStream.rangeClosed(1, 1_000).mapToObj(i -> helper.docId()).collect(Collectors.toList());

    // Create.of unwraps the list of document ids, so wrap it in another list
    testPipeline
        .apply(Create.of(Collections.singletonList(documentIds)))
        .apply(createWrite)
        .apply(FirestoreIO.v1().write().batchWrite().withRpcQosOptions(RPC_QOS_OPTIONS).build());

    testPipeline.run(TestPipeline.testingPipelineOptions());

    List<String> actualDocumentIds =
        helper
            .listDocumentsViaQuery(
                String.format("%s/%s", helper.getBaseDocumentPath(), collectionId))
            .map(name -> name.substring(name.lastIndexOf("/") + 1))
            .collect(Collectors.toList());

    assertEquals(documentIds, actualDocumentIds);
  }

  private static final class RunQueryResponseToDocument extends DoFn<RunQueryResponse, Document> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getDocument());
    }
  }

  private static final class BatchGetDocumentsResponseToDocument
      extends DoFn<BatchGetDocumentsResponse, Document> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getFound());
    }
  }

  private static final class DocumentToName extends DoFn<Document, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getName());
    }
  }
}
