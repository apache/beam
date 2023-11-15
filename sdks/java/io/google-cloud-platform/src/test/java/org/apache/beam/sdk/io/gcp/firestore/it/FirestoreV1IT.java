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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldFilter;
import com.google.firestore.v1.StructuredQuery.FieldFilter.Operator;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Filter;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public final class FirestoreV1IT extends BaseFirestoreIT {

  @Test
  public void batchWrite_partialFailureOutputsToDeadLetterQueue()
      throws InterruptedException, ExecutionException, TimeoutException {
    String collectionId = "a";

    String docId = helper.docId();
    Write validWrite =
        Write.newBuilder()
            .setUpdate(
                Document.newBuilder()
                    .setName(docPath(helper.getBaseDocumentPath(), collectionId, docId))
                    .putFields("foo", Value.newBuilder().setStringValue(docId).build()))
            .build();

    long millis = System.currentTimeMillis();
    Timestamp timestamp =
        Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1000000))
            .build();

    String docId2 = helper.docId();
    helper
        .getBaseDocument()
        .collection(collectionId)
        .document(docId2)
        .create(ImmutableMap.of("foo", "baz"))
        .get(10, TimeUnit.SECONDS);

    // this will fail because we're setting a updateTime precondition to before it was created
    Write conditionalUpdate =
        Write.newBuilder()
            .setUpdate(
                Document.newBuilder()
                    .setName(docPath(helper.getBaseDocumentPath(), collectionId, docId2))
                    .putFields("foo", Value.newBuilder().setStringValue(docId).build()))
            .setCurrentDocument(Precondition.newBuilder().setUpdateTime(timestamp))
            .build();

    List<Write> writes = newArrayList(validWrite, conditionalUpdate);

    RpcQosOptions options =
        BaseFirestoreIT.RPC_QOS_OPTIONS.toBuilder().withBatchMaxCount(2).build();
    PCollection<WriteFailure> writeFailurePCollection =
        testPipeline
            .apply(Create.of(writes))
            .apply(
                FirestoreIO.v1()
                    .write()
                    .batchWrite()
                    .withDeadLetterQueue()
                    .withRpcQosOptions(options)
                    .build());

    PAssert.that(writeFailurePCollection)
        .satisfies(
            (writeFailures) -> {
              Iterator<WriteFailure> iterator = writeFailures.iterator();
              assertTrue(iterator.hasNext());
              WriteFailure failure = iterator.next();
              assertEquals(Code.FAILED_PRECONDITION, Code.forNumber(failure.getStatus().getCode()));
              assertNotNull(failure.getWriteResult());
              assertFalse(failure.getWriteResult().hasUpdateTime());
              assertEquals(conditionalUpdate, failure.getWrite());
              assertFalse(iterator.hasNext());
              return null;
            });
    testPipeline.run(TestPipeline.testingPipelineOptions());

    ApiFuture<QuerySnapshot> actualDocsQuery =
        helper.getBaseDocument().collection(collectionId).orderBy("__name__").get();
    QuerySnapshot querySnapshot = actualDocsQuery.get(10, TimeUnit.SECONDS);
    List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();
    List<KV<String, String>> actualDocumentIds =
        documents.stream()
            .map(doc -> KV.of(doc.getId(), doc.getString("foo")))
            .collect(Collectors.toList());

    List<KV<String, String>> expected = newArrayList(KV.of(docId, docId), KV.of(docId2, "baz"));
    assertEquals(expected, actualDocumentIds);
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<ListCollectionIdsRequest>>
      getListCollectionIdsPTransform(String testMethodName) {
    return new ListCollectionIds(helper.getDatabase(), helper.getBaseDocumentPath());
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<ListDocumentsRequest>>
      getListDocumentsPTransform(String testMethodName) {
    return new ListDocuments(helper.getDatabase(), helper.getBaseDocumentPath());
  }

  @Override
  protected PTransform<PCollection<List<String>>, PCollection<BatchGetDocumentsRequest>>
      getBatchGetDocumentsPTransform(String testMethodName, String collectionId) {
    return new BatchGetDocuments(helper.getDatabase(), helper.getBaseDocumentPath(), collectionId);
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<RunQueryRequest>> getRunQueryPTransform(
      String testMethodName) {
    return new RunQuery(helper.getDatabase(), helper.getBaseDocumentPath());
  }

  @Override
  protected PTransform<PCollection<String>, PCollection<PartitionQueryRequest>>
      getPartitionQueryPTransform(String testMethodName, int partitionCount) {
    return new PartitionQuery(helper.getDatabase(), helper.getDocumentRoot(), partitionCount);
  }

  @Override
  protected PTransform<PCollection<List<String>>, PCollection<Write>> getWritePTransform(
      String testMethodName, String collectionId) {
    return new WritePTransform(helper.getDatabase(), helper.getBaseDocumentPath(), collectionId);
  }

  private static final class ListCollectionIds
      extends BasePTransform<String, ListCollectionIdsRequest> {

    public ListCollectionIds(String database, String baseDocumentPath) {
      super(database, baseDocumentPath, "");
    }

    @Override
    public PCollection<ListCollectionIdsRequest> expand(PCollection<String> input) {
      return input.apply(
          ParDo.of(
              new DoFn<String, ListCollectionIdsRequest>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(
                      ListCollectionIdsRequest.newBuilder().setParent(baseDocumentPath).build());
                }
              }));
    }
  }

  private static final class ListDocuments extends BasePTransform<String, ListDocumentsRequest> {

    public ListDocuments(String database, String baseDocumentPath) {
      super(database, baseDocumentPath, "");
    }

    @Override
    public PCollection<ListDocumentsRequest> expand(PCollection<String> input) {
      return input.apply(
          ParDo.of(
              new DoFn<String, ListDocumentsRequest>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(
                      ListDocumentsRequest.newBuilder()
                          .setParent(baseDocumentPath)
                          .setCollectionId(c.element())
                          .build());
                }
              }));
    }
  }

  private static final class BatchGetDocuments
      extends BasePTransform<List<String>, BatchGetDocumentsRequest> {

    public BatchGetDocuments(String database, String baseDocumentPath, String collectionId) {
      super(database, baseDocumentPath, collectionId);
    }

    @Override
    public PCollection<BatchGetDocumentsRequest> expand(PCollection<List<String>> input) {
      return input.apply(
          ParDo.of(
              new DoFn<List<String>, BatchGetDocumentsRequest>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  BatchGetDocumentsRequest.Builder builder =
                      BatchGetDocumentsRequest.newBuilder().setDatabase(database);
                  builder.addDocuments(docPath("404"));
                  c.element().stream().map(docId -> docPath(docId)).forEach(builder::addDocuments);
                  c.output(builder.build());
                }
              }));
    }
  }

  private static final class RunQuery extends BasePTransform<String, RunQueryRequest> {

    public RunQuery(String database, String baseDocumentPath) {
      super(database, baseDocumentPath, "");
    }

    @Override
    public PCollection<RunQueryRequest> expand(PCollection<String> input) {
      return input.apply(
          ParDo.of(
              new DoFn<String, RunQueryRequest>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(
                      RunQueryRequest.newBuilder()
                          .setParent(baseDocumentPath)
                          .setStructuredQuery(
                              StructuredQuery.newBuilder()
                                  .addFrom(
                                      CollectionSelector.newBuilder().setCollectionId(c.element()))
                                  .setWhere(
                                      Filter.newBuilder()
                                          .setFieldFilter(
                                              FieldFilter.newBuilder()
                                                  .setField(
                                                      FieldReference.newBuilder()
                                                          .setFieldPath("foo"))
                                                  .setOp(Operator.EQUAL)
                                                  .setValue(
                                                      Value.newBuilder().setStringValue("bar")))))
                          .build());
                }
              }));
    }
  }

  private static final class PartitionQuery extends BasePTransform<String, PartitionQueryRequest> {

    private final int partitionCount;

    public PartitionQuery(String database, String baseDocumentPath, int partitionCount) {
      super(database, baseDocumentPath, "");
      this.partitionCount = partitionCount;
    }

    @Override
    public PCollection<PartitionQueryRequest> expand(PCollection<String> input) {
      return input.apply(
          ParDo.of(
              new DoFn<String, PartitionQueryRequest>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  c.output(
                      PartitionQueryRequest.newBuilder()
                          .setParent(baseDocumentPath)
                          // set the page size smaller than the number of partitions we're
                          // requesting to ensure
                          // that more than one page is to be fetched so that our multi-page
                          // handling code is
                          // tested.
                          .setPageSize(1)
                          .setPartitionCount(partitionCount)
                          .setStructuredQuery(
                              StructuredQuery.newBuilder()
                                  .addFrom(
                                      CollectionSelector.newBuilder()
                                          .setCollectionId(c.element())
                                          .setAllDescendants(true))
                                  .addOrderBy(
                                      Order.newBuilder()
                                          .setField(
                                              FieldReference.newBuilder().setFieldPath("__name__"))
                                          .setDirection(Direction.ASCENDING)))
                          .build());
                }
              }));
    }
  }

  private static final class WritePTransform extends BasePTransform<List<String>, Write> {

    public WritePTransform(String database, String baseDocumentPath, String collectionId) {
      super(database, baseDocumentPath, collectionId);
    }

    @Override
    public PCollection<Write> expand(PCollection<List<String>> input) {
      return input.apply(
          ParDo.of(
              new DoFn<List<String>, Write>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  List<String> documentIds = c.element();
                  documentIds.stream()
                      .map(
                          docId ->
                              Write.newBuilder()
                                  .setUpdate(
                                      Document.newBuilder()
                                          .setName(docPath(docId))
                                          .putFields(
                                              "foo",
                                              Value.newBuilder().setStringValue(docId).build()))
                                  .build())
                      .forEach(c::output);
                }
              }));
    }
  }

  private abstract static class BasePTransform<InT, OutT>
      extends PTransform<PCollection<InT>, PCollection<OutT>> {

    protected final String database;
    protected final String baseDocumentPath;
    protected final String collectionId;

    private BasePTransform(String database, String baseDocumentPath, String collectionId) {
      this.database = database;
      this.baseDocumentPath = baseDocumentPath;
      this.collectionId = collectionId;
    }

    protected String docPath(String docId) {
      return FirestoreV1IT.docPath(baseDocumentPath, collectionId, docId);
    }
  }

  private static String docPath(String baseDocumentPath, String collectionId, String docId) {
    return String.format("%s/%s/%s", baseDocumentPath, collectionId, docId);
  }
}
