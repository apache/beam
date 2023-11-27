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

import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.StructuredQuery.Projection;
import com.google.firestore.v1.Write;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FirestoreTestingHelper implements TestRule {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreTestingHelper.class);

  static final String BASE_COLLECTION_ID;

  static {
    Instant now = Clock.systemUTC().instant();
    DateTimeFormatter formatter =
        DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));
    BASE_COLLECTION_ID = formatter.format(now);
  }

  enum CleanupMode {
    ALWAYS,
    ON_SUCCESS_ONLY,
    NEVER
  }

  enum DataLayout {
    Shallow,
    Deep
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.METHOD)
  @interface TestDataLayoutHint {
    DataLayout value() default DataLayout.Shallow;
  }

  private final GcpOptions gcpOptions;
  private final org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions firestoreBeamOptions;
  private final FirestoreOptions firestoreOptions;

  private final Firestore fs;
  private final FirestoreRpc rpc;
  private final CleanupMode cleanupMode;

  private Class<?> testClass;
  private String testName;
  private DataLayout dataLayout;

  private int docIdCounter;
  private int colIdCounter;

  private boolean testSuccess = false;

  @SuppressWarnings(
      "initialization.fields.uninitialized") // testClass and testName are managed via #apply
  public FirestoreTestingHelper(CleanupMode cleanupMode) {
    this.cleanupMode = cleanupMode;
    gcpOptions = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    firestoreBeamOptions =
        TestPipeline.testingPipelineOptions()
            .as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class);
    firestoreOptions =
        FirestoreOptions.newBuilder()
            .setCredentials(gcpOptions.getGcpCredential())
            .setProjectId(gcpOptions.getProject())
            .setDatabaseId(firestoreBeamOptions.getFirestoreDb())
            .setHost(firestoreBeamOptions.getFirestoreHost())
            .build();
    fs = firestoreOptions.getService();
    rpc = (FirestoreRpc) firestoreOptions.getRpc();
  }

  @Override
  public Statement apply(@NonNull Statement base, Description description) {
    testClass = description.getTestClass();
    testName = description.getMethodName();
    TestDataLayoutHint hint = description.getAnnotation(TestDataLayoutHint.class);
    if (hint != null) {
      dataLayout = hint.value();
    } else {
      dataLayout = DataLayout.Shallow;
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          base.evaluate();
          testSuccess = true;
        } finally {
          if (cleanupMode == CleanupMode.ALWAYS
              || (cleanupMode == CleanupMode.ON_SUCCESS_ONLY && testSuccess)) {
            try {
              cleanUp(getBaseDocumentPath());
            } catch (Exception e) {
              if (LOG.isDebugEnabled() || LOG.isTraceEnabled()) {
                LOG.debug("Error while running cleanup", e);
              } else {
                LOG.info(
                    "Error while running cleanup: {} (set log level higher for stacktrace)",
                    e.getMessage() == null ? e.getClass().getName() : e.getMessage());
              }
            }
          }
        }
      }
    };
  }

  Firestore getFs() {
    return fs;
  }

  String getDatabase() {
    return String.format(
        "projects/%s/databases/%s",
        firestoreOptions.getProjectId(), firestoreOptions.getDatabaseId());
  }

  String getDocumentRoot() {
    return getDatabase() + "/documents";
  }

  DocumentReference getBaseDocument() {
    return getBaseDocument(fs, testClass, testName);
  }

  String getBaseDocumentPath() {
    return String.format("%s/%s", getDocumentRoot(), getBaseDocument().getPath());
  }

  String docId() {
    return id("doc", docIdCounter++);
  }

  String colId() {
    return id("col", colIdCounter++);
  }

  DocumentGenerator documentGenerator(int to, String collectionId) {
    return documentGenerator(to, collectionId, false);
  }

  DocumentGenerator documentGenerator(int from, int to, String collectionId) {
    return documentGenerator(from, to, collectionId, false);
  }

  DocumentGenerator documentGenerator(int to, String collectionId, boolean addBazDoc) {
    return documentGenerator(1, to, collectionId, addBazDoc);
  }

  DocumentGenerator documentGenerator(int from, int to, String collectionId, boolean addBazDoc) {
    return new DocumentGenerator(from, to, collectionId, addBazDoc);
  }

  Stream<String> listCollectionIds(String parent) {
    ListCollectionIdsRequest lcir = ListCollectionIdsRequest.newBuilder().setParent(parent).build();
    // LOGGER.debug("lcir = {}", lcir);

    ListCollectionIdsPagedResponse response = rpc.listCollectionIdsPagedCallable().call(lcir);
    return StreamSupport.stream(response.iteratePages().spliterator(), false)
        .flatMap(page -> StreamSupport.stream(page.getValues().spliterator(), false))
        .map(colId -> String.format("%s/%s", parent, colId));
  }

  Stream<String> listDocumentIds(String collectionPath) {
    int index = collectionPath.lastIndexOf('/');
    String parent = collectionPath.substring(0, index);
    String collectionId = collectionPath.substring(index + 1);
    ListDocumentsRequest ldr =
        ListDocumentsRequest.newBuilder()
            .setParent(parent)
            .setCollectionId(collectionId)
            .setShowMissing(true)
            .build();
    // LOGGER.debug("ldr = {}", ldr);

    ListDocumentsPagedResponse response = rpc.listDocumentsPagedCallable().call(ldr);
    return StreamSupport.stream(response.iteratePages().spliterator(), false)
        .flatMap(page -> page.getResponse().getDocumentsList().stream())
        .map(Document::getName)
        .filter(s -> !s.isEmpty());
  }

  Stream<String> listDocumentsViaQuery(String collectionPath) {
    int index = collectionPath.lastIndexOf('/');
    String parent = collectionPath.substring(0, index);
    String collectionId = collectionPath.substring(index + 1);
    FieldReference nameField = FieldReference.newBuilder().setFieldPath("__name__").build();
    RunQueryRequest rqr =
        RunQueryRequest.newBuilder()
            .setParent(parent)
            .setStructuredQuery(
                StructuredQuery.newBuilder()
                    .addFrom(CollectionSelector.newBuilder().setCollectionId(collectionId))
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(nameField)
                            .setDirection(Direction.ASCENDING)
                            .build())
                    .setSelect(Projection.newBuilder().addFields(nameField).build()))
            .build();
    // LOGGER.debug("rqr = {}", rqr);

    return StreamSupport.stream(rpc.runQueryCallable().call(rqr).spliterator(), false)
        .filter(RunQueryResponse::hasDocument)
        .map(RunQueryResponse::getDocument)
        .map(Document::getName);
  }

  private void cleanUp(String baseDocument) {
    LOG.debug("Running cleanup...");
    Batcher batcher = new Batcher();
    walkDoc(batcher, baseDocument);
    batcher.flush();
    LOG.debug("Running cleanup complete");
  }

  private void walkDoc(Batcher batcher, String baseDocument) {
    batcher.checkState();
    listCollectionIds(baseDocument).forEach(col -> walkCol(batcher, col));
    batcher.add(baseDocument);
  }

  private void walkCol(Batcher batcher, String baseCollection) {
    batcher.checkState();
    if (dataLayout == DataLayout.Shallow) {
      // queries are much faster for flat listing when we don't need to try and account for
      // `show_missing`
      listDocumentsViaQuery(baseCollection).forEach(batcher::add);
      // flush any pending writes before we start recursively walking down the tree
      batcher.flush();
    }
    listDocumentIds(baseCollection).forEach(doc -> walkDoc(batcher, doc));
  }

  static DocumentReference getBaseDocument(Firestore fs, Class<?> testClass, String testName) {
    return fs.collection("beam")
        .document("IT")
        .collection(BASE_COLLECTION_ID)
        .document(testClass.getSimpleName())
        .collection("test")
        .document(testName);
  }

  static <T> Stream<List<T>> chunkUpDocIds(List<T> things) {
    return Streams.stream(Iterables.partition(things, 500));
  }

  static <T> ApiFunction<List<List<T>>, List<T>> flattenListList() {
    return input -> {
      List<T> retVal = new ArrayList<>();
      for (List<T> writeResults : input) {
        retVal.addAll(writeResults);
      }
      return retVal;
    };
  }

  private static String id(String docOrCol, int counter) {
    return String.format("%s-%05d", docOrCol, counter);
  }

  final class DocumentGenerator {

    private final List<String> documentIds;
    private final String collectionId;
    private final boolean addBazDoc;

    private DocumentGenerator(int from, int to, String collectionId, boolean addBazDoc) {
      this.documentIds =
          Collections.unmodifiableList(
              IntStream.rangeClosed(from, to).mapToObj(i -> docId()).collect(Collectors.toList()));
      this.collectionId = collectionId;
      this.addBazDoc = addBazDoc;
    }

    ApiFuture<List<WriteResult>> generateDocuments() {
      // create some documents for listing and asserting in the test
      CollectionReference baseCollection = getBaseDocument().collection(collectionId);

      List<ApiFuture<List<WriteResult>>> futures =
          Streams.concat(
                  chunkUpDocIds(documentIds)
                      .map(
                          chunk -> {
                            WriteBatch batch = fs.batch();
                            chunk.stream()
                                .map(baseCollection::document)
                                .forEach(ref -> batch.set(ref, ImmutableMap.of("foo", "bar")));
                            return batch.commit();
                          }),
                  Stream.of(Optional.of(addBazDoc))
                      .filter(o -> o.filter(b -> b).isPresent())
                      .map(
                          x -> {
                            WriteBatch batch = fs.batch();
                            batch.set(baseCollection.document(), ImmutableMap.of("foo", "baz"));
                            return batch.commit();
                          }))
              .collect(Collectors.toList());

      return ApiFutures.transform(
          ApiFutures.allAsList(futures),
          FirestoreTestingHelper.flattenListList(),
          MoreExecutors.directExecutor());
    }

    List<String> getDocumentIds() {
      return documentIds;
    }

    List<String> expectedDocumentPaths() {
      return documentIds.stream()
          .map(
              id ->
                  String.format(
                      "%s/%s/%s",
                      getDocumentRoot(), getBaseDocument().collection(collectionId).getPath(), id))
          .collect(Collectors.toList());
    }
  }

  @SuppressWarnings({
    "nullness",
    "initialization.fields.uninitialized"
  }) // batch is managed as part of use
  private final class Batcher {
    private static final int MAX_BATCH_SIZE = 500;

    private final BatchWriteRequest.Builder batch;
    private boolean failed;

    public Batcher() {
      batch = BatchWriteRequest.newBuilder().setDatabase(getDatabase());
      this.failed = false;
    }

    public void add(String docName) {
      checkState();
      batch.addWrites(Write.newBuilder().setDelete(docName));
    }

    private void checkState() {
      requireNotFailed();
      maybeFlush();
    }

    private void requireNotFailed() {
      if (failed) {
        throw new IllegalStateException(
            "Previous batch commit failed, unable to enqueue new operation");
      }
    }

    private void maybeFlush() {
      if (batch.getWritesCount() == MAX_BATCH_SIZE) {
        flush();
      }
    }

    private void flush() {
      if (batch.getWritesCount() == 0) {
        return;
      }
      try {
        LOG.trace("Flushing {} elements...", batch.getWritesCount());
        rpc.batchWriteCallable().futureCall(batch.build()).get(30, TimeUnit.SECONDS);
        LOG.trace("Flushing {} elements complete", batch.getWritesCount());
        batch.clearWrites();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        failed = true;
        throw new RuntimeException(e);
      }
    }
  }
}
