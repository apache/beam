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
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;

import com.google.api.gax.paging.AbstractPage;
import com.google.api.gax.paging.AbstractPagedListResponse;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPage;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchGetDocumentsResponse.ResultCase;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.Value;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.util.Timestamps;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreDoFn.ImplicitlyWindowedFirestoreDoFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A collection of {@link org.apache.beam.sdk.transforms.DoFn DoFn}s for each of the supported read
 * RPC methods from the Cloud Firestore V1 API.
 */
final class FirestoreV1ReadFn {

  /**
   * {@link DoFn} for Firestore V1 {@link RunQueryRequest}s.
   *
   * <p>This Fn uses a stream to obtain responses, each response from the stream will be output to
   * the next stage of the pipeline. Each response from the stream represents an individual document
   * with the associated metadata.
   *
   * <p>If an error is encountered while reading from the stream, the stream will attempt to resume
   * rather than starting over. The restarting of the stream will continue within the scope of the
   * completion of the request (meaning any possibility of resumption is contingent upon an attempt
   * being available in the Qos budget).
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class RunQueryFn
      extends StreamingFirestoreV1ReadFn<RunQueryRequest, RunQueryResponse> {

    RunQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    RunQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.RunQuery;
    }

    @Override
    protected ServerStreamingCallable<RunQueryRequest, RunQueryResponse> getCallable(
        FirestoreStub firestoreStub) {
      return firestoreStub.runQueryCallable();
    }

    @Override
    protected RunQueryRequest setStartFrom(
        RunQueryRequest element, RunQueryResponse runQueryResponse) {
      StructuredQuery query = element.getStructuredQuery();
      StructuredQuery.Builder builder = query.toBuilder();
      builder.addAllOrderBy(QueryUtils.getImplicitOrderBy(query));
      Cursor.Builder cursor = Cursor.newBuilder().setBefore(false);
      for (Order order : builder.getOrderByList()) {
        Value value =
            QueryUtils.lookupDocumentValue(
                runQueryResponse.getDocument(), order.getField().getFieldPath());
        if (value == null) {
          throw new IllegalStateException(
              String.format(
                  "Failed to build query resumption token, field '%s' not found in doc with __name__ '%s'",
                  order.getField().getFieldPath(), runQueryResponse.getDocument().getName()));
        }
        cursor.addValues(value);
      }
      builder.setStartAt(cursor.build());
      return element.toBuilder().setStructuredQuery(builder.build()).build();
    }

    @Override
    protected RunQueryRequest setReadTime(RunQueryRequest element, Instant readTime) {
      return element.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
    }

    @Override
    protected @Nullable RunQueryResponse resumptionValue(
        @Nullable RunQueryResponse previousValue, RunQueryResponse nextValue) {
      // We need a document to resume, may be null if reporting partial progress.
      return nextValue.hasDocument() ? nextValue : previousValue;
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link PartitionQueryRequest}s.
   *
   * <p>This Fn uses pagination to obtain responses, all pages will be aggregated before being
   * emitted to the next stage of the pipeline. Aggregation of pages is necessary as the next step
   * of pairing of cursors to create N queries must first sort all cursors. See <a target="_blank"
   * rel="noopener noreferrer"
   * href="https://cloud.google.com/firestore/docs/reference/rest/v1/projects.databases.documents/partitionQuery#request-body">{@code
   * pageToken}s</a> documentation for details.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class PartitionQueryFn
      extends BaseFirestoreV1ReadFn<PartitionQueryRequest, PartitionQueryPair> {

    public PartitionQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, null);
    }

    public PartitionQueryFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.PartitionQuery;
    }

    @Override
    public void processElement(ProcessContext context) throws Exception {
      @SuppressWarnings("nullness")
      final PartitionQueryRequest element =
          requireNonNull(context.element(), "c.element() must be non null");

      RpcQos.RpcReadAttempt attempt = rpcQos.newReadAttempt(getRpcAttemptContext());
      PartitionQueryResponse.Builder aggregate = null;
      while (true) {
        if (!attempt.awaitSafeToProceed(clock.instant())) {
          continue;
        }

        try {
          PartitionQueryRequest request = setPageToken(element, aggregate);
          request = readTime == null ? request : setReadTime(request, readTime);
          attempt.recordRequestStart(clock.instant());
          PartitionQueryPagedResponse pagedResponse =
              firestoreStub.partitionQueryPagedCallable().call(request);
          for (PartitionQueryPage page : pagedResponse.iteratePages()) {
            attempt.recordRequestSuccessful(clock.instant());
            PartitionQueryResponse response = page.getResponse();
            if (aggregate == null) {
              aggregate = response.toBuilder();
            } else {
              aggregate.addAllPartitions(response.getPartitionsList());
              if (page.hasNextPage()) {
                aggregate.setNextPageToken(response.getNextPageToken());
              } else {
                aggregate.clearNextPageToken();
              }
            }
            if (page.hasNextPage()) {
              attempt.recordRequestStart(clock.instant());
            }
          }
          attempt.completeSuccess();
          break;
        } catch (RuntimeException exception) {
          Instant end = clock.instant();
          attempt.recordRequestFailed(end);
          attempt.checkCanRetry(end, exception);
        }
      }
      if (aggregate != null) {
        context.output(new PartitionQueryPair(element, aggregate.build()));
      }
    }

    private PartitionQueryRequest setPageToken(
        PartitionQueryRequest element, PartitionQueryResponse.@Nullable Builder aggregate) {
      if (aggregate != null && aggregate.getNextPageToken() != null) {
        return element.toBuilder().setPageToken(aggregate.getNextPageToken()).build();
      }
      return element;
    }

    @Override
    protected PartitionQueryRequest setReadTime(PartitionQueryRequest element, Instant readTime) {
      return element.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link ListDocumentsRequest}s.
   *
   * <p>This Fn uses pagination to obtain responses, the response from each page will be output to
   * the next stage of the pipeline.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class ListDocumentsFn
      extends PaginatedFirestoreV1ReadFn<
          ListDocumentsRequest,
          ListDocumentsPagedResponse,
          ListDocumentsPage,
          ListDocumentsResponse> {

    ListDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, null);
    }

    ListDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.ListDocuments;
    }

    @Override
    protected UnaryCallable<ListDocumentsRequest, ListDocumentsPagedResponse> getCallable(
        FirestoreStub firestoreStub) {
      return firestoreStub.listDocumentsPagedCallable();
    }

    @Override
    protected ListDocumentsRequest setPageToken(
        ListDocumentsRequest element, String nextPageToken) {
      return element.toBuilder().setPageToken(nextPageToken).build();
    }

    @Override
    protected ListDocumentsRequest setReadTime(ListDocumentsRequest element, Instant readTime) {
      return element.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link ListCollectionIdsRequest}s.
   *
   * <p>This Fn uses pagination to obtain responses, the response from each page will be output to
   * the next stage of the pipeline.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class ListCollectionIdsFn
      extends PaginatedFirestoreV1ReadFn<
          ListCollectionIdsRequest,
          ListCollectionIdsPagedResponse,
          ListCollectionIdsPage,
          ListCollectionIdsResponse> {

    ListCollectionIdsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, null);
    }

    ListCollectionIdsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.ListCollectionIds;
    }

    @Override
    protected UnaryCallable<ListCollectionIdsRequest, ListCollectionIdsPagedResponse> getCallable(
        FirestoreStub firestoreStub) {
      return firestoreStub.listCollectionIdsPagedCallable();
    }

    @Override
    protected ListCollectionIdsRequest setPageToken(
        ListCollectionIdsRequest element, String nextPageToken) {
      return element.toBuilder().setPageToken(nextPageToken).build();
    }

    @Override
    protected ListCollectionIdsRequest setReadTime(
        ListCollectionIdsRequest element, Instant readTime) {
      return element.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
    }
  }

  /**
   * {@link DoFn} for Firestore V1 {@link BatchGetDocumentsRequest}s.
   *
   * <p>This Fn uses a stream to obtain responses, each response from the stream will be output to
   * the next stage of the pipeline. Each response from the stream represents an individual document
   * with the associated metadata.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   */
  static final class BatchGetDocumentsFn
      extends StreamingFirestoreV1ReadFn<BatchGetDocumentsRequest, BatchGetDocumentsResponse> {

    BatchGetDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    BatchGetDocumentsFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public Context getRpcAttemptContext() {
      return FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.BatchGetDocuments;
    }

    @Override
    protected ServerStreamingCallable<BatchGetDocumentsRequest, BatchGetDocumentsResponse>
        getCallable(FirestoreStub firestoreStub) {
      return firestoreStub.batchGetDocumentsCallable();
    }

    @Override
    protected BatchGetDocumentsRequest setStartFrom(
        BatchGetDocumentsRequest element, BatchGetDocumentsResponse mostRecentResponse) {
      int startIndex = -1;
      ProtocolStringList documentsList = element.getDocumentsList();
      String missing = mostRecentResponse.getMissing();
      String foundName =
          mostRecentResponse.hasFound() ? mostRecentResponse.getFound().getName() : null;
      // we only scan until the second to last originalRequest. If the final element were to be
      // reached
      // the full request would be complete and we wouldn't be in this scenario
      int maxIndex = documentsList.size() - 2;
      for (int i = 0; i <= maxIndex; i++) {
        String docName = documentsList.get(i);
        if (docName.equals(missing) || docName.equals(foundName)) {
          startIndex = i;
          break;
        }
      }
      if (0 <= startIndex) {
        BatchGetDocumentsRequest.Builder builder = element.toBuilder().clearDocuments();
        documentsList.stream()
            .skip(startIndex + 1) // start from the next entry from the one we found
            .forEach(builder::addDocuments);
        return builder.build();
      }
      throw new IllegalStateException(
          String.format(
              "Unable to determine BatchGet resumption point. Most recently received doc __name__ '%s'",
              foundName != null ? foundName : missing));
    }

    @Override
    protected @Nullable BatchGetDocumentsResponse resumptionValue(
        @Nullable BatchGetDocumentsResponse previousValue, BatchGetDocumentsResponse newValue) {
      // No sense in resuming from an empty result.
      return newValue.getResultCase() == ResultCase.RESULT_NOT_SET ? previousValue : newValue;
    }

    @Override
    protected BatchGetDocumentsRequest setReadTime(
        BatchGetDocumentsRequest element, Instant readTime) {
      return element.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
    }
  }

  /**
   * {@link DoFn} Providing support for a Read type RPC operation which uses a Stream rather than
   * pagination. Each response from the stream will be output to the next stage of the pipeline.
   *
   * <p>All request quality-of-service is managed via the instance of {@link RpcQos} associated with
   * the lifecycle of this Fn.
   *
   * @param <InT> Request type
   * @param <OutT> Response type
   */
  private abstract static class StreamingFirestoreV1ReadFn<
          InT extends Message, OutT extends Message>
      extends BaseFirestoreV1ReadFn<InT, OutT> {

    protected StreamingFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, null);
    }

    protected StreamingFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    protected abstract ServerStreamingCallable<InT, OutT> getCallable(FirestoreStub firestoreStub);

    protected abstract InT setStartFrom(InT element, OutT out);

    protected abstract @Nullable OutT resumptionValue(@Nullable OutT previousValue, OutT newValue);

    @Override
    public final void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings(
          "nullness") // for some reason requireNonNull thinks its parameter but be non-null...
      final InT element = requireNonNull(c.element(), "c.element() must be non null");

      RpcQos.RpcReadAttempt attempt = rpcQos.newReadAttempt(getRpcAttemptContext());
      OutT lastReceivedValue = null;
      while (true) {
        if (!attempt.awaitSafeToProceed(clock.instant())) {
          continue;
        }

        Instant start = clock.instant();
        InT request =
            lastReceivedValue == null ? element : setStartFrom(element, lastReceivedValue);
        request = readTime == null ? request : setReadTime(request, readTime);
        try {
          attempt.recordRequestStart(start);
          ServerStream<OutT> serverStream = getCallable(firestoreStub).call(request);
          attempt.recordRequestSuccessful(clock.instant());
          for (OutT out : serverStream) {
            lastReceivedValue = resumptionValue(lastReceivedValue, out);
            attempt.recordStreamValue(clock.instant());
            c.output(out);
          }
          attempt.completeSuccess();
          break;
        } catch (RuntimeException exception) {
          Instant end = clock.instant();
          attempt.recordRequestFailed(end);
          attempt.checkCanRetry(end, exception);
        }
      }
    }
  }

  /**
   * {@link DoFn} Providing support for a Read type RPC operation which uses pagination rather than
   * a Stream.
   *
   * @param <RequestT> Request type
   * @param <ResponseT> Response type
   */
  @SuppressWarnings({
    // errorchecker doesn't like the second ? on PagedResponse, seemingly because of different
    // recursion depth limits; 3 on the found vs 4 on the required.
    // The second ? is the type of collection the paged response uses to hold all responses if
    // trying to expand all pages to a single collection. We are emitting a single page at a time
    // while tracking read progress so we can resume if an error has occurred and we still have
    // attempt budget available.
    "type.argument"
  })
  private abstract static class PaginatedFirestoreV1ReadFn<
          RequestT extends Message,
          PagedResponseT extends AbstractPagedListResponse<RequestT, ResponseT, ?, PageT, ?>,
          PageT extends AbstractPage<RequestT, ResponseT, ?, PageT>,
          ResponseT extends Message>
      extends BaseFirestoreV1ReadFn<RequestT, ResponseT> {

    protected PaginatedFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    protected abstract UnaryCallable<RequestT, PagedResponseT> getCallable(
        FirestoreStub firestoreStub);

    protected abstract RequestT setPageToken(RequestT request, String nextPageToken);

    @Override
    public final void processElement(ProcessContext c) throws Exception {
      @SuppressWarnings(
          "nullness") // for some reason requireNonNull thinks its parameter but be non-null...
      final RequestT element = requireNonNull(c.element(), "c.element() must be non null");

      RpcQos.RpcReadAttempt attempt = rpcQos.newReadAttempt(getRpcAttemptContext());
      String nextPageToken = null;
      while (true) {
        if (!attempt.awaitSafeToProceed(clock.instant())) {
          continue;
        }

        try {
          RequestT request = nextPageToken == null ? element : setPageToken(element, nextPageToken);
          request = readTime == null ? request : setReadTime(request, readTime);
          attempt.recordRequestStart(clock.instant());
          PagedResponseT pagedResponse = getCallable(firestoreStub).call(request);
          for (PageT page : pagedResponse.iteratePages()) {
            ResponseT response = page.getResponse();
            attempt.recordRequestSuccessful(clock.instant());
            c.output(response);
            if (page.hasNextPage()) {
              nextPageToken = page.getNextPageToken();
              attempt.recordRequestStart(clock.instant());
            }
          }
          attempt.completeSuccess();
          break;
        } catch (RuntimeException exception) {
          Instant end = clock.instant();
          attempt.recordRequestFailed(end);
          attempt.checkCanRetry(end, exception);
        }
      }
    }
  }

  /**
   * Base class for all {@link org.apache.beam.sdk.transforms.DoFn DoFn}s which provide access to
   * RPCs from the Cloud Firestore V1 API.
   *
   * <p>This class takes care of common lifecycle elements and transient state management for
   * subclasses allowing subclasses to provide the minimal implementation for {@link
   * ImplicitlyWindowedFirestoreDoFn#processElement(DoFn.ProcessContext)}}
   *
   * @param <InT> The type of element coming into this {@link DoFn}
   * @param <OutT> The type of element output from this {@link DoFn}
   */
  abstract static class BaseFirestoreV1ReadFn<InT, OutT>
      extends ImplicitlyWindowedFirestoreDoFn<InT, OutT> implements HasRpcAttemptContext {

    protected final JodaClock clock;
    protected final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    protected final RpcQosOptions rpcQosOptions;

    protected final @Nullable Instant readTime;

    // transient running state information, not important to any possible checkpointing
    protected transient FirestoreStub firestoreStub;
    protected transient RpcQos rpcQos;
    protected transient String projectId;

    @SuppressWarnings(
        "initialization.fields.uninitialized") // allow transient fields to be managed by component
    // lifecycle
    protected BaseFirestoreV1ReadFn(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      this.clock = requireNonNull(clock, "clock must be non null");
      this.firestoreStatefulComponentFactory =
          requireNonNull(firestoreStatefulComponentFactory, "firestoreFactory must be non null");
      this.rpcQosOptions = requireNonNull(rpcQosOptions, "rpcQosOptions must be non null");
      this.readTime = readTime;
    }

    /** {@inheritDoc} */
    @Override
    public void setup() {
      rpcQos = firestoreStatefulComponentFactory.getRpcQos(rpcQosOptions);
    }

    /** {@inheritDoc} */
    @Override
    public final void startBundle(StartBundleContext c) {
      String project = c.getPipelineOptions().as(FirestoreOptions.class).getFirestoreProject();
      if (project == null) {
        project = c.getPipelineOptions().as(GcpOptions.class).getProject();
      }
      projectId =
          requireNonNull(
              project,
              "project must be defined on FirestoreOptions or GcpOptions of PipelineOptions");
      firestoreStub = firestoreStatefulComponentFactory.getFirestoreStub(c.getPipelineOptions());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("nullness") // allow clearing transient fields
    @Override
    public void finishBundle() throws Exception {
      projectId = null;
      firestoreStub.close();
    }

    /** {@inheritDoc} */
    @Override
    public final void populateDisplayData(DisplayData.Builder builder) {
      builder.include("rpcQosOptions", rpcQosOptions);
      builder.addIfNotNull(DisplayData.item("readTime", readTime).withLabel("ReadTime"));
    }

    protected abstract InT setReadTime(InT element, Instant readTime);
  }

  /**
   * Tuple class for a PartitionQuery Request and Response Pair.
   *
   * <p>When processing the response of a ParitionQuery it only is useful in the context of the
   * original request as the cursors from the response are tied to the index resolved from the
   * request. This class ties these two together so that they can be passed along the pipeline
   * together.
   */
  static final class PartitionQueryPair implements Serializable {
    private final PartitionQueryRequest request;
    private final PartitionQueryResponse response;

    @VisibleForTesting
    PartitionQueryPair(PartitionQueryRequest request, PartitionQueryResponse response) {
      this.request = request;
      this.response = response;
    }

    public PartitionQueryRequest getRequest() {
      return request;
    }

    public PartitionQueryResponse getResponse() {
      return response;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PartitionQueryPair)) {
        return false;
      }
      PartitionQueryPair that = (PartitionQueryPair) o;
      return request.equals(that.request) && response.equals(that.response);
    }

    @Override
    public int hashCode() {
      return Objects.hash(request, response);
    }

    @Override
    public String toString() {
      return "PartitionQueryPair{" + "request=" + request + ", response=" + response + '}';
    }
  }
}
