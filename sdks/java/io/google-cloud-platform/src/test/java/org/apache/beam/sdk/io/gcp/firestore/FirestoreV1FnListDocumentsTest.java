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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListDocumentsPagedResponse;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.Value;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.ListDocumentsFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class FirestoreV1FnListDocumentsTest
    extends BaseFirestoreV1ReadFnTest<ListDocumentsRequest, ListDocumentsResponse> {

  @Mock private UnaryCallable<ListDocumentsRequest, ListDocumentsPagedResponse> callable;
  @Mock private ListDocumentsPagedResponse pagedResponse1;
  @Mock private ListDocumentsPage page1;
  @Mock private ListDocumentsPagedResponse pagedResponse2;
  @Mock private ListDocumentsPage page2;

  @Test
  public void endToEnd() throws Exception {
    // First page of the response
    ListDocumentsRequest request1 =
        ListDocumentsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    ListDocumentsResponse response1 =
        ListDocumentsResponse.newBuilder()
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-1")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-2")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-3")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .setNextPageToken("page2")
            .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);

    // Second page of the response
    ListDocumentsResponse response2 =
        ListDocumentsResponse.newBuilder()
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_2-1")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse1.iteratePages()).thenReturn(ImmutableList.of(page1, page2));
    when(callable.call(request1)).thenReturn(pagedResponse1);

    when(stub.listDocumentsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<ListDocumentsResponse> responses =
        ArgumentCaptor.forClass(ListDocumentsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListDocumentsFn fn = new ListDocumentsFn(clock, ff, options);

    runFunction(fn);

    List<ListDocumentsResponse> expected = newArrayList(response1, response2);
    List<ListDocumentsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    // First page of the response
    ListDocumentsRequest request1 =
        ListDocumentsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    ListDocumentsResponse response1 =
        ListDocumentsResponse.newBuilder()
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-1")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-2")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_1-3")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .setNextPageToken("page2")
            .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(callable.call(request1)).thenReturn(pagedResponse1);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(pagedResponse1.iteratePages())
        .thenAnswer(
            invocation ->
                new Iterable<ListDocumentsPage>() {
                  @Override
                  public Iterator<ListDocumentsPage> iterator() {
                    return new AbstractIterator<ListDocumentsPage>() {
                      private boolean first = true;

                      @Override
                      protected ListDocumentsPage computeNext() {
                        if (first) {
                          first = false;
                          return page1;
                        } else {
                          throw RETRYABLE_ERROR;
                        }
                      }
                    };
                  }
                });

    // Second page of the response
    ListDocumentsRequest request2 =
        ListDocumentsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .setPageToken("page2")
            .build();
    ListDocumentsResponse response2 =
        ListDocumentsResponse.newBuilder()
            .addDocuments(
                Document.newBuilder()
                    .setName("doc_2-1")
                    .putAllFields(
                        ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                    .build())
            .build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(callable.call(request2)).thenReturn(pagedResponse2);
    when(pagedResponse2.iteratePages()).thenReturn(ImmutableList.of(page2));

    when(stub.listDocumentsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);

    ArgumentCaptor<ListDocumentsResponse> responses =
        ArgumentCaptor.forClass(ListDocumentsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListDocumentsFn fn = new ListDocumentsFn(clock, ff, rpcQosOptions);

    runFunction(fn);

    List<ListDocumentsResponse> expected = newArrayList(response1, response2);
    List<ListDocumentsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public ListDocumentsRequest getRequest() {
        return ListDocumentsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreStub stub) {
        when(stub.listDocumentsPagedCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(ListDocumentsRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected ListDocumentsFn getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions) {
    return new ListDocumentsFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }
}
