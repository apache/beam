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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPage;
import com.google.cloud.firestore.v1.FirestoreClient.ListCollectionIdsPagedResponse;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.ListCollectionIdsFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public final class FirestoreV1FnListCollectionIdsTest
    extends BaseFirestoreV1ReadFnTest<ListCollectionIdsRequest, ListCollectionIdsResponse> {

  @Parameter public Instant readTime;

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private UnaryCallable<ListCollectionIdsRequest, ListCollectionIdsPagedResponse> callable;
  @Mock private ListCollectionIdsPagedResponse pagedResponse1;
  @Mock private ListCollectionIdsPage page1;
  @Mock private ListCollectionIdsPagedResponse pagedResponse2;
  @Mock private ListCollectionIdsPage page2;

  @Parameters(name = "readTime = {0}")
  public static Collection<Object> data() {
    return Arrays.asList(null, Instant.now());
  }

  private ListCollectionIdsRequest withReadTime(ListCollectionIdsRequest input, Instant readTime) {
    return readTime == null
        ? input
        : input.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
  }

  @Test
  public void endToEnd() throws Exception {
    // First page of the response
    ListCollectionIdsRequest request1 =
        ListCollectionIdsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    ListCollectionIdsResponse response1 =
        ListCollectionIdsResponse.newBuilder()
            .addCollectionIds("col_1-1")
            .addCollectionIds("col_1-2")
            .addCollectionIds("col_1-3")
            .setNextPageToken("page2")
            .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);

    // Second page of the response
    ListCollectionIdsResponse response2 =
        ListCollectionIdsResponse.newBuilder().addCollectionIds("col_2-1").build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(pagedResponse1.iteratePages()).thenReturn(ImmutableList.of(page1, page2));
    when(callable.call(withReadTime(request1, readTime))).thenReturn(pagedResponse1);

    when(stub.listCollectionIdsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<ListCollectionIdsResponse> responses =
        ArgumentCaptor.forClass(ListCollectionIdsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListCollectionIdsFn fn = new ListCollectionIdsFn(clock, ff, options, readTime);

    runFunction(fn);

    List<ListCollectionIdsResponse> expected = newArrayList(response1, response2);
    List<ListCollectionIdsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    // First page of the response
    ListCollectionIdsRequest request1 =
        ListCollectionIdsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    ListCollectionIdsResponse response1 =
        ListCollectionIdsResponse.newBuilder()
            .addCollectionIds("col_1-1")
            .addCollectionIds("col_1-2")
            .addCollectionIds("col_1-3")
            .setNextPageToken("page2")
            .build();
    when(page1.getNextPageToken()).thenReturn(response1.getNextPageToken());
    when(page1.getResponse()).thenReturn(response1);
    when(page1.hasNextPage()).thenReturn(true);
    when(callable.call(withReadTime(request1, readTime))).thenReturn(pagedResponse1);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(pagedResponse1.iteratePages())
        .thenAnswer(
            invocation ->
                new Iterable<ListCollectionIdsPage>() {
                  @Override
                  public Iterator<ListCollectionIdsPage> iterator() {
                    return new AbstractIterator<ListCollectionIdsPage>() {
                      private boolean first = true;

                      @Override
                      protected ListCollectionIdsPage computeNext() {
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
    ListCollectionIdsRequest request2 =
        ListCollectionIdsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .setPageToken("page2")
            .build();
    ListCollectionIdsResponse response2 =
        ListCollectionIdsResponse.newBuilder().addCollectionIds("col_2-1").build();
    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(callable.call(withReadTime(request2, readTime))).thenReturn(pagedResponse2);
    when(pagedResponse2.iteratePages()).thenReturn(ImmutableList.of(page2));

    when(stub.listCollectionIdsPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);

    ArgumentCaptor<ListCollectionIdsResponse> responses =
        ArgumentCaptor.forClass(ListCollectionIdsResponse.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    ListCollectionIdsFn fn = new ListCollectionIdsFn(clock, ff, rpcQosOptions, readTime);

    runFunction(fn);

    List<ListCollectionIdsResponse> expected = newArrayList(response1, response2);
    List<ListCollectionIdsResponse> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public ListCollectionIdsRequest getRequest() {
        return ListCollectionIdsRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreStub stub) {
        when(stub.listCollectionIdsPagedCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(ListCollectionIdsRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected ListCollectionIdsFn getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions) {
    return new ListCollectionIdsFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }
}
