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
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPage;
import com.google.cloud.firestore.v1.FirestoreClient.PartitionQueryPagedResponse;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.Value;
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryPair;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public final class FirestoreV1FnPartitionQueryTest
    extends BaseFirestoreV1ReadFnTest<PartitionQueryRequest, PartitionQueryPair> {

  @Parameterized.Parameter public Instant readTime;

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private UnaryCallable<PartitionQueryRequest, PartitionQueryPagedResponse> callable;
  @Mock private PartitionQueryPagedResponse pagedResponse1;
  @Mock private PartitionQueryPage page1;
  @Mock private PartitionQueryPagedResponse pagedResponse2;
  @Mock private PartitionQueryPage page2;

  @Parameterized.Parameters(name = "readTime = {0}")
  public static Collection<Object> data() {
    return Arrays.asList(null, Instant.now());
  }

  private PartitionQueryRequest withReadTime(PartitionQueryRequest request, Instant readTime) {
    return readTime == null
        ? request
        : request.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
  }

  @Test
  public void endToEnd() throws Exception {
    // First page of the response
    PartitionQueryRequest request1 =
        PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    PartitionQueryResponse response1 =
        PartitionQueryResponse.newBuilder()
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-100")))
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-200")))
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-300")))
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-400")))
            .build();
    when(callable.call(withReadTime(request1, readTime))).thenReturn(pagedResponse1);
    when(page1.getResponse()).thenReturn(response1);
    when(pagedResponse1.iteratePages()).thenReturn(ImmutableList.of(page1));

    when(stub.partitionQueryPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<PartitionQueryPair> responses =
        ArgumentCaptor.forClass(PartitionQueryPair.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    PartitionQueryFn fn = new PartitionQueryFn(clock, ff, options, readTime);

    runFunction(fn);

    List<PartitionQueryPair> expected = newArrayList(new PartitionQueryPair(request1, response1));
    List<PartitionQueryPair> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Test
  public void endToEnd_emptyCursors() throws Exception {
    // First page of the response
    PartitionQueryRequest request1 =
        PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    PartitionQueryResponse response1 = PartitionQueryResponse.newBuilder().build();
    when(callable.call(withReadTime(request1, readTime))).thenReturn(pagedResponse1);
    when(page1.getResponse()).thenReturn(response1);
    when(pagedResponse1.iteratePages()).thenReturn(ImmutableList.of(page1));

    when(stub.partitionQueryPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<PartitionQueryPair> responses =
        ArgumentCaptor.forClass(PartitionQueryPair.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    PartitionQueryFn fn = new PartitionQueryFn(clock, ff, options, readTime);

    runFunction(fn);

    List<PartitionQueryPair> expected = newArrayList(new PartitionQueryPair(request1, response1));
    List<PartitionQueryPair> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    // First page of the response
    PartitionQueryRequest request1 =
        PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
    PartitionQueryResponse response1 =
        PartitionQueryResponse.newBuilder()
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-100")))
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-200")))
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-300")))
            .setNextPageToken("page2")
            .build();
    when(page1.getResponse()).thenReturn(response1);
    when(callable.call(withReadTime(request1, readTime))).thenReturn(pagedResponse1);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(pagedResponse1.iteratePages())
        .thenAnswer(
            invocation ->
                new Iterable<PartitionQueryPage>() {
                  @Override
                  public Iterator<PartitionQueryPage> iterator() {
                    return new AbstractIterator<PartitionQueryPage>() {
                      private boolean first = true;

                      @Override
                      protected PartitionQueryPage computeNext() {
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
    PartitionQueryRequest request2 =
        PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .setPageToken("page2")
            .build();
    PartitionQueryResponse response2 =
        PartitionQueryResponse.newBuilder()
            .addPartitions(
                Cursor.newBuilder().addValues(Value.newBuilder().setReferenceValue("doc-400")))
            .build();

    PartitionQueryResponse expectedResponse =
        response1
            .toBuilder()
            .clearNextPageToken()
            .addAllPartitions(response2.getPartitionsList())
            .build();

    when(page2.getResponse()).thenReturn(response2);
    when(page2.hasNextPage()).thenReturn(false);
    when(callable.call(withReadTime(request2, readTime))).thenReturn(pagedResponse2);
    when(pagedResponse2.iteratePages()).thenReturn(ImmutableList.of(page2));

    when(stub.partitionQueryPagedCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);

    ArgumentCaptor<PartitionQueryPair> responses =
        ArgumentCaptor.forClass(PartitionQueryPair.class);

    doNothing().when(processContext).output(responses.capture());

    when(processContext.element()).thenReturn(request1);

    PartitionQueryFn fn = new PartitionQueryFn(clock, ff, rpcQosOptions, readTime);

    runFunction(fn);

    List<PartitionQueryPair> expected =
        newArrayList(new PartitionQueryPair(request1, expectedResponse));
    List<PartitionQueryPair> allValues = responses.getAllValues();
    assertEquals(expected, allValues);
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public PartitionQueryRequest getRequest() {
        return PartitionQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreStub stub) {
        when(stub.partitionQueryPagedCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(PartitionQueryRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected PartitionQueryFn getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions) {
    return new PartitionQueryFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }
}
