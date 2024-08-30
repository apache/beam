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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.rpc.ServerStreamingCallable;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.BatchGetDocumentsFn;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
public final class FirestoreV1FnBatchGetDocumentsTest
    extends BaseFirestoreV1ReadFnTest<BatchGetDocumentsRequest, BatchGetDocumentsResponse> {
  @Parameterized.Parameter public Instant readTime;

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock
  private ServerStreamingCallable<BatchGetDocumentsRequest, BatchGetDocumentsResponse> callable;

  @Mock private ServerStream<BatchGetDocumentsResponse> responseStream1;
  @Mock private ServerStream<BatchGetDocumentsResponse> responseStream2;
  @Mock private ServerStream<BatchGetDocumentsResponse> responseStream3;

  @Parameterized.Parameters(name = "readTime = {0}")
  public static Collection<Object> data() {
    return Arrays.asList(null, Instant.now());
  }

  private BatchGetDocumentsRequest withReadTime(
      BatchGetDocumentsRequest request, Instant readTime) {
    return readTime == null
        ? request
        : request.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
  }

  @Test
  public void endToEnd() throws Exception {
    final BatchGetDocumentsRequest request =
        BatchGetDocumentsRequest.newBuilder()
            .setDatabase(String.format("projects/%s/databases/(default)", projectId))
            .addDocuments("doc_1-1")
            .addDocuments("doc_1-2")
            .addDocuments("doc_1-3")
            .build();

    final BatchGetDocumentsResponse response1 = newFound(1);
    final BatchGetDocumentsResponse response2 = newFound(2);
    final BatchGetDocumentsResponse response3 = newFound(3);

    List<BatchGetDocumentsResponse> responses = ImmutableList.of(response1, response2, response3);
    when(responseStream1.iterator()).thenReturn(responses.iterator());

    when(callable.call(withReadTime(request, readTime))).thenReturn(responseStream1);

    when(stub.batchGetDocumentsCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(rpcQosOptions));

    ArgumentCaptor<BatchGetDocumentsResponse> responsesCaptor =
        ArgumentCaptor.forClass(BatchGetDocumentsResponse.class);
    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(request);

    runFunction(new BatchGetDocumentsFn(clock, ff, rpcQosOptions, readTime));

    List<BatchGetDocumentsResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(responses, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {

    final BatchGetDocumentsResponse response1 = newMissing(1);
    final BatchGetDocumentsResponse response2 = newFound(2);
    final BatchGetDocumentsResponse response3 = newMissing(3);
    final BatchGetDocumentsResponse response4 = newFound(4);

    final BatchGetDocumentsRequest request1 =
        BatchGetDocumentsRequest.newBuilder()
            .setDatabase(String.format("projects/%s/databases/(default)", projectId))
            .addDocuments(response1.getMissing())
            .addDocuments(response2.getFound().getName())
            .addDocuments(response3.getMissing())
            .addDocuments(response4.getFound().getName())
            .build();

    BatchGetDocumentsRequest request2 =
        BatchGetDocumentsRequest.newBuilder()
            .setDatabase(String.format("projects/%s/databases/(default)", projectId))
            .addDocuments(response3.getMissing())
            .addDocuments(response4.getFound().getName())
            .build();
    BatchGetDocumentsRequest request3 =
        BatchGetDocumentsRequest.newBuilder()
            .setDatabase(String.format("projects/%s/databases/(default)", projectId))
            .addDocuments(response4.getFound().getName())
            .build();

    when(responseStream1.iterator())
        .thenReturn(
            new AbstractIterator<BatchGetDocumentsResponse>() {
              private int counter = 10;

              @Override
              protected BatchGetDocumentsResponse computeNext() {
                int count = counter++;
                if (count == 10) {
                  return response1;
                } else if (count == 11) {
                  return response2;
                } else {
                  throw RETRYABLE_ERROR;
                }
              }
            });
    when(responseStream2.iterator())
        .thenReturn(
            new AbstractIterator<BatchGetDocumentsResponse>() {
              private int counter = 20;

              @Override
              protected BatchGetDocumentsResponse computeNext() {
                int count = counter++;
                if (count == 20) {
                  return response3;
                } else {
                  throw RETRYABLE_ERROR;
                }
              }
            });
    when(responseStream3.iterator()).thenReturn(ImmutableList.of(response4).iterator());

    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(callable.call(withReadTime(request1, readTime))).thenReturn(responseStream1);
    when(callable.call(withReadTime(request2, readTime))).thenReturn(responseStream2);
    when(callable.call(withReadTime(request3, readTime))).thenReturn(responseStream3);

    when(stub.batchGetDocumentsCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    ArgumentCaptor<BatchGetDocumentsResponse> responsesCaptor =
        ArgumentCaptor.forClass(BatchGetDocumentsResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(request1);

    BatchGetDocumentsFn fn = new BatchGetDocumentsFn(clock, ff, rpcQosOptions, readTime);

    runFunction(fn);

    List<BatchGetDocumentsResponse> expectedResponses =
        ImmutableList.of(response1, response2, response3, response4);
    List<BatchGetDocumentsResponse> actualResponses = responsesCaptor.getAllValues();
    assertEquals(expectedResponses, actualResponses);

    verify(callable, times(1)).call(withReadTime(request1, readTime));
    verify(callable, times(1)).call(withReadTime(request2, readTime));
    verify(attempt, times(4)).recordStreamValue(any());
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public BatchGetDocumentsRequest getRequest() {
        return BatchGetDocumentsRequest.newBuilder()
            .setDatabase(String.format("projects/%s/databases/(default)", projectId))
            .addDocuments("doc_1-1")
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreStub stub) {
        when(stub.batchGetDocumentsCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(BatchGetDocumentsRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected BatchGetDocumentsFn getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions) {
    return new BatchGetDocumentsFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }

  private static BatchGetDocumentsResponse newFound(int docNumber) {
    String docName = docName(docNumber);
    return BatchGetDocumentsResponse.newBuilder()
        .setFound(
            Document.newBuilder()
                .setName(docName)
                .putAllFields(
                    ImmutableMap.of("foo", Value.newBuilder().setStringValue("bar").build()))
                .build())
        .build();
  }

  private static BatchGetDocumentsResponse newMissing(int docNumber) {
    String docName = docName(docNumber);
    return BatchGetDocumentsResponse.newBuilder().setMissing(docName).build();
  }

  private static String docName(int docNumber) {
    return String.format("doc-%d", docNumber);
  }
}
