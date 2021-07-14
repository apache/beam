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
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.CollectionSelector;
import com.google.firestore.v1.StructuredQuery.Direction;
import com.google.firestore.v1.StructuredQuery.FieldFilter;
import com.google.firestore.v1.StructuredQuery.FieldFilter.Operator;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Filter;
import com.google.firestore.v1.StructuredQuery.Order;
import com.google.firestore.v1.Value;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.RunQueryFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public final class FirestoreV1FnRunQueryTest
    extends BaseFirestoreV1ReadFnTest<RunQueryRequest, RunQueryResponse> {

  @Mock private ServerStreamingCallable<RunQueryRequest, RunQueryResponse> callable;
  @Mock private ServerStream<RunQueryResponse> responseStream1;
  @Mock private ServerStream<RunQueryResponse> responseStream2;

  @Test
  public void endToEnd() throws Exception {
    TestData testData = TestData.fieldEqualsBar().setProjectId(projectId).build();

    List<RunQueryResponse> responses =
        ImmutableList.of(testData.response1, testData.response2, testData.response3);
    when(responseStream1.iterator()).thenReturn(responses.iterator());

    when(callable.call(testData.request)).thenReturn(responseStream1);

    when(stub.runQueryCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<RunQueryResponse> responsesCaptor =
        ArgumentCaptor.forClass(RunQueryResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(testData.request);

    RunQueryFn fn = new RunQueryFn(clock, ff, options);

    runFunction(fn);

    List<RunQueryResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(responses, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    TestData testData =
        TestData.fieldEqualsBar()
            .setProjectId(projectId)
            .setOrderFunction(
                f ->
                    Collections.singletonList(
                        Order.newBuilder().setDirection(Direction.ASCENDING).setField(f).build()))
            .build();

    RunQueryRequest request2 =
        RunQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .setStructuredQuery(
                testData
                    .request
                    .getStructuredQuery()
                    .toBuilder()
                    .setStartAt(
                        Cursor.newBuilder()
                            .setBefore(false)
                            .addValues(Value.newBuilder().setStringValue("bar"))))
            .build();

    List<RunQueryResponse> responses =
        ImmutableList.of(testData.response1, testData.response2, testData.response3);
    when(responseStream1.iterator())
        .thenReturn(
            new AbstractIterator<RunQueryResponse>() {
              private int invocationCount = 1;

              @Override
              protected RunQueryResponse computeNext() {
                int count = invocationCount++;
                if (count == 1) {
                  return responses.get(0);
                } else if (count == 2) {
                  return responses.get(1);
                } else {
                  throw RETRYABLE_ERROR;
                }
              }
            });

    when(callable.call(testData.request)).thenReturn(responseStream1);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(responseStream2.iterator()).thenReturn(ImmutableList.of(responses.get(2)).iterator());
    when(callable.call(request2)).thenReturn(responseStream2);

    when(stub.runQueryCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    ArgumentCaptor<RunQueryResponse> responsesCaptor =
        ArgumentCaptor.forClass(RunQueryResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(testData.request);

    RunQueryFn fn = new RunQueryFn(clock, ff, rpcQosOptions);

    runFunction(fn);

    List<RunQueryResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(responses, allValues);

    verify(callable, times(1)).call(testData.request);
    verify(callable, times(1)).call(request2);
    verify(attempt, times(3)).recordStreamValue(any());
  }

  @Test
  public void resumeFromLastReadValue_withNoOrderBy() throws Exception {
    TestData testData = TestData.fieldEqualsBar().setProjectId(projectId).build();

    RunQueryRequest request2 =
        RunQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .setStructuredQuery(
                testData
                    .request
                    .getStructuredQuery()
                    .toBuilder()
                    .setStartAt(
                        Cursor.newBuilder()
                            .setBefore(false)
                            .addValues(
                                Value.newBuilder()
                                    .setReferenceValue(testData.response2.getDocument().getName())))
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                            .setDirection(Direction.ASCENDING)))
            .build();

    List<RunQueryResponse> responses =
        ImmutableList.of(testData.response1, testData.response2, testData.response3);
    when(responseStream1.iterator())
        .thenReturn(
            new AbstractIterator<RunQueryResponse>() {
              private int invocationCount = 1;

              @Override
              protected RunQueryResponse computeNext() {
                int count = invocationCount++;
                if (count == 1) {
                  return responses.get(0);
                } else if (count == 2) {
                  return responses.get(1);
                } else {
                  throw RETRYABLE_ERROR;
                }
              }
            });

    when(callable.call(testData.request)).thenReturn(responseStream1);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(responseStream2.iterator()).thenReturn(ImmutableList.of(testData.response3).iterator());
    when(callable.call(request2)).thenReturn(responseStream2);

    when(stub.runQueryCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    ArgumentCaptor<RunQueryResponse> responsesCaptor =
        ArgumentCaptor.forClass(RunQueryResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(testData.request);

    RunQueryFn fn = new RunQueryFn(clock, ff, rpcQosOptions);

    runFunction(fn);

    List<RunQueryResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(responses, allValues);

    verify(callable, times(1)).call(testData.request);
    verify(callable, times(1)).call(request2);
    verify(attempt, times(3)).recordStreamValue(any());
  }

  @Override
  protected V1RpcFnTestCtx newCtx() {
    return new V1RpcFnTestCtx() {
      @Override
      public RunQueryRequest getRequest() {
        return RunQueryRequest.newBuilder()
            .setParent(String.format("projects/%s/databases/(default)/document", projectId))
            .build();
      }

      @Override
      public void mockRpcToCallable(FirestoreStub stub) {
        when(stub.runQueryCallable()).thenReturn(callable);
      }

      @Override
      public void whenCallableCall(RunQueryRequest in, Throwable... throwables) {
        when(callable.call(in)).thenThrow(throwables);
      }

      @Override
      public void verifyNoInteractionsWithCallable() {
        verifyNoMoreInteractions(callable);
      }
    };
  }

  @Override
  protected RunQueryFn getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
      RpcQosOptions rpcQosOptions) {
    return new RunQueryFn(clock, firestoreStatefulComponentFactory, rpcQosOptions);
  }

  private static final class TestData {

    private final RunQueryRequest request;
    private final RunQueryResponse response1;
    private final RunQueryResponse response2;
    private final RunQueryResponse response3;

    public TestData(String projectId, Function<FieldReference, List<Order>> orderFunction) {
      String fieldPath = "foo";
      FieldReference foo = FieldReference.newBuilder().setFieldPath(fieldPath).build();
      StructuredQuery.Builder builder =
          StructuredQuery.newBuilder()
              .addFrom(
                  CollectionSelector.newBuilder()
                      .setAllDescendants(false)
                      .setCollectionId("collection"))
              .setWhere(
                  Filter.newBuilder()
                      .setFieldFilter(
                          FieldFilter.newBuilder()
                              .setField(foo)
                              .setOp(Operator.EQUAL)
                              .setValue(Value.newBuilder().setStringValue("bar"))
                              .build()));

      orderFunction.apply(foo).forEach(builder::addOrderBy);
      request =
          RunQueryRequest.newBuilder()
              .setParent(String.format("projects/%s/databases/(default)/document", projectId))
              .setStructuredQuery(builder)
              .build();

      response1 = newResponse(fieldPath, 1);
      response2 = newResponse(fieldPath, 2);
      response3 = newResponse(fieldPath, 3);
    }

    private static RunQueryResponse newResponse(String field, int docNumber) {
      String docId = String.format("doc-%d", docNumber);
      return RunQueryResponse.newBuilder()
          .setDocument(
              Document.newBuilder()
                  .setName(docId)
                  .putAllFields(
                      ImmutableMap.of(field, Value.newBuilder().setStringValue("bar").build()))
                  .build())
          .build();
    }

    private static Builder fieldEqualsBar() {
      return new Builder();
    }

    @SuppressWarnings("initialization.fields.uninitialized") // fields set via builder methods
    private static final class Builder {

      private String projectId;
      private Function<FieldReference, List<Order>> orderFunction;

      public Builder() {
        orderFunction = f -> Collections.emptyList();
      }

      public Builder setProjectId(String projectId) {
        this.projectId = projectId;
        return this;
      }

      public Builder setOrderFunction(Function<FieldReference, List<Order>> orderFunction) {
        this.orderFunction = orderFunction;
        return this;
      }

      private TestData build() {
        return new TestData(
            requireNonNull(projectId, "projectId must be non null"),
            requireNonNull(orderFunction, "orderFunction must be non null"));
      }
    }
  }
}
