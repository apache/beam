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
import com.google.firestore.v1.MapValue;
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
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.RunQueryFn;
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
public final class FirestoreV1FnRunQueryTest
    extends BaseFirestoreV1ReadFnTest<RunQueryRequest, RunQueryResponse> {

  @Parameterized.Parameter public Instant readTime;

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private ServerStreamingCallable<RunQueryRequest, RunQueryResponse> callable;
  @Mock private ServerStream<RunQueryResponse> responseStream;
  @Mock private ServerStream<RunQueryResponse> retryResponseStream;

  @Parameterized.Parameters(name = "readTime = {0}")
  public static Collection<Object> data() {
    return Arrays.asList(null, Instant.now());
  }

  private RunQueryRequest withReadTime(RunQueryRequest request, Instant readTime) {
    return readTime == null
        ? request
        : request.toBuilder().setReadTime(Timestamps.fromMillis(readTime.getMillis())).build();
  }

  @Test
  public void endToEnd() throws Exception {
    TestData testData =
        new TestData.Builder().setFilter(TestData.FIELD_EQUALS_BAR).setProjectId(projectId).build();

    List<RunQueryResponse> responses =
        ImmutableList.of(testData.response1, testData.response2, testData.response3);
    when(responseStream.iterator()).thenReturn(responses.iterator());

    when(callable.call(withReadTime(testData.request, readTime))).thenReturn(responseStream);

    when(stub.runQueryCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    RpcQosOptions options = RpcQosOptions.defaultOptions();
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    ArgumentCaptor<RunQueryResponse> responsesCaptor =
        ArgumentCaptor.forClass(RunQueryResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(testData.request);

    RunQueryFn fn = new RunQueryFn(clock, ff, options, readTime);

    runFunction(fn);

    List<RunQueryResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(responses, allValues);
  }

  @Override
  public void resumeFromLastReadValue() throws Exception {
    buildAndRunQueryRetryTest("foo", "bar");
  }

  @Test
  public void resumeFromLastReadValue_nestedOrderBy() throws Exception {
    buildAndRunQueryRetryTest("baz.qux", "val");
  }

  @Test
  public void resumeFromLastReadValue_nestedOrderBySimpleEscaping() throws Exception {
    buildAndRunQueryRetryTest("`quux.quuz`", "123");
  }

  @Test
  public void resumeFromLastReadValue_nestedOrderByComplexEscaping() throws Exception {
    buildAndRunQueryRetryTest("`fo\\`o.m\\`ap`.`bar.key`", "bar.val");
  }

  @Test
  public void resumeFromLastReadValue_withNoOrderBy() throws Exception {
    TestData testData =
        new TestData.Builder()
            .setFilter(TestData.FIELD_NOT_EQUALS_FOO)
            .setProjectId(projectId)
            .build();
    RunQueryRequest expectedRetryRequest =
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
                            .addValues(Value.newBuilder().setStringValue("bar"))
                            .addValues(
                                Value.newBuilder()
                                    .setReferenceValue(testData.response2.getDocument().getName())))
                    .addOrderBy(
                        // Implicit orderBy adds order for inequality filters
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("foo"))
                            .setDirection(Direction.ASCENDING)
                            .build())
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                            .setDirection(Direction.ASCENDING)))
            .build();

    runQueryRetryTest(testData, expectedRetryRequest);
  }

  private void buildAndRunQueryRetryTest(String fieldName, String fieldValue) throws Exception {
    TestData testData =
        new TestData.Builder()
            .setFilter(TestData.FIELD_EQUALS_BAR)
            .setProjectId(projectId)
            .setOrderFunction(
                f -> {
                  FieldReference f2 = FieldReference.newBuilder().setFieldPath(fieldName).build();
                  return Collections.singletonList(
                      Order.newBuilder().setDirection(Direction.ASCENDING).setField(f2).build());
                })
            .build();
    RunQueryRequest expectedRetryRequest =
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
                            .addValues(Value.newBuilder().setStringValue(fieldValue))
                            .addValues(
                                Value.newBuilder()
                                    .setReferenceValue(testData.response2.getDocument().getName())))
                    .addOrderBy(
                        Order.newBuilder()
                            .setField(FieldReference.newBuilder().setFieldPath("__name__"))
                            .setDirection(Direction.ASCENDING)))
            .build();

    runQueryRetryTest(testData, expectedRetryRequest);
  }

  private void runQueryRetryTest(TestData testData, RunQueryRequest expectedRetryRequest)
      throws Exception {
    when(responseStream.iterator())
        .thenReturn(
            new AbstractIterator<RunQueryResponse>() {
              private int invocationCount = 1;

              @Override
              protected RunQueryResponse computeNext() {
                int count = invocationCount++;
                if (count == 1) {
                  return testData.response1;
                } else if (count == 2) {
                  return testData.response2;
                } else {
                  throw RETRYABLE_ERROR;
                }
              }
            });

    when(callable.call(withReadTime(testData.request, readTime))).thenReturn(responseStream);
    doNothing().when(attempt).checkCanRetry(any(), eq(RETRYABLE_ERROR));
    when(retryResponseStream.iterator())
        .thenReturn(ImmutableList.of(testData.response3).iterator());
    when(callable.call(withReadTime(expectedRetryRequest, readTime)))
        .thenReturn(retryResponseStream);

    when(stub.runQueryCallable()).thenReturn(callable);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newReadAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);

    ArgumentCaptor<RunQueryResponse> responsesCaptor =
        ArgumentCaptor.forClass(RunQueryResponse.class);

    doNothing().when(processContext).output(responsesCaptor.capture());

    when(processContext.element()).thenReturn(testData.request);

    RunQueryFn fn = new RunQueryFn(clock, ff, rpcQosOptions, readTime);

    runFunction(fn);

    verify(callable, times(1)).call(withReadTime(testData.request, readTime));
    verify(callable, times(1)).call(withReadTime(expectedRetryRequest, readTime));
    verify(attempt, times(3)).recordStreamValue(any());

    List<RunQueryResponse> allValues = responsesCaptor.getAllValues();
    assertEquals(
        ImmutableList.of(testData.response1, testData.response2, testData.response3), allValues);
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

    static final FieldReference FILTER_FIELD_PATH =
        FieldReference.newBuilder().setFieldPath("foo").build();
    static final Filter FIELD_EQUALS_BAR =
        Filter.newBuilder()
            .setFieldFilter(
                FieldFilter.newBuilder()
                    .setField(FILTER_FIELD_PATH)
                    .setOp(Operator.EQUAL)
                    .setValue(Value.newBuilder().setStringValue("bar"))
                    .build())
            .build();
    static final Filter FIELD_NOT_EQUALS_FOO =
        Filter.newBuilder()
            .setFieldFilter(
                FieldFilter.newBuilder()
                    .setField(FILTER_FIELD_PATH)
                    .setOp(Operator.NOT_EQUAL)
                    .setValue(Value.newBuilder().setStringValue("foo"))
                    .build())
            .build();

    private final RunQueryRequest request;
    private final RunQueryResponse response1;
    private final RunQueryResponse response2;
    private final RunQueryResponse response3;

    public TestData(
        String projectId, Function<FieldReference, List<Order>> orderFunction, Filter filter) {
      StructuredQuery.Builder builder =
          StructuredQuery.newBuilder()
              .addFrom(
                  CollectionSelector.newBuilder()
                      .setAllDescendants(false)
                      .setCollectionId("collection"))
              .setWhere(filter);

      orderFunction.apply(FILTER_FIELD_PATH).forEach(builder::addOrderBy);
      request =
          RunQueryRequest.newBuilder()
              .setParent(String.format("projects/%s/databases/(default)/document", projectId))
              .setStructuredQuery(builder)
              .build();

      response1 = newResponse(1);
      response2 = newResponse(2);
      response3 = newResponse(3);
    }

    /**
     * Returns single-document response like this: { "__name__": "doc-{docNumber}", "foo": "bar",
     * "fo`o.m`ap": { "bar.key": "bar.val" }, "baz" : { "qux" : "val" }, "quux.quuz" : "123" }.
     */
    private static RunQueryResponse newResponse(int docNumber) {
      String docId = String.format("doc-%d", docNumber);
      return RunQueryResponse.newBuilder()
          .setDocument(
              Document.newBuilder()
                  .setName(docId)
                  .putAllFields(
                      ImmutableMap.of(
                          "foo",
                          Value.newBuilder().setStringValue("bar").build(),
                          "fo`o.m`ap",
                          Value.newBuilder()
                              .setMapValue(
                                  MapValue.newBuilder()
                                      .putFields(
                                          "bar.key",
                                          Value.newBuilder().setStringValue("bar.val").build())
                                      .build())
                              .build(),
                          "baz",
                          Value.newBuilder()
                              .setMapValue(
                                  MapValue.newBuilder()
                                      .putFields(
                                          "qux", Value.newBuilder().setStringValue("val").build())
                                      .build())
                              .build(),
                          "quux.quuz",
                          Value.newBuilder().setStringValue("123").build())))
          .build();
    }

    @SuppressWarnings("initialization.fields.uninitialized") // fields set via builder methods
    private static final class Builder {

      private String projectId;
      private Function<FieldReference, List<Order>> orderFunction;
      private Filter filter;

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

      public Builder setFilter(Filter filter) {
        this.filter = filter;
        return this;
      }

      private TestData build() {
        return new TestData(
            requireNonNull(projectId, "projectId must be non null"),
            requireNonNull(orderFunction, "orderFunction must be non null"),
            requireNonNull(filter, "filter must be non-null"));
      }
    }
  }
}
