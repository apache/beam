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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.firestore.v1.ArrayValue;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.DocumentTransform;
import com.google.firestore.v1.DocumentTransform.FieldTransform;
import com.google.firestore.v1.Precondition;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.rpc.Code;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.FailedWritesException;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteFailure;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.WriteSuccessSummary;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BaseBatchWriteFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BatchWriteFnWithSummary;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.WriteElement;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings({
  "initialization.fields.uninitialized", // mockito fields are initialized via the Mockito Runner
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class FirestoreV1FnBatchWriteWithSummaryTest
    extends BaseFirestoreV1WriteFnTest<WriteSuccessSummary, BatchWriteFnWithSummary> {

  @After
  public void tearDown() {
    verify(processContext, never()).output(any());
  }

  @Override
  @Test
  public void enqueueingWritesValidateBytesSize() throws Exception {
    int maxBytes = 50;
    RpcQosOptions options = rpcQosOptions.toBuilder().withBatchMaxBytes(maxBytes).build();

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any()))
        .thenReturn(FirestoreStatefulComponentFactory.INSTANCE.getRpcQos(options));

    byte[] bytes = new byte[maxBytes + 1];
    SecureRandom.getInstanceStrong().nextBytes(bytes);
    byte[] base64Bytes = Base64.getEncoder().encode(bytes);
    String base64String = Base64.getEncoder().encodeToString(bytes);

    Value largeValue =
        Value.newBuilder().setStringValueBytes(ByteString.copyFrom(base64Bytes)).build();

    // apply a doc transform that is too large
    Write write1 =
        Write.newBuilder()
            .setTransform(
                DocumentTransform.newBuilder()
                    .setDocument(String.format("doc-%03d", 2))
                    .addFieldTransforms(
                        FieldTransform.newBuilder()
                            .setAppendMissingElements(
                                ArrayValue.newBuilder().addValues(largeValue))))
            .build();
    // delete a doc that is too large
    Write write2 =
        Write.newBuilder().setDelete(String.format("doc-%03d_%s", 3, base64String)).build();
    // update a doc that is too large
    Write write3 =
        Write.newBuilder()
            .setUpdate(
                Document.newBuilder()
                    .setName(String.format("doc-%03d", 4))
                    .putAllFields(ImmutableMap.of("foo", largeValue)))
            .build();

    BatchWriteFnWithSummary fn =
        getFn(
            clock, ff, options, metricsFixture.counterFactory, metricsFixture.distributionFactory);
    fn.populateDisplayData(displayDataBuilder);
    fn.setup();
    fn.startBundle(startBundleContext);
    try {
      when(processContext.element()).thenReturn(write1);
      fn.processElement(processContext, window);
      fail("expected validation error");
    } catch (FailedWritesException e) {
      WriteFailure failure = e.getWriteFailures().get(0);
      assertNotNull(failure);
      String message = failure.getStatus().getMessage();
      assertTrue(message.contains("TRANSFORM"));
      assertTrue(message.contains("larger than configured max allowed bytes per batch"));
    }
    try {
      when(processContext.element()).thenReturn(write2);
      fn.processElement(processContext, window);
      fail("expected validation error");
    } catch (FailedWritesException e) {
      WriteFailure failure = e.getWriteFailures().get(0);
      assertNotNull(failure);
      String message = failure.getStatus().getMessage();
      assertTrue(message.contains("DELETE"));
      assertTrue(message.contains("larger than configured max allowed bytes per batch"));
    }
    try {
      when(processContext.element()).thenReturn(write3);
      fn.processElement(processContext, window);
      fail("expected validation error");
    } catch (FailedWritesException e) {
      WriteFailure failure = e.getWriteFailures().get(0);
      assertNotNull(failure);
      String message = failure.getStatus().getMessage();
      assertTrue(message.contains("UPDATE"));
      assertTrue(message.contains("larger than configured max allowed bytes per batch"));
    }

    assertEquals(0, fn.writes.size());
  }

  @Test
  public void nonRetryableWriteResultStopsAttempts() throws Exception {
    Write write0 = FirestoreProtoHelpers.newWrite(0);
    Write write1 =
        FirestoreProtoHelpers.newWrite(1)
            .toBuilder()
            .setCurrentDocument(Precondition.newBuilder().setExists(false).build())
            .build();

    BatchWriteRequest expectedRequest1 =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write0)
            .addWrites(write1)
            .build();

    BatchWriteResponse response1 =
        BatchWriteResponse.newBuilder()
            .addStatus(STATUS_OK)
            .addWriteResults(
                WriteResult.newBuilder()
                    .setUpdateTime(Timestamp.newBuilder().setSeconds(1).build())
                    .build())
            .addStatus(statusForCode(Code.ALREADY_EXISTS))
            .addWriteResults(WriteResult.newBuilder().build())
            .build();

    RpcQosOptions options =
        rpcQosOptions.toBuilder().withMaxAttempts(1).withBatchMaxCount(2).build();

    when(processContext.element())
        .thenReturn(write0, write1)
        .thenThrow(new IllegalStateException("too many calls"));

    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(any()))
        .thenReturn(newFlushBuffer(options))
        .thenReturn(newFlushBuffer(options))
        .thenThrow(new IllegalStateException("too many attempt#newFlushBuffer calls"));
    when(attempt.isCodeRetryable(Code.ALREADY_EXISTS)).thenReturn(false);

    ArgumentCaptor<BatchWriteRequest> requestCaptor1 =
        ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor1.capture())).thenReturn(response1);

    BaseBatchWriteFn<WriteSuccessSummary> fn =
        new BatchWriteFnWithSummary(clock, ff, options, CounterFactory.DEFAULT);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window); // write0
    try {
      fn.processElement(processContext, window); // write1
      fail("expected an exception when trying to apply a write with a failed precondition");
    } catch (FailedWritesException e) {
      List<WriteFailure> writeFailures = e.getWriteFailures();
      assertEquals(1, writeFailures.size());
      WriteFailure failure = writeFailures.get(0);
      assertEquals(Code.ALREADY_EXISTS.getNumber(), failure.getStatus().getCode());
      assertEquals(write1, failure.getWrite());
      assertEquals(WriteResult.getDefaultInstance(), failure.getWriteResult());
    }

    assertEquals(expectedRequest1, requestCaptor1.getValue());

    List<WriteElement> actualWrites = new ArrayList<>(fn.writes);

    Instant flush1Attempt1Begin = Instant.ofEpochMilli(1);
    Instant flush1Attempt1RpcStart = Instant.ofEpochMilli(2);
    Instant flush1Attempt1RpcEnd = Instant.ofEpochMilli(3);

    assertTrue(actualWrites.isEmpty());

    fn.finishBundle(finishBundleContext);

    verify(attempt, times(1)).newFlushBuffer(flush1Attempt1Begin);
    verify(attempt, times(1)).recordRequestStart(flush1Attempt1RpcStart, 2);
    verify(attempt, times(1)).recordWriteCounts(flush1Attempt1RpcEnd, 1, 1);

    verify(attempt, never()).completeSuccess();
    verify(callable, times(1)).call(any());
    verifyNoMoreInteractions(callable);
  }

  @Override
  protected BatchWriteFnWithSummary getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory ff,
      RpcQosOptions rpcQosOptions,
      CounterFactory counterFactory,
      DistributionFactory distributionFactory) {
    return new BatchWriteFnWithSummary(clock, ff, rpcQosOptions, counterFactory);
  }
}
