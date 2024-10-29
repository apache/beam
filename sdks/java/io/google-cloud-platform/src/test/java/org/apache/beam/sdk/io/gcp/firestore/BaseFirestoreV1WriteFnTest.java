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

import static org.apache.beam.sdk.io.gcp.firestore.FirestoreProtoHelpers.newWrite;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.BatchWriteResponse;
import com.google.firestore.v1.Write;
import com.google.firestore.v1.WriteResult;
import com.google.rpc.Code;
import com.google.rpc.Status;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BaseBatchWriteFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.WriteElement;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.Element;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcWriteAttempt.FlushBuffer;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosImpl.FlushBufferImpl;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
public abstract class BaseFirestoreV1WriteFnTest<
        OutT, FnT extends BaseBatchWriteFn<OutT> & HasRpcAttemptContext>
    extends BaseFirestoreV1FnTest<Write, OutT, FnT> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFirestoreV1WriteFnTest.class);

  protected static final Status STATUS_OK =
      Status.newBuilder().setCode(Code.OK.getNumber()).build();
  protected static final Status STATUS_DEADLINE_EXCEEDED =
      Status.newBuilder().setCode(Code.DEADLINE_EXCEEDED.getNumber()).build();

  @Mock(lenient = true)
  protected BoundedWindow window;

  @Mock protected DoFn<Write, OutT>.FinishBundleContext finishBundleContext;
  @Mock protected UnaryCallable<BatchWriteRequest, BatchWriteResponse> callable;
  @Mock protected RpcQos.RpcWriteAttempt attempt;
  @Mock protected RpcQos.RpcWriteAttempt attempt2;

  protected MetricsFixture metricsFixture;

  @Before
  public final void setUp() {
    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt, attempt2);

    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(ff.getFirestoreStub(pipelineOptions)).thenReturn(stub);
    when(stub.batchWriteCallable()).thenReturn(callable);
    metricsFixture = new MetricsFixture();
  }

  @Override
  @Test
  public final void attemptsExhaustedForRetryableError() throws Exception {
    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpc1Start = Instant.ofEpochMilli(1);
    Instant rpc1End = Instant.ofEpochMilli(2);
    Instant rpc2Start = Instant.ofEpochMilli(3);
    Instant rpc2End = Instant.ofEpochMilli(4);
    Instant rpc3Start = Instant.ofEpochMilli(5);
    Instant rpc3End = Instant.ofEpochMilli(6);
    Write write = newWrite();
    Element<Write> element1 = new WriteElement(0, write, window);

    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newWriteAttempt(FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.BatchWrite))
        .thenReturn(attempt);
    when(stub.batchWriteCallable()).thenReturn(callable);

    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(rpcQosOptions));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    when(flushBuffer.offer(element1)).thenReturn(true);
    when(flushBuffer.iterator()).thenReturn(newArrayList(element1).iterator());
    when(flushBuffer.getBufferedElementsCount()).thenReturn(1);
    when(flushBuffer.isFull()).thenReturn(true);

    when(callable.call(any())).thenThrow(RETRYABLE_ERROR, RETRYABLE_ERROR, RETRYABLE_ERROR);
    doNothing().when(attempt).recordWriteCounts(any(), anyInt(), anyInt());
    doNothing()
        .doNothing()
        .doThrow(RETRYABLE_ERROR)
        .when(attempt)
        .checkCanRetry(any(), eq(RETRYABLE_ERROR));

    when(processContext.element()).thenReturn(write);

    try {
      runFunction(
          getFn(clock, ff, rpcQosOptions, CounterFactory.DEFAULT, DistributionFactory.DEFAULT));
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (ApiException e) {
      assertSame(RETRYABLE_ERROR, e);
    }

    verify(attempt, times(1)).awaitSafeToProceed(attemptStart);
    verify(attempt, times(1)).recordRequestStart(rpc1Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc1End, 0, 1);
    verify(attempt, times(1)).recordRequestStart(rpc2Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc2End, 0, 1);
    verify(attempt, times(1)).recordRequestStart(rpc3Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc3End, 0, 1);
    verify(attempt, times(0)).recordWriteCounts(any(), gt(0), anyInt());
    verify(attempt, never()).completeSuccess();
  }

  @Override
  @Test
  public final void noRequestIsSentIfNotSafeToProceed() throws Exception {
    when(ff.getFirestoreStub(any())).thenReturn(stub);
    when(ff.getRpcQos(any())).thenReturn(rpcQos);
    when(rpcQos.newWriteAttempt(FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext.BatchWrite))
        .thenReturn(attempt);

    InterruptedException interruptedException = new InterruptedException();
    when(attempt.awaitSafeToProceed(any())).thenReturn(false).thenThrow(interruptedException);

    when(processContext.element()).thenReturn(newWrite());

    try {
      runFunction(
          getFn(clock, ff, rpcQosOptions, CounterFactory.DEFAULT, DistributionFactory.DEFAULT));
      fail("Expected ApiException to be throw after exhausted attempts");
    } catch (InterruptedException e) {
      assertSame(interruptedException, e);
    }

    verify(stub, times(1)).close();
    verifyNoMoreInteractions(stub);
    verifyNoMoreInteractions(callable);
    verify(attempt, times(0)).recordWriteCounts(any(), anyInt(), anyInt());
  }

  @Test
  public abstract void enqueueingWritesValidateBytesSize() throws Exception;

  @Test
  public final void endToEnd_success() throws Exception {

    Write write = newWrite();
    BatchWriteRequest expectedRequest =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write)
            .build();

    BatchWriteResponse response = BatchWriteResponse.newBuilder().addStatus(STATUS_OK).build();

    Element<Write> element1 = new WriteElement(0, write, window);

    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpcStart = Instant.ofEpochMilli(1);
    Instant rpcEnd = Instant.ofEpochMilli(2);

    RpcQosOptions options = rpcQosOptions.toBuilder().withBatchMaxCount(1).build();
    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(options));
    when(processContext.element()).thenReturn(write);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    ArgumentCaptor<BatchWriteRequest> requestCaptor =
        ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor.capture())).thenReturn(response);

    runFunction(getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT));

    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(flushBuffer, times(1)).offer(element1);
    verify(flushBuffer, times(1)).isFull();
    verify(attempt, times(1)).recordRequestStart(rpcStart, 1);
    verify(attempt, times(1)).recordWriteCounts(rpcEnd, 1, 0);
    verify(attempt, never()).recordWriteCounts(any(), anyInt(), gt(0));
    verify(attempt, never()).checkCanRetry(any(), any());
  }

  @Test
  public final void endToEnd_exhaustingAttemptsResultsInException() throws Exception {
    ApiException err1 =
        ApiExceptionFactory.createException(
            new IOException("err1"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED), false);
    ApiException err2 =
        ApiExceptionFactory.createException(
            new IOException("err2"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED), false);
    ApiException err3 =
        ApiExceptionFactory.createException(
            new IOException("err3"), GrpcStatusCode.of(io.grpc.Status.Code.ABORTED), false);

    Instant attemptStart = Instant.ofEpochMilli(0);
    Instant rpc1Start = Instant.ofEpochMilli(1);
    Instant rpc1End = Instant.ofEpochMilli(2);
    Instant rpc2Start = Instant.ofEpochMilli(3);
    Instant rpc2End = Instant.ofEpochMilli(4);
    Instant rpc3Start = Instant.ofEpochMilli(5);
    Instant rpc3End = Instant.ofEpochMilli(6);

    Write write = newWrite();

    Element<Write> element1 = new WriteElement(0, write, window);

    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(rpcQosOptions));
    when(processContext.element()).thenReturn(write);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(attemptStart)).thenReturn(flushBuffer);
    when(flushBuffer.isFull()).thenReturn(true);
    when(flushBuffer.offer(element1)).thenReturn(true);
    when(flushBuffer.iterator()).thenReturn(newArrayList(element1).iterator());
    when(flushBuffer.getBufferedElementsCount()).thenReturn(1);
    when(callable.call(any())).thenThrow(err1, err2, err3);
    doNothing().when(attempt).checkCanRetry(any(), eq(err1));
    doNothing().when(attempt).checkCanRetry(any(), eq(err2));
    doThrow(err3).when(attempt).checkCanRetry(any(), eq(err3));

    try {
      FnT fn = getFn(clock, ff, rpcQosOptions, CounterFactory.DEFAULT, DistributionFactory.DEFAULT);
      runFunction(fn);
      fail("Expected exception");
    } catch (ApiException e) {
      assertNotNull(e.getMessage());
      assertTrue(e.getMessage().contains("err3"));
    }

    verify(flushBuffer, times(1)).offer(element1);
    verify(flushBuffer, atLeastOnce()).isFull();
    verify(attempt, times(1)).recordRequestStart(rpc1Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc1End, 0, 1);
    verify(attempt, times(1)).recordRequestStart(rpc2Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc2End, 0, 1);
    verify(attempt, times(1)).recordRequestStart(rpc3Start, 1);
    verify(attempt, times(1)).recordWriteCounts(rpc3End, 0, 1);
    verify(attempt, never()).recordWriteCounts(any(), gt(0), anyInt());
    verify(attempt, never()).completeSuccess();
  }

  @Test
  public final void endToEnd_awaitSafeToProceed_falseIsTerminalForAttempt() throws Exception {
    RpcQosOptions options = rpcQosOptions.toBuilder().withBatchMaxCount(2).build();

    Instant rpc1Start = Instant.ofEpochMilli(3);
    Instant rpc1End = Instant.ofEpochMilli(4);
    ArgumentCaptor<BatchWriteRequest> requestCaptor =
        ArgumentCaptor.forClass(BatchWriteRequest.class);

    Write write = newWrite();
    BatchWriteRequest expectedRequest =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write)
            .build();
    BatchWriteResponse response = BatchWriteResponse.newBuilder().addStatus(STATUS_OK).build();

    when(processContext.element()).thenReturn(write);
    // process element attempt 1
    when(attempt.awaitSafeToProceed(any()))
        .thenReturn(false)
        .thenThrow(new IllegalStateException("too many attempt1#awaitSafeToProceed"));
    // process element attempt 2
    when(attempt2.awaitSafeToProceed(any()))
        .thenReturn(true)
        .thenThrow(new IllegalStateException("too many attempt2#awaitSafeToProceed"));
    when(attempt2.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));
    // finish bundle attempt
    RpcQos.RpcWriteAttempt finishBundleAttempt = mock(RpcWriteAttempt.class);
    when(finishBundleAttempt.awaitSafeToProceed(any()))
        .thenReturn(true, true)
        .thenThrow(new IllegalStateException("too many finishBundleAttempt#awaitSafeToProceed"));
    when(finishBundleAttempt.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));
    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt, attempt2, finishBundleAttempt);
    when(callable.call(requestCaptor.capture())).thenReturn(response);

    FnT fn = getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT);
    runFunction(fn);

    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(attempt, times(1)).awaitSafeToProceed(any());
    verifyNoMoreInteractions(attempt);
    verify(attempt2, times(1)).awaitSafeToProceed(any());
    verify(attempt2, times(1)).newFlushBuffer(any());
    verifyNoMoreInteractions(attempt2);
    verify(finishBundleAttempt, times(1)).recordRequestStart(rpc1Start, 1);
    verify(finishBundleAttempt, times(1)).recordWriteCounts(rpc1End, 1, 0);
    verify(finishBundleAttempt, times(1)).completeSuccess();
    verify(finishBundleAttempt, never()).checkCanRetry(any(), any());
  }

  @Test
  public final void endToEnd_deadlineExceededOnAnIndividualWriteResultsInThrottling()
      throws Exception {
    final long totalDocCount = 1_000_000;
    final int numWorkers = 100;
    final long docCount = totalDocCount / numWorkers;
    LOG.info("docCount = {}", docCount);
    RpcQosOptions options =
        rpcQosOptions
            .toBuilder()
            .withHintMaxNumWorkers(numWorkers)
            .withSamplePeriod(Duration.standardMinutes(10))
            // .withBatchInitialCount(5)
            .withReportDiagnosticMetrics()
            .build();
    LOG.debug("options = {}", options);

    FirestoreStatefulComponentFactory ff = mock(FirestoreStatefulComponentFactory.class);
    when(ff.getFirestoreStub(any())).thenReturn(stub);
    Random random = new Random(12345);
    TestClock clock = new TestClock(Instant.EPOCH, Duration.standardSeconds(1));
    Sleeper sleeper = millis -> clock.setNext(advanceClockBy(Duration.millis(millis)));
    RpcQosImpl qos =
        new RpcQosImpl(
            options,
            random,
            sleeper,
            metricsFixture.counterFactory,
            metricsFixture.distributionFactory);
    RpcQos qosSpy =
        mock(
            RpcQos.class,
            invocation -> {
              Method method = invocation.getMethod();
              LOG.debug("method = {}", method);
              Method actualMethod =
                  qos.getClass().getMethod(method.getName(), method.getParameterTypes());
              return actualMethod.invoke(qos, invocation.getArguments());
            });

    when(ff.getRpcQos(options)).thenReturn(qosSpy);

    int defaultDocumentWriteLatency = 30;
    final AtomicLong writeCounter = new AtomicLong();
    when(processContext.element())
        .thenAnswer(invocation -> newWrite(writeCounter.getAndIncrement()));
    when(callable.call(any()))
        .thenAnswer(
            new Answer<BatchWriteResponse>() {
              private final Random rand = new Random(84572908);
              private final Instant threshold =
                  Instant.ofEpochMilli(Duration.standardMinutes(20).getMillis());

              @Override
              public BatchWriteResponse answer(InvocationOnMock invocation) throws Throwable {
                BatchWriteRequest request = invocation.getArgument(0, BatchWriteRequest.class);
                LOG.debug("request = {}", request);
                long requestDurationMs = 0;
                BatchWriteResponse.Builder builder = BatchWriteResponse.newBuilder();
                for (Write ignored : request.getWritesList()) {
                  builder.addWriteResults(WriteResult.newBuilder().build());
                  if (clock.prev.isBefore(threshold)) {
                    requestDurationMs += defaultDocumentWriteLatency;
                    builder.addStatus(STATUS_OK);
                  } else {
                    int latency = rand.nextInt(1500);
                    LOG.debug("latency = {}", latency);
                    if (latency > 300) {
                      builder.addStatus(STATUS_DEADLINE_EXCEEDED);
                    } else {
                      builder.addStatus(STATUS_OK);
                    }
                    requestDurationMs += latency;
                  }
                }
                clock.setNext(advanceClockBy(Duration.millis(requestDurationMs)));
                return builder.build();
              }
            });
    LOG.info(
        "### parameters: {defaultDocumentWriteLatency: {}, rpcQosOptions: {}}",
        defaultDocumentWriteLatency,
        options);

    FnT fn =
        getFn(
            clock, ff, options, metricsFixture.counterFactory, metricsFixture.distributionFactory);
    fn.setup();
    fn.startBundle(startBundleContext);
    while (writeCounter.get() < docCount) {
      fn.processElement(processContext, window);
    }
    fn.finishBundle(finishBundleContext);

    LOG.info("writeCounter = {}", writeCounter.get());
    LOG.info("clock.prev = {}", clock.prev);

    MyDistribution qosAdaptiveThrottlerThrottlingMs =
        metricsFixture.distributions.get("qos_adaptiveThrottler_throttlingMs");
    assertNotNull(qosAdaptiveThrottlerThrottlingMs);
    List<Long> updateInvocations = qosAdaptiveThrottlerThrottlingMs.updateInvocations;
    assertFalse(updateInvocations.isEmpty());
  }

  @Test
  public final void endToEnd_maxBatchSizeRespected() throws Exception {

    Instant enqueue0 = Instant.ofEpochMilli(0);
    Instant enqueue1 = Instant.ofEpochMilli(1);
    Instant enqueue2 = Instant.ofEpochMilli(2);
    Instant enqueue3 = Instant.ofEpochMilli(3);
    Instant enqueue4 = Instant.ofEpochMilli(4);
    Instant group1Rpc1Start = Instant.ofEpochMilli(5);
    Instant group1Rpc1End = Instant.ofEpochMilli(6);

    Instant enqueue5 = Instant.ofEpochMilli(7);
    Instant finalFlush = Instant.ofEpochMilli(8);
    Instant group2Rpc1Start = Instant.ofEpochMilli(9);
    Instant group2Rpc1End = Instant.ofEpochMilli(10);

    Write write0 = newWrite(0);
    Write write1 = newWrite(1);
    Write write2 = newWrite(2);
    Write write3 = newWrite(3);
    Write write4 = newWrite(4);
    Write write5 = newWrite(5);
    int maxValuesPerGroup = 5;

    BatchWriteRequest.Builder builder =
        BatchWriteRequest.newBuilder().setDatabase("projects/testing-project/databases/(default)");

    BatchWriteRequest expectedGroup1Request =
        builder
            .build()
            .toBuilder()
            .addWrites(write0)
            .addWrites(write1)
            .addWrites(write2)
            .addWrites(write3)
            .addWrites(write4)
            .build();

    BatchWriteRequest expectedGroup2Request = builder.build().toBuilder().addWrites(write5).build();

    BatchWriteResponse group1Response =
        BatchWriteResponse.newBuilder()
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .build();

    BatchWriteResponse group2Response =
        BatchWriteResponse.newBuilder().addStatus(STATUS_OK).build();

    RpcQosOptions options = rpcQosOptions.toBuilder().withBatchMaxCount(maxValuesPerGroup).build();
    FlushBuffer<Element<Write>> flushBuffer = spy(newFlushBuffer(options));
    FlushBuffer<Element<Write>> flushBuffer2 = spy(newFlushBuffer(options));

    when(processContext.element()).thenReturn(write0, write1, write2, write3, write4, write5);

    when(rpcQos.newWriteAttempt(any()))
        .thenReturn(attempt, attempt, attempt, attempt, attempt, attempt2, attempt2, attempt2)
        .thenThrow(new IllegalStateException("too many attempts"));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt2.awaitSafeToProceed(any())).thenReturn(true);

    when(attempt.<Element<Write>>newFlushBuffer(enqueue0)).thenReturn(newFlushBuffer(options));
    when(attempt.<Element<Write>>newFlushBuffer(enqueue1)).thenReturn(newFlushBuffer(options));
    when(attempt.<Element<Write>>newFlushBuffer(enqueue2)).thenReturn(newFlushBuffer(options));
    when(attempt.<Element<Write>>newFlushBuffer(enqueue3)).thenReturn(newFlushBuffer(options));
    when(attempt.<Element<Write>>newFlushBuffer(enqueue4)).thenReturn(flushBuffer);
    when(callable.call(expectedGroup1Request)).thenReturn(group1Response);

    when(attempt2.<Element<Write>>newFlushBuffer(enqueue5)).thenReturn(newFlushBuffer(options));
    when(attempt2.<Element<Write>>newFlushBuffer(finalFlush)).thenReturn(flushBuffer2);
    when(callable.call(expectedGroup2Request)).thenReturn(group2Response);

    runFunction(
        getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT),
        maxValuesPerGroup + 1);

    verify(attempt, times(1)).recordRequestStart(group1Rpc1Start, 5);
    verify(attempt, times(1)).recordWriteCounts(group1Rpc1End, 5, 0);
    verify(attempt, times(1)).completeSuccess();
    verify(attempt2, times(1)).recordRequestStart(group2Rpc1Start, 1);
    verify(attempt2, times(1)).recordWriteCounts(group2Rpc1End, 1, 0);
    verify(attempt2, times(1)).completeSuccess();
    verify(callable, times(1)).call(expectedGroup1Request);
    verify(callable, times(1)).call(expectedGroup2Request);
    verifyNoMoreInteractions(callable);
    verify(flushBuffer, times(maxValuesPerGroup)).offer(any());
    verify(flushBuffer2, times(1)).offer(any());
  }

  @Test
  public final void endToEnd_partialSuccessReturnsWritesToQueue() throws Exception {
    Write write0 = newWrite(0);
    Write write1 = newWrite(1);
    Write write2 = newWrite(2);
    Write write3 = newWrite(3);
    Write write4 = newWrite(4);

    BatchWriteRequest expectedRequest1 =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write0)
            .addWrites(write1)
            .addWrites(write2)
            .addWrites(write3)
            .addWrites(write4)
            .build();

    BatchWriteResponse response1 =
        BatchWriteResponse.newBuilder()
            .addStatus(STATUS_OK)
            .addStatus(statusForCode(Code.INVALID_ARGUMENT))
            .addStatus(statusForCode(Code.FAILED_PRECONDITION))
            .addStatus(statusForCode(Code.UNAUTHENTICATED))
            .addStatus(STATUS_OK)
            .build();

    BatchWriteRequest expectedRequest2 =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write1)
            .addWrites(write2)
            .addWrites(write3)
            .build();

    BatchWriteResponse response2 =
        BatchWriteResponse.newBuilder()
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .addStatus(STATUS_OK)
            .build();

    RpcQosOptions options =
        rpcQosOptions.toBuilder().withMaxAttempts(1).withBatchMaxCount(5).build();

    when(processContext.element())
        .thenReturn(write0, write1, write2, write3, write4)
        .thenThrow(new IllegalStateException("too many calls"));

    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));
    when(attempt.isCodeRetryable(Code.INVALID_ARGUMENT)).thenReturn(true);
    when(attempt.isCodeRetryable(Code.FAILED_PRECONDITION)).thenReturn(true);
    when(attempt.isCodeRetryable(Code.UNAUTHENTICATED)).thenReturn(true);

    ArgumentCaptor<BatchWriteRequest> requestCaptor1 =
        ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor1.capture())).thenReturn(response1);

    FnT fn = getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window); // write0
    fn.processElement(processContext, window); // write1
    fn.processElement(processContext, window); // write2
    fn.processElement(processContext, window); // write3
    fn.processElement(processContext, window); // write4

    assertEquals(expectedRequest1, requestCaptor1.getValue());

    List<WriteElement> expectedRemainingWrites =
        newArrayList(
            new WriteElement(1, write1, window),
            new WriteElement(2, write2, window),
            new WriteElement(3, write3, window));
    List<WriteElement> actualWrites = new ArrayList<>(fn.writes);

    assertEquals(expectedRemainingWrites, actualWrites);
    assertEquals(5, fn.queueNextEntryPriority);

    ArgumentCaptor<BatchWriteRequest> requestCaptor2 =
        ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor2.capture())).thenReturn(response2);
    fn.finishBundle(finishBundleContext);
    assertEquals(expectedRequest2, requestCaptor2.getValue());

    assertEquals(0, fn.queueNextEntryPriority);

    verify(attempt, times(1)).recordRequestStart(any(), eq(5));
    verify(attempt, times(1)).recordWriteCounts(any(), eq(2), eq(3));

    verify(attempt, times(1)).recordRequestStart(any(), eq(3));
    verify(attempt, times(1)).recordWriteCounts(any(), eq(3), eq(0));
    verify(attempt, times(1)).completeSuccess();
    verify(callable, times(2)).call(any());
    verifyNoMoreInteractions(callable);
  }

  @Test
  public final void writesRemainInQueueWhenFlushIsNotReadyAndThenFlushesInFinishBundle()
      throws Exception {
    RpcQosOptions options = rpcQosOptions.toBuilder().withMaxAttempts(1).build();

    Write write = newWrite();
    BatchWriteRequest expectedRequest =
        BatchWriteRequest.newBuilder()
            .setDatabase("projects/testing-project/databases/(default)")
            .addWrites(write)
            .build();
    BatchWriteResponse response = BatchWriteResponse.newBuilder().addStatus(STATUS_OK).build();

    when(processContext.element())
        .thenReturn(write)
        .thenThrow(new IllegalStateException("too many element calls"));
    when(rpcQos.newWriteAttempt(any()))
        .thenReturn(attempt, attempt2)
        .thenThrow(new IllegalStateException("too many attempt calls"));
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt2.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));
    when(attempt2.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));

    FnT fn = getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT);
    fn.populateDisplayData(displayDataBuilder);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window);

    assertEquals(1, fn.writes.size());
    verify(attempt, never()).recordWriteCounts(any(), anyInt(), anyInt());
    verify(attempt, never()).checkCanRetry(any(), any());
    verify(attempt, never()).completeSuccess();

    Instant attempt2RpcStart = Instant.ofEpochMilli(2);
    Instant attempt2RpcEnd = Instant.ofEpochMilli(3);

    ArgumentCaptor<BatchWriteRequest> requestCaptor =
        ArgumentCaptor.forClass(BatchWriteRequest.class);
    when(callable.call(requestCaptor.capture())).thenReturn(response);
    fn.finishBundle(finishBundleContext);

    assertEquals(0, fn.writes.size());
    assertEquals(expectedRequest, requestCaptor.getValue());
    verify(attempt2, times(1)).recordRequestStart(attempt2RpcStart, 1);
    verify(attempt2, times(1)).recordWriteCounts(attempt2RpcEnd, 1, 0);
    verify(attempt2, never()).recordWriteCounts(any(), anyInt(), gt(0));
    verify(attempt2, never()).checkCanRetry(any(), any());
    verify(attempt2, times(1)).completeSuccess();
  }

  @Test
  public final void queuedWritesMaintainPriorityIfNotFlushed() throws Exception {
    RpcQosOptions options = rpcQosOptions.toBuilder().withMaxAttempts(1).build();

    Write write0 = newWrite(0);
    Write write1 = newWrite(1);
    Write write2 = newWrite(2);
    Write write3 = newWrite(3);
    Write write4 = newWrite(4);
    Instant write4Start = Instant.ofEpochMilli(4);

    when(processContext.element())
        .thenReturn(write0, write1, write2, write3, write4)
        .thenThrow(new IllegalStateException("too many calls"));

    when(rpcQos.newWriteAttempt(any())).thenReturn(attempt);
    when(attempt.awaitSafeToProceed(any())).thenReturn(true);
    when(attempt.<Element<Write>>newFlushBuffer(any()))
        .thenAnswer(invocation -> newFlushBuffer(options));

    FnT fn = getFn(clock, ff, options, CounterFactory.DEFAULT, DistributionFactory.DEFAULT);
    fn.setup();
    fn.startBundle(startBundleContext);
    fn.processElement(processContext, window); // write0
    fn.processElement(processContext, window); // write1
    fn.processElement(processContext, window); // write2
    fn.processElement(processContext, window); // write3
    fn.processElement(processContext, window); // write4

    List<WriteElement> expectedWrites =
        newArrayList(
            new WriteElement(0, write0, window),
            new WriteElement(1, write1, window),
            new WriteElement(2, write2, window),
            new WriteElement(3, write3, window),
            new WriteElement(4, write4, window));
    List<WriteElement> actualWrites = new ArrayList<>(fn.writes);

    assertEquals(expectedWrites, actualWrites);
    assertEquals(5, fn.queueNextEntryPriority);
    verify(attempt, times(1)).newFlushBuffer(write4Start);
    verifyNoMoreInteractions(callable);
  }

  @Override
  protected final FnT getFn() {
    return getFn(
        JodaClock.DEFAULT,
        FirestoreStatefulComponentFactory.INSTANCE,
        rpcQosOptions,
        CounterFactory.DEFAULT,
        DistributionFactory.DEFAULT);
  }

  protected abstract FnT getFn(
      JodaClock clock,
      FirestoreStatefulComponentFactory ff,
      RpcQosOptions rpcQosOptions,
      CounterFactory counterFactory,
      DistributionFactory distributionFactory);

  @Override
  protected final void processElementsAndFinishBundle(FnT fn, int processElementCount)
      throws Exception {
    try {
      for (int i = 0; i < processElementCount; i++) {
        fn.processElement(processContext, window);
      }
    } finally {
      fn.finishBundle(finishBundleContext);
    }
  }

  protected FlushBufferImpl<Element<Write>> newFlushBuffer(RpcQosOptions options) {
    return new FlushBufferImpl<>(options.getBatchMaxCount(), options.getBatchMaxBytes());
  }

  protected static Status statusForCode(Code code) {
    return Status.newBuilder().setCode(code.getNumber()).build();
  }

  private static Function<Instant, Instant> advanceClockBy(Duration duration) {
    return (i) -> i.withDurationAdded(duration, 1);
  }

  private static class TestClock implements JodaClock {
    private final Function<Instant, Instant> defaultNext;

    private Function<Instant, Instant> next;
    private Instant prev;

    public TestClock(Instant start, Duration defaultInterval) {
      prev = start;
      defaultNext = advanceClockBy(defaultInterval);
    }

    public TestClock setNext(Function<Instant, Instant> next) {
      this.next = next;
      return this;
    }

    @Override
    public Instant instant() {
      final Instant ret;
      if (next != null) {
        ret = next.apply(prev);
        next = null;
      } else {
        ret = defaultNext.apply(prev);
      }
      prev = ret;
      LOG.trace("{} testClock:instant:{}", METRIC_MARKER, ret.toString());
      return ret;
    }
  }

  private static final String METRIC_MARKER = "XXX";

  public static final class MetricsFixture {
    final Map<String, MyCounter> counters;
    final Map<String, MyDistribution> distributions;
    final CounterFactory counterFactory;
    final DistributionFactory distributionFactory;

    public MetricsFixture() {
      counters = new HashMap<>();
      distributions = new HashMap<>();
      counterFactory =
          (namespace, name) ->
              counters.computeIfAbsent(name, (k) -> new MyCounter(namespace, name));
      distributionFactory =
          (namespace, name) ->
              distributions.computeIfAbsent(name, (k) -> new MyDistribution(namespace, name));
    }

    public Map<String, MyCounter> getCounters() {
      return ImmutableMap.copyOf(counters);
    }

    public Map<String, MyDistribution> getDistributions() {
      return ImmutableMap.copyOf(distributions);
    }
  }

  private static class MyCounter implements Counter {
    private final MetricName named;

    private final List<Long> incInvocations;

    public MyCounter(String namespace, String name) {
      named = MetricName.named(namespace, name);
      incInvocations = new ArrayList<>();
    }

    @Override
    public void inc() {
      LOG.trace("{} {}:inc()", METRIC_MARKER, named);
    }

    @Override
    public void inc(long n) {
      LOG.trace("{} {}:inc(n = {})", METRIC_MARKER, named, n);
      incInvocations.add(n);
    }

    @Override
    public void dec() {
      dec(1);
    }

    @Override
    public void dec(long n) {
      throw new IllegalStateException("not implemented");
    }

    @Override
    public MetricName getName() {
      return named;
    }
  }

  private static class MyDistribution implements Distribution {
    private final MetricName name;

    private final List<Long> updateInvocations;

    public MyDistribution(String namespace, String name) {
      this.name = MetricName.named(namespace, name);
      this.updateInvocations = new ArrayList<>();
    }

    @Override
    public void update(long value) {
      LOG.trace("{} {}:update(value = {})", METRIC_MARKER, name, value);
      updateInvocations.add(value);
    }

    @Override
    public void update(long sum, long count, long min, long max) {
      throw new IllegalStateException("not implemented");
    }

    @Override
    public MetricName getName() {
      return name;
    }
  }
}
