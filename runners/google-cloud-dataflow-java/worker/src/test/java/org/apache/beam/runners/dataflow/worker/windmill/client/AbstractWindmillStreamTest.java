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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class AbstractWindmillStreamTest {
  private static final long DEADLINE_SECONDS = 10;
  private final Set<AbstractWindmillStream<?, ?>> streamRegistry = ConcurrentHashMap.newKeySet();
  private final StreamObserverFactory streamObserverFactory =
      StreamObserverFactory.direct(DEADLINE_SECONDS, 1);

  @Before
  public void setUp() {
    streamRegistry.clear();
  }

  private TestStream newStream(
      Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory) {
    return new TestStream(clientFactory, streamRegistry, streamObserverFactory);
  }

  @Test
  public void testShutdown_notBlockedBySend() throws InterruptedException, ExecutionException {
    TestCallStreamObserver callStreamObserver = new TestCallStreamObserver();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    Future<WindmillStreamShutdownException> sendFuture =
        sendExecutor.submit(
            () ->
                assertThrows(WindmillStreamShutdownException.class, () -> testStream.testSend(1)));
    testStream.shutdown();

    // Sleep a bit to give sendExecutor time to execute the send().
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    callStreamObserver.unblockSend();
    assertThat(sendFuture.get()).isInstanceOf(WindmillStreamShutdownException.class);
  }

  @Test
  public void testMaybeSendHealthCheck() throws InterruptedException, ExecutionException {
    TestCallStreamObserver callStreamObserver = new TestCallStreamObserver();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    Instant reportingThreshold = Instant.now().minus(Duration.millis(1));
    Future<?> sendFuture =
        sendExecutor.submit(() -> testStream.maybeSendHealthCheck(reportingThreshold));

    // Sleep a bit to give sendExecutor time to execute the send().
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    callStreamObserver.unblockSend();

    // Make sure the future completes.
    sendFuture.get();

    assertThat(testStream.numHealthChecks.get()).isEqualTo(1);
    testStream.shutdown();
  }

  @Test
  public void testMaybeSendHealthCheck_doesNotSendIfLastSendLessThanThreshold()
      throws ExecutionException, InterruptedException {
    TestCallStreamObserver callStreamObserver = new TestCallStreamObserver();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    Future<?> sendFuture =
        sendExecutor.submit(
            () -> {
              try {
                testStream.trySend(1);
              } catch (WindmillStreamShutdownException e) {
                throw new RuntimeException(e);
              }

              // Sleep a bit to give sendExecutor time to execute the send().
              Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

              // Set a really long reporting threshold.
              Instant reportingThreshold = Instant.now().minus(Duration.standardHours(1));
              // Should not send health checks since we just sent the above message.
              testStream.maybeSendHealthCheck(reportingThreshold);
              testStream.maybeSendHealthCheck(reportingThreshold);
            });

    callStreamObserver.unblockSend();

    // Make sure the future completes.
    sendFuture.get();
    assertThat(testStream.numHealthChecks.get()).isEqualTo(0);
    testStream.shutdown();
  }

  private static class TestStream extends AbstractWindmillStream<Integer, Integer> {
    private final AtomicInteger numStarts = new AtomicInteger();
    private final AtomicInteger numHealthChecks = new AtomicInteger();

    private TestStream(
        Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory,
        Set<AbstractWindmillStream<?, ?>> streamRegistry,
        StreamObserverFactory streamObserverFactory) {
      super(
          LoggerFactory.getLogger(AbstractWindmillStreamTest.class),
          "Test",
          clientFactory,
          FluentBackoff.DEFAULT.backoff(),
          streamObserverFactory,
          streamRegistry,
          1,
          "Test");
    }

    @Override
    protected void onResponse(Integer response) {}

    @Override
    protected void onNewStream() {
      numStarts.incrementAndGet();
    }

    @Override
    protected boolean hasPendingRequests() {
      return false;
    }

    @Override
    protected void startThrottleTimer() {}

    public void testSend(Integer i) throws WindmillStreamShutdownException {
      trySend(i);
    }

    @Override
    protected void sendHealthCheck() {
      numHealthChecks.incrementAndGet();
    }

    @Override
    protected void appendSpecificHtml(PrintWriter writer) {}

    @Override
    protected void shutdownInternal() {}
  }

  private static class TestCallStreamObserver extends CallStreamObserver<Integer> {
    private final CountDownLatch sendBlocker = new CountDownLatch(1);

    private void unblockSend() {
      sendBlocker.countDown();
    }

    @Override
    public void onNext(Integer integer) {
      try {
        sendBlocker.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {}

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable runnable) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int i) {}

    @Override
    public void setMessageCompression(boolean b) {}
  }
}
