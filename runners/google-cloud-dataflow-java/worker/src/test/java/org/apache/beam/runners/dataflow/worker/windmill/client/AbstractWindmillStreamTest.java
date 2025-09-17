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
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
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
    return new TestStream(
        clientFactory,
        streamRegistry,
        streamObserverFactory,
        Duration.ZERO,
        Executors.newScheduledThreadPool(0));
  }

  @Test
  public void testShutdown_notBlockedBySend() throws InterruptedException, ExecutionException {
    TestCallStreamObserver callStreamObserver = TestCallStreamObserver.notReady();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    Future<WindmillStreamShutdownException> sendFuture =
        sendExecutor.submit(
            () -> {
              // Send a few times to trigger blocking in the CallStreamObserver.
              testStream.testSend();
              testStream.testSend();
              return assertThrows(WindmillStreamShutdownException.class, testStream::testSend);
            });

    // Wait for 1 send since it always goes through, the rest may buffer.
    callStreamObserver.waitForSends(1);

    testStream.shutdown();

    assertThat(sendFuture.get()).isInstanceOf(WindmillStreamShutdownException.class);
  }

  @Test
  public void testMaybeScheduleHealthCheck() {
    TestCallStreamObserver callStreamObserver = TestCallStreamObserver.create();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    Instant reportingThreshold = Instant.now().minus(Duration.millis(1));

    testStream.maybeScheduleHealthCheck(reportingThreshold);
    testStream.waitForHealthChecks(1);
    assertThat(testStream.numHealthChecks.get()).isEqualTo(1);
    testStream.shutdown();
  }

  @Test
  public void testMaybeSendHealthCheck_doesNotSendIfLastScheduleLessThanThreshold() {
    TestCallStreamObserver callStreamObserver = TestCallStreamObserver.create();
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored -> callStreamObserver;

    TestStream testStream = newStream(clientFactory);
    testStream.start();

    try {
      testStream.trySend(1);
    } catch (WindmillStreamShutdownException e) {
      throw new RuntimeException(e);
    }

    // Set a really long reporting threshold.
    Instant reportingThreshold = Instant.now().minus(Duration.standardHours(1));

    // Should not send health checks since we just sent the above message.
    testStream.maybeScheduleHealthCheck(reportingThreshold);
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    testStream.maybeScheduleHealthCheck(reportingThreshold);
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    callStreamObserver.waitForSends(1);
    // Sleep just to ensure an async health check doesn't show up
    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    assertThat(testStream.numHealthChecks.get()).isEqualTo(0);
    testStream.shutdown();
  }

  private static class TestStream extends AbstractWindmillStream<Integer, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractWindmillStreamTest.class);

    private final AtomicInteger numStarts = new AtomicInteger();
    private final AtomicInteger numFlushPending = new AtomicInteger();
    private final AtomicInteger numHealthChecks = new AtomicInteger();

    private TestStream(
        Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory,
        Set<AbstractWindmillStream<?, ?>> streamRegistry,
        StreamObserverFactory streamObserverFactory,
        Duration halfCloseAfterTimeout,
        ScheduledExecutorService executorService) {
      super(
          LoggerFactory.getLogger(AbstractWindmillStreamTest.class),
          clientFactory,
          FluentBackoff.DEFAULT.backoff(),
          streamObserverFactory,
          streamRegistry,
          1,
          "Test",
          java.time.Duration.of(halfCloseAfterTimeout.getMillis(), ChronoUnit.MILLIS),
          executorService);
    }

    @Override
    protected PhysicalStreamHandler newResponseHandler() {
      return new PhysicalStreamHandler() {

        @Override
        public void onResponse(Integer response) {}

        @Override
        public boolean hasPendingRequests() {
          return false;
        }

        @Override
        public void onDone(Status status) {}

        @Override
        public void appendHtml(PrintWriter writer) {}
      };
    }

    @Override
    protected void onFlushPending(boolean isNewStream) {
      if (isNewStream) {
        numStarts.incrementAndGet();
      }
      numFlushPending.incrementAndGet();
    }

    private void testSend() throws WindmillStreamShutdownException {
      trySend(1);
    }

    @Override
    protected void sendHealthCheck() {
      numHealthChecks.incrementAndGet();
    }

    private void waitForHealthChecks(int expectedHealthChecks) {
      int waitedMillis = 0;
      while (numHealthChecks.get() < expectedHealthChecks) {
        LOG.info(
            "Waited for {}ms for {} health checks. Current health check count is {}.",
            waitedMillis,
            numHealthChecks.get(),
            expectedHealthChecks);
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }

    @Override
    protected void appendSpecificHtml(PrintWriter writer) {}
  }

  private static class TestCallStreamObserver extends CallStreamObserver<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractWindmillStreamTest.class);
    private final AtomicInteger numSends = new AtomicInteger();
    private final boolean isReady;

    private TestCallStreamObserver(boolean isReady) {
      this.isReady = isReady;
    }

    private static TestCallStreamObserver create() {
      return new TestCallStreamObserver(true);
    }

    private static TestCallStreamObserver notReady() {
      return new TestCallStreamObserver(false);
    }

    @Override
    public void onNext(Integer integer) {
      numSends.incrementAndGet();
    }

    private void waitForSends(int expectedSends) {
      int millisWaited = 0;
      while (numSends.get() < expectedSends) {
        LOG.info(
            "Waited {}ms for {} sends, current sends: {}", millisWaited, expectedSends, numSends);
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        millisWaited += 100;
      }
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void onCompleted() {}

    @Override
    public boolean isReady() {
      return isReady;
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
