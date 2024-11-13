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
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
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
    CountDownLatch sendBlocker = new CountDownLatch(1);
    Function<StreamObserver<Integer>, StreamObserver<Integer>> clientFactory =
        ignored ->
            new CallStreamObserver<Integer>() {
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
                return false;
              }

              @Override
              public void setOnReadyHandler(Runnable runnable) {}

              @Override
              public void disableAutoInboundFlowControl() {}

              @Override
              public void request(int i) {}

              @Override
              public void setMessageCompression(boolean b) {}
            };

    TestStream testStream = newStream(clientFactory);
    testStream.start();
    ExecutorService sendExecutor = Executors.newSingleThreadExecutor();
    Future<WindmillStreamShutdownException> sendFuture =
        sendExecutor.submit(
            () ->
                assertThrows(WindmillStreamShutdownException.class, () -> testStream.testSend(1)));
    testStream.shutdown();

    // Sleep a bit to give sendExecutor time to execute the send().
    Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);

    sendBlocker.countDown();
    assertThat(sendFuture.get()).isInstanceOf(WindmillStreamShutdownException.class);
  }

  private static class TestStream extends AbstractWindmillStream<Integer, Integer> {
    private final AtomicInteger numStarts = new AtomicInteger();

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

    public void testSend(Integer i)
        throws ResettableThrowingStreamObserver.StreamClosedException,
            WindmillStreamShutdownException {
      trySend(i);
    }

    @Override
    protected void sendHealthCheck() {}

    @Override
    protected void appendSpecificHtml(PrintWriter writer) {}

    @Override
    protected void shutdownInternal() {}
  }
}
