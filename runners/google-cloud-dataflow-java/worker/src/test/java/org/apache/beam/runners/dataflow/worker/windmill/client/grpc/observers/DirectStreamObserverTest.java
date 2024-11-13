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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.base.VerifyException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DirectStreamObserverTest {

  @Test
  public void testTerminate_waitingForReady() throws ExecutionException, InterruptedException {
    CountDownLatch sendBlocker = new CountDownLatch(1);
    TestStreamObserver delegate = spy(new TestStreamObserver(sendBlocker, 2));
    delegate.setIsReady(false);
    DirectStreamObserver<Integer> streamObserver =
        new DirectStreamObserver<>(new AdvancingPhaser(1), delegate, Long.MAX_VALUE, 1);
    ExecutorService onNextExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch blockLatch = new CountDownLatch(1);
    Future<Throwable> onNextFuture =
        onNextExecutor.submit(
            () -> {
              // Won't block on the first one.
              streamObserver.onNext(1);
              blockLatch.countDown();
              try {
                // We will check isReady on the next message, will block here.
                streamObserver.onNext(1);
              } catch (Throwable e) {
                return e;
              }

              return new VerifyException();
            });
    RuntimeException terminationException = new RuntimeException("terminated");

    assertTrue(blockLatch.await(5, TimeUnit.SECONDS));
    streamObserver.terminate(terminationException);
    assertThat(onNextFuture.get()).isInstanceOf(StreamObserverCancelledException.class);
    verify(delegate).onError(same(terminationException));
    // onNext should only have been called once.
    verify(delegate, times(1)).onNext(any());
  }

  @Test
  public void testOnNext_interruption() throws ExecutionException, InterruptedException {
    CountDownLatch sendBlocker = new CountDownLatch(1);
    TestStreamObserver delegate = spy(new TestStreamObserver(sendBlocker, 2));
    delegate.setIsReady(false);
    DirectStreamObserver<Integer> streamObserver =
        new DirectStreamObserver<>(new AdvancingPhaser(1), delegate, Long.MAX_VALUE, 1);
    ExecutorService onNextExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch streamObserverExitLatch = new CountDownLatch(1);
    Future<StreamObserverCancelledException> onNextFuture =
        onNextExecutor.submit(
            () -> {
              // Won't block on the first one.
              streamObserver.onNext(1);
              // We will check isReady on the next message, will block here.
              StreamObserverCancelledException e =
                  assertThrows(
                      StreamObserverCancelledException.class, () -> streamObserver.onNext(1));
              streamObserverExitLatch.countDown();
              return e;
            });

    // Assert that onNextFuture is blocked.
    assertFalse(onNextFuture.isDone());
    assertThat(streamObserverExitLatch.getCount()).isEqualTo(1);

    onNextExecutor.shutdownNow();
    assertTrue(streamObserverExitLatch.await(5, TimeUnit.SECONDS));
    assertThat(onNextFuture.get()).hasCauseThat().isInstanceOf(InterruptedException.class);

    // onNext should only have been called once.
    verify(delegate, times(1)).onNext(any());
  }

  @Test
  public void testOnNext_timeOut() throws ExecutionException, InterruptedException {
    CountDownLatch sendBlocker = new CountDownLatch(1);
    TestStreamObserver delegate = spy(new TestStreamObserver(sendBlocker, 2));
    delegate.setIsReady(false);
    DirectStreamObserver<Integer> streamObserver =
        new DirectStreamObserver<>(new AdvancingPhaser(1), delegate, 1, 1);
    ExecutorService onNextExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch streamObserverExitLatch = new CountDownLatch(1);
    Future<WindmillServerStub.WindmillRpcException> onNextFuture =
        onNextExecutor.submit(
            () -> {
              // Won't block on the first one.
              streamObserver.onNext(1);
              // We will check isReady on the next message, will block here.
              WindmillServerStub.WindmillRpcException e =
                  assertThrows(
                      WindmillServerStub.WindmillRpcException.class,
                      () -> streamObserver.onNext(1));
              streamObserverExitLatch.countDown();
              return e;
            });

    // Assert that onNextFuture is blocked.
    assertFalse(onNextFuture.isDone());
    assertThat(streamObserverExitLatch.getCount()).isEqualTo(1);

    assertTrue(streamObserverExitLatch.await(10, TimeUnit.SECONDS));
    assertThat(onNextFuture.get()).hasCauseThat().isInstanceOf(TimeoutException.class);

    // onNext should only have been called once.
    verify(delegate, times(1)).onNext(any());
  }

  private static class TestStreamObserver extends CallStreamObserver<Integer> {
    private final CountDownLatch sendBlocker;
    private final int blockAfter;
    private final AtomicInteger seen = new AtomicInteger(0);
    private volatile boolean isReady = false;

    private TestStreamObserver(CountDownLatch sendBlocker, int blockAfter) {
      this.blockAfter = blockAfter;
      this.sendBlocker = sendBlocker;
    }

    @Override
    public void onNext(Integer integer) {
      try {
        if (seen.incrementAndGet() == blockAfter) {
          sendBlocker.await();
        }
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
      return isReady;
    }

    private void setIsReady(boolean isReadyOverride) {
      isReady = isReadyOverride;
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
