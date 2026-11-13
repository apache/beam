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
package org.apache.beam.sdk.fn.stream;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.fn.CancellableQueue;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A thread safe {@link StreamObserver} which uses a bounded queue to pass elements to a processing
 * thread responsible for interacting with the underlying {@link CallStreamObserver}.
 *
 * <p>Flow control with the underlying {@link CallStreamObserver} is handled with a {@link Phaser}
 * which waits for advancement of the phase if the {@link CallStreamObserver} is not ready. Callers
 * are expected to advance the {@link Phaser} whenever the underlying {@link CallStreamObserver}
 * becomes ready.
 */
@ThreadSafe
public final class BufferingStreamObserver<T extends @NonNull Object> implements StreamObserver<T> {
  /**
   * Internal exception used to signal that the queue drainer thread should invoke {@link
   * StreamObserver#onError}.
   */
  private static class OnErrorException extends Exception {
    public OnErrorException(@NonNull Throwable throwable) {
      super(throwable);
    }

    @Override
    @SuppressWarnings("return")
    public synchronized @NonNull Throwable getCause() {
      return super.getCause();
    }
  }

  private static final Object POISON_PILL = new Object();
  private static final int STALL_POLL_INTERVAL_MS = 50;
  private static final int STALL_NOTIFY_INTERVAL_MS = 500;
  private final CancellableQueue<T> queue;
  private final Phaser phaser;
  private final CallStreamObserver<T> outboundObserver;
  private final @Nullable StalledPollHandler stalledPollHandler;
  private final Future<?> queueDrainer;
  private final int bufferSize;

  public BufferingStreamObserver(
      Phaser phaser,
      CallStreamObserver<T> outboundObserver,
      ExecutorService executor,
      int bufferSize) {
    this(phaser, outboundObserver, executor, bufferSize, null);
  }

  /**
   * Like {@link #BufferingStreamObserver(Phaser, CallStreamObserver, ExecutorService, int)}. While
   * the drainer waits for the outbound stream to become ready, it calls {@code stalledPollHandler}
   * every 500 ms.
   */
  @SuppressWarnings("methodref.receiver.bound")
  public BufferingStreamObserver(
      Phaser phaser,
      CallStreamObserver<T> outboundObserver,
      ExecutorService executor,
      int bufferSize,
      @Nullable StalledPollHandler stalledPollHandler) {
    this.phaser = phaser;
    this.bufferSize = bufferSize;
    this.queue = new CancellableQueue<>(bufferSize);
    this.outboundObserver = outboundObserver;
    this.stalledPollHandler = stalledPollHandler;
    this.queueDrainer = executor.submit(this::drainQueue);
  }

  private void drainQueue() {
    try {
      while (true) {
        int currentPhase = phaser.getPhase();
        while (outboundObserver.isReady()) {
          T value = queue.take();
          if (value != POISON_PILL) {
            outboundObserver.onNext(value);
          } else {
            outboundObserver.onCompleted();
            return;
          }
        }
        awaitReady(currentPhase);
      }
    } catch (OnErrorException e) {
      outboundObserver.onError(e.getCause());
    } catch (Exception e) {
      queue.cancel(e);
      outboundObserver.onError(e);
    }
  }

  /** Waits for the phase to advance. With a stall handler, also polls readiness and notifies it. */
  private void awaitReady(int phase) throws InterruptedException {
    StalledPollHandler handler = stalledPollHandler;
    if (handler == null) {
      phaser.awaitAdvance(phase);
      return;
    }
    int totalMsWaited = 0;
    while (phase >= 0 && !outboundObserver.isReady()) {
      try {
        phaser.awaitAdvanceInterruptibly(phase, STALL_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        return;
      } catch (TimeoutException e) {
        totalMsWaited += STALL_POLL_INTERVAL_MS;
        if (totalMsWaited % STALL_NOTIFY_INTERVAL_MS == 0) {
          handler.onStalledPoll();
        }
      }
    }
  }

  @Override
  public void onNext(T value) {
    try {
      // Attempt to add an element to the bounded queue
      queue.put(value);
    } catch (InterruptedException e) {
      queue.cancel(e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (Exception e) {
      // All the other exception types already imply the queue has been cancelled.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onError(Throwable t) {
    queue.cancel(new OnErrorException(t));
    try {
      queueDrainer.get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onCompleted() {
    try {
      queue.put((T) POISON_PILL);
      queueDrainer.get();
    } catch (Exception e) {
      queue.cancel(e);
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  public int getBufferSize() {
    return bufferSize;
  }
}
