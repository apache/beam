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

package org.apache.beam.fn.harness.stream;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

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
public final class BufferingStreamObserver<T> implements StreamObserver<T> {
  private static final Object POISON_PILL = new Object();
  private final LinkedBlockingDeque<T> queue;
  private final Phaser phaser;
  private final CallStreamObserver<T> outboundObserver;
  private final Future<?> queueDrainer;
  private final int bufferSize;

  public BufferingStreamObserver(
      Phaser phaser,
      CallStreamObserver<T> outboundObserver,
      ExecutorService executor,
      int bufferSize) {
    this.phaser = phaser;
    this.bufferSize = bufferSize;
    this.queue = new LinkedBlockingDeque<>(bufferSize);
    this.outboundObserver = outboundObserver;
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
            return;
          }
        }
        phaser.awaitAdvance(currentPhase);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void onNext(T value) {
    try {
      // Attempt to add an element to the bounded queue occasionally checking to see
      // if the queue drainer is still alive.
      while (!queue.offer(value, 60, TimeUnit.SECONDS)) {
        checkState(!queueDrainer.isDone(), "Stream observer has finished.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onError(Throwable t) {
    synchronized (outboundObserver) {
      // If we are done, then a previous caller has already shutdown the queue processing thread
      // hence we don't need to do it again.
      if (!queueDrainer.isDone()) {
        // We check to see if we were able to successfully insert the poison pill at the front of
        // the queue to cancel the processing thread eagerly or if the processing thread is done.
        try {
          // We shouldn't attempt to insert into the queue if the queue drainer thread is done
          // since the queue may be full and nothing will be emptying it.
          while (!queueDrainer.isDone()
              && !queue.offerFirst((T) POISON_PILL, 60, TimeUnit.SECONDS)) {
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        waitTillFinish();
      }
      outboundObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    synchronized (outboundObserver) {
      // If we are done, then a previous caller has already shutdown the queue processing thread
      // hence we don't need to do it again.
      if (!queueDrainer.isDone()) {
        // We check to see if we were able to successfully insert the poison pill at the end of
        // the queue forcing the remainder of the elements to be processed or if the processing
        // thread is done.
        try {
          // We shouldn't attempt to insert into the queue if the queue drainer thread is done
          // since the queue may be full and nothing will be emptying it.
          while (!queueDrainer.isDone()
              && !queue.offerLast((T) POISON_PILL, 60, TimeUnit.SECONDS)) {
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        waitTillFinish();
      }
      outboundObserver.onCompleted();
    }
  }

  @VisibleForTesting
  public int getBufferSize() {
    return bufferSize;
  }

  private void waitTillFinish() {
    try {
      queueDrainer.get();
    } catch (CancellationException e) {
      // Cancellation is expected
      return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
