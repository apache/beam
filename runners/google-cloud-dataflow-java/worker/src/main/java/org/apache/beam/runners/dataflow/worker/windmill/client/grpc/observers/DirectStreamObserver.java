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

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamObserver} which synchronizes access to the underlying {@link CallStreamObserver}
 * to provide thread safety.
 *
 * <p>Flow control with the underlying {@link CallStreamObserver} is handled with a {@link Phaser}
 * which waits for advancement of the phase if the {@link CallStreamObserver} is not ready. Creator
 * is expected to advance the {@link Phaser} whenever the underlying {@link CallStreamObserver}
 * becomes ready.
 */
@ThreadSafe
public final class DirectStreamObserver<T> implements StreamObserver<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DirectStreamObserver.class);
  private static final long OUTPUT_CHANNEL_CONSIDERED_STALLED_SECONDS = 30;

  private final Phaser isReadyNotifier;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final CallStreamObserver<T> outboundObserver;

  private final long deadlineSeconds;
  private final int messagesBetweenIsReadyChecks;

  @GuardedBy("lock")
  private int messagesSinceReady = 0;

  public DirectStreamObserver(
      Phaser isReadyNotifier,
      CallStreamObserver<T> outboundObserver,
      long deadlineSeconds,
      int messagesBetweenIsReadyChecks) {
    this.isReadyNotifier = isReadyNotifier;
    this.outboundObserver = outboundObserver;
    this.deadlineSeconds = deadlineSeconds;
    // We always let the first message pass through without blocking because it is performed under
    // the StreamPool synchronized block and single header message isn't going to cause memory
    // issues due to excessive buffering within grpc.
    this.messagesBetweenIsReadyChecks = Math.max(1, messagesBetweenIsReadyChecks);
  }

  @Override
  public void onNext(T value) {
    int awaitPhase = -1;
    long totalSecondsWaited = 0;
    long waitSeconds = 1;
    while (true) {
      try {
        synchronized (lock) {
          int currentPhase = isReadyNotifier.getPhase();
          // Phaser is terminated so don't use the outboundObserver. Since onError and onCompleted
          // are synchronized after terminating the phaser if we observe that the phaser is not
          // terminated the onNext calls below are guaranteed to not be called on a closed observer.
          if (currentPhase < 0) return;

          // If we awaited previously and timed out, wait for the same phase. Otherwise we're
          // careful to observe the phase before observing isReady.
          if (awaitPhase < 0) {
            awaitPhase = isReadyNotifier.getPhase();
            // If getPhase() returns a value less than 0, the phaser has been terminated.
            if (awaitPhase < 0) {
              return;
            }
          }

          // We only check isReady periodically to effectively allow for increasing the outbound
          // buffer periodically. This reduces the overhead of blocking while still restricting
          // memory because there is a limited # of streams, and we have a max messages size of 2MB.
          if (++messagesSinceReady <= messagesBetweenIsReadyChecks) {
            outboundObserver.onNext(value);
            return;
          }

          if (outboundObserver.isReady()) {
            messagesSinceReady = 0;
            outboundObserver.onNext(value);
            return;
          }
        }

        // A callback has been registered to advance the phaser whenever the observer
        // transitions to  is ready. Since we are waiting for a phase observed before the
        // outboundObserver.isReady() returned false, we expect it to advance after the
        // channel has become ready.  This doesn't always seem to be the case (despite
        // documentation stating otherwise) so we poll periodically and enforce an overall
        // timeout related to the stream deadline.
        int nextPhase =
            isReadyNotifier.awaitAdvanceInterruptibly(awaitPhase, waitSeconds, TimeUnit.SECONDS);
        // If nextPhase is a value less than 0, the phaser has been terminated.
        if (nextPhase < 0) {
          return;
        }

        synchronized (lock) {
          int currentPhase = isReadyNotifier.getPhase();
          // Phaser is terminated so don't use the outboundObserver. Since onError and onCompleted
          // are synchronized after terminating the phaser if we observe that the phaser is not
          // terminated the onNext calls below are guaranteed to not be called on a closed observer.
          if (currentPhase < 0) return;
          messagesSinceReady = 0;
          outboundObserver.onNext(value);
          return;
        }
      } catch (TimeoutException e) {
        totalSecondsWaited += waitSeconds;
        if (totalSecondsWaited > deadlineSeconds) {
          String errorMessage = constructStreamCancelledErrorMessage(totalSecondsWaited);
          LOG.error(errorMessage);
          throw new StreamObserverCancelledException(errorMessage, e);
        }

        if (totalSecondsWaited > OUTPUT_CHANNEL_CONSIDERED_STALLED_SECONDS) {
          LOG.info(
              "Output channel stalled for {}s, outbound thread {}.",
              totalSecondsWaited,
              Thread.currentThread().getName());
        }

        waitSeconds = waitSeconds * 2;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StreamObserverCancelledException(e);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    // Free the blocked threads in onNext().
    isReadyNotifier.forceTermination();
    synchronized (lock) {
      outboundObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    // Free the blocked threads in onNext().
    isReadyNotifier.forceTermination();
    synchronized (lock) {
      outboundObserver.onCompleted();
    }
  }

  private String constructStreamCancelledErrorMessage(long totalSecondsWaited) {
    return deadlineSeconds > 0
        ? "Waited "
            + totalSecondsWaited
            + "s which exceeds given deadline of "
            + deadlineSeconds
            + "s for the outboundObserver to become ready meaning "
            + "that the stream deadline was not respected."
        : "Output channel has been blocked for "
            + totalSecondsWaited
            + "s. Restarting stream internally.";
  }
}
