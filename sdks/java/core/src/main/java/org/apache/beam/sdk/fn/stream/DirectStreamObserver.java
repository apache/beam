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

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamObserver} which uses synchronization on the underlying {@link CallStreamObserver}
 * to provide thread safety.
 *
 * <p>Flow control with the underlying {@link CallStreamObserver} is handled with a {@link Phaser}
 * which waits for advancement of the phase if the {@link CallStreamObserver} is not ready. Creator
 * is expected to advance the {@link Phaser} whenever the underlying {@link CallStreamObserver}
 * becomes ready. If the {@link Phaser} is terminated, {@link DirectStreamObserver<T>.onNext(T)}
 * will no longer wait for the {@link CallStreamObserver} to become ready.
 */
@ThreadSafe
public final class DirectStreamObserver<T> implements StreamObserver<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DirectStreamObserver.class);
  private static final int DEFAULT_MAX_MESSAGES_BEFORE_CHECK = 100;

  private final Phaser phaser;
  private final CallStreamObserver<T> outboundObserver;

  /**
   * Controls the number of messages that will be sent before isReady is invoked for the following
   * message. For example, maxMessagesBeforeCheck = 0, would mean to check isReady for each message
   * while maxMessagesBeforeCheck = 10, would mean that you are willing to send 10 messages and then
   * check isReady before the 11th message is sent.
   */
  private final int maxMessagesBeforeCheck;

  private final Object lock = new Object();
  private int numMessages = -1;

  public DirectStreamObserver(Phaser phaser, CallStreamObserver<T> outboundObserver) {
    this(phaser, outboundObserver, DEFAULT_MAX_MESSAGES_BEFORE_CHECK);
  }

  DirectStreamObserver(
      Phaser phaser, CallStreamObserver<T> outboundObserver, int maxMessagesBeforeCheck) {
    this.phaser = phaser;
    this.outboundObserver = outboundObserver;
    this.maxMessagesBeforeCheck = maxMessagesBeforeCheck;
  }

  @Override
  public void onNext(T value) {
    synchronized (lock) {
      if (++numMessages >= maxMessagesBeforeCheck) {
        numMessages = 0;
        int waitSeconds = 1;
        int totalSecondsWaited = 0;
        int phase = phaser.getPhase();
        // Record the initial phase in case we are in the inbound gRPC thread where the phase won't
        // advance.
        int initialPhase = phase;
        // A negative phase indicates that the phaser is terminated.
        while (phase >= 0 && !outboundObserver.isReady()) {
          try {
            phase = phaser.awaitAdvanceInterruptibly(phase, waitSeconds, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            totalSecondsWaited += waitSeconds;
            // Double the backoff for re-evaluating the isReady bit up to a maximum of once per
            // minute. This bounds the waiting if the onReady callback is not called as expected.
            waitSeconds = Math.min(waitSeconds * 2, 60);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
        if (totalSecondsWaited > 0) {
          // If the phase didn't change, this means that the installed onReady callback had not
          // been invoked.
          if (initialPhase == phase) {
            LOG.info(
                "Output channel stalled for {}s, outbound thread {}. OnReady notification was "
                    + "not invoked, ensure the inbound gRPC thread is not used for output.",
                totalSecondsWaited,
                Thread.currentThread().getName());
          } else if (totalSecondsWaited > 60) {
            LOG.warn(
                "Output channel stalled for {}s, outbound thread {}.",
                totalSecondsWaited,
                Thread.currentThread().getName());
          } else {
            LOG.debug(
                "Output channel stalled for {}s, outbound thread {}.",
                totalSecondsWaited,
                Thread.currentThread().getName());
          }
        }
      }
      outboundObserver.onNext(value);
    }
  }

  @Override
  public void onError(Throwable t) {
    synchronized (lock) {
      outboundObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    synchronized (lock) {
      outboundObserver.onCompleted();
    }
  }
}
