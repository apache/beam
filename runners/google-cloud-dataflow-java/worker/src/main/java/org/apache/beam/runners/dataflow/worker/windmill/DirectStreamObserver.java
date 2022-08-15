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
package org.apache.beam.runners.dataflow.worker.windmill;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamObserver} which uses synchronization on the underlying {@link CallStreamObserver}
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
  private final Phaser phaser;

  @GuardedBy("outboundObserver")
  private final CallStreamObserver<T> outboundObserver;

  private final long deadlineSeconds;

  @GuardedBy("outboundObserver")
  private boolean firstMessage = true;

  public DirectStreamObserver(
      Phaser phaser, CallStreamObserver<T> outboundObserver, long deadlineSeconds) {
    this.phaser = phaser;
    this.outboundObserver = outboundObserver;
    this.deadlineSeconds = deadlineSeconds;
  }

  @Override
  public void onNext(T value) {
    final int phase = phaser.getPhase();
    long totalSecondsWaited = 0;
    long waitSeconds = 1;
    while (true) {
      try {
        synchronized (outboundObserver) {
          // We let the first message passthrough without blocking because it is performed under the
          // StreamPool synchronized block and single message isn't going to cause memory issues due
          // to excessive buffering within grpc.
          if (firstMessage || outboundObserver.isReady()) {
            firstMessage = false;
            outboundObserver.onNext(value);
            return;
          }
        }
        // A callback has been registered to advance the phaser whenever the observer transitions to
        // is ready. Since we are waiting for a phase observed before the outboundObserver.isReady()
        // returned false, we expect it to advance after the channel has become ready.  This doesn't
        // always seem to be the case (despite documentation stating otherwise) so we poll
        // periodically and enforce an overall timeout related to the stream deadline.
        phaser.awaitAdvanceInterruptibly(phase, waitSeconds, TimeUnit.SECONDS);
        synchronized (outboundObserver) {
          outboundObserver.onNext(value);
          return;
        }
      } catch (TimeoutException e) {
        totalSecondsWaited += waitSeconds;
        if (totalSecondsWaited > deadlineSeconds) {
          LOG.error(
              "Exceeded timeout waiting for the outboundObserver to become ready meaning "
                  + "that the streamdeadline was not respected.");
          throw new RuntimeException(e);
        }
        waitSeconds = waitSeconds * 2;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (totalSecondsWaited > 30) {
        LOG.info(
            "Output channel stalled for {}s, outbound thread {}.",
            totalSecondsWaited,
            Thread.currentThread().getName());
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    synchronized (outboundObserver) {
      outboundObserver.onError(t);
    }
  }

  @Override
  public void onCompleted() {
    synchronized (outboundObserver) {
      outboundObserver.onCompleted();
    }
  }
}
