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
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;
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
  private static final int DEFAULT_MAX_MESSAGES_BEFORE_CHECK = 100;

  private final Phaser phaser;
  private final CallStreamObserver<T> outboundObserver;
  private final int maxMessagesBeforeCheck;

  private int numberOfMessagesBeforeReadyCheck;

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
    numberOfMessagesBeforeReadyCheck += 1;
    if (numberOfMessagesBeforeReadyCheck >= maxMessagesBeforeCheck) {
      numberOfMessagesBeforeReadyCheck = 0;
      int waitTime = 1;
      int totalTimeWaited = 0;
      int phase = phaser.getPhase();
      while (!outboundObserver.isReady()) {
        try {
          phaser.awaitAdvanceInterruptibly(phase, waitTime, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
          totalTimeWaited += waitTime;
          waitTime = waitTime * 2;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
      if (totalTimeWaited > 0) {
        // If the phase didn't change, this means that the installed onReady callback had not
        // been invoked.
        if (phase == phaser.getPhase()) {
          LOG.info(
              "Output channel stalled for {}s, outbound thread {}. See: "
                  + "https://issues.apache.org/jira/browse/BEAM-4280 for the history for "
                  + "this issue.",
              totalTimeWaited,
              Thread.currentThread().getName());
        } else {
          LOG.debug(
              "Output channel stalled for {}s, outbound thread {}.",
              totalTimeWaited,
              Thread.currentThread().getName());
        }
      }
    }
    synchronized (outboundObserver) {
      outboundObserver.onNext(value);
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
