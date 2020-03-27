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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.CallStreamObserver;
import org.apache.beam.vendor.grpc.v1p26p0.io.grpc.stub.StreamObserver;

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
  private final Phaser phaser;
  private final CallStreamObserver<T> outboundObserver;

  public DirectStreamObserver(Phaser phaser, CallStreamObserver<T> outboundObserver) {
    this.phaser = phaser;
    this.outboundObserver = outboundObserver;
  }

  @Override
  public void onNext(T value) {
    int phase = phaser.getPhase(); // A negative phase indicates it has been terminated.
    // The registered onReady may be blocked, so we periodically poll the observer directly.
    // Additionally to avoid becoming permanently stuck due to synchronization we fallback
    // to queuing in the outbound observer after 1 minute, see BEAM-9651 for more context.
    for (int waitLoops = 0;
        phase >= 0 && !outboundObserver.isReady() && waitLoops < 600;
        ++waitLoops) {
      try {
        phase = phaser.awaitAdvanceInterruptibly(phase, 100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (TimeoutException e) {
        // Polling isReady in case the callback is delayed
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
