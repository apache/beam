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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedTimeoutException;
import org.slf4j.Logger;

/** Allows an interface for queueing streams to be closed asynchronously. */
@ThreadSafe
final class AsyncStreamCloser {
  private static final String STREAM_CLOSER_THREAD_NAME_FORMAT = "StreamCloserThread-%d";
  private final Logger logger;
  private final BlockingQueue<ResettableThrowingStreamObserver<?>> streamsToClose;
  private final ExecutorService streamCloserExecutor;

  @GuardedBy("this")
  private boolean isRunning;

  AsyncStreamCloser(Logger logger) {
    this.logger = logger;
    streamsToClose = new LinkedBlockingQueue<>();
    streamCloserExecutor =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat(STREAM_CLOSER_THREAD_NAME_FORMAT).build());
  }

  /** Returns true if the stream is successfully scheduled to be closed. */
  synchronized boolean tryScheduleForClosure(ResettableThrowingStreamObserver<?> stream) {
    if (isRunning) {
      streamsToClose.add(stream);
      return true;
    }

    return false;
  }

  synchronized void start() throws WindmillStreamShutdownException {
    if (!isRunning) {
      try {
        streamCloserExecutor.execute(
            () -> {
              while (true) {
                try {
                  timeoutStream(streamsToClose.take());
                } catch (InterruptedException e) {
                  // Drain streamsToClose to prevent any dangling StreamObservers.
                  streamsToClose.forEach(this::timeoutStream);
                  break;
                }
              }
            });
        isRunning = true;
      } catch (RejectedExecutionException e) {
        // Stream has already been shutdown.
        throw new WindmillStreamShutdownException("Stream has been previously shutdown.");
      }
    }
  }

  synchronized void shutdown() {
    streamCloserExecutor.shutdown();
    try {
      if (!streamCloserExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        streamCloserExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedTimeoutException(
          "Interrupted while waiting for streamCloserExecutor to terminate.", e);
    } finally {
      isRunning = false;
    }
  }

  private void timeoutStream(ResettableThrowingStreamObserver<?> streamObserver) {
    try {
      streamObserver.onCompleted();
    } catch (IllegalStateException onErrorException) {
      // The delegate above was already terminated via onError or onComplete.
      // Fallthrough since this is possibly due to queued onNext() calls that are being made from
      // previously blocked threads.
    } catch (RuntimeException onErrorException) {
      logger.warn("Encountered unexpected error when timing out StreamObserver.", onErrorException);
    } catch (ResettableThrowingStreamObserver.StreamClosedException
        | WindmillStreamShutdownException e) {
      logger.debug("Stream has already been closed or shutdown.");
    }
  }
}
