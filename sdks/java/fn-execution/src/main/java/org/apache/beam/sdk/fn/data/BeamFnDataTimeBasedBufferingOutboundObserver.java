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
package org.apache.beam.sdk.fn.data;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.grpc.v1p21p0.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A buffering outbound {@link FnDataReceiver} with both size-based buffer and time-based buffer
 * enabled for the Beam Fn Data API.
 */
public class BeamFnDataTimeBasedBufferingOutboundObserver<T>
    extends BeamFnDataSizeBasedBufferingOutboundObserver<T> {

  private final Object lock;
  private final ScheduledFuture<?> flushFuture;
  @VisibleForTesting final AtomicReference<IOException> flushException;

  BeamFnDataTimeBasedBufferingOutboundObserver(
      int sizeLimit,
      long timeLimit,
      LogicalEndpoint outputLocation,
      Coder<T> coder,
      StreamObserver<BeamFnApi.Elements> outboundObserver) {
    super(sizeLimit, outputLocation, coder, outboundObserver);
    this.lock = new Object();
    this.flushFuture =
        Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("DataBufferOutboundFlusher-thread")
                    .build())
            .scheduleAtFixedRate(this::periodicFlush, timeLimit, timeLimit, TimeUnit.MILLISECONDS);
    this.flushException = new AtomicReference<>(null);
  }

  @Override
  public void close() throws Exception {
    checkFlushThreadException();
    synchronized (lock) {
      flushFuture.cancel(true);
      try {
        flushFuture.get();
      } catch (CancellationException | ExecutionException | InterruptedException exn) {
        // expected
      }
    }
    super.close();
  }

  @Override
  public void flush() throws IOException {
    synchronized (lock) {
      super.flush();
    }
  }

  @Override
  public void accept(T t) throws IOException {
    checkFlushThreadException();
    super.accept(t);
  }

  private void periodicFlush() {
    try {
      flush();
    } catch (Throwable t) {
      if (t instanceof IOException) {
        flushException.set((IOException) t);
      } else {
        flushException.set(new IOException(t));
      }
      throw new RuntimeException(t);
    }
  }

  /** Check if the flush thread failed with an exception. */
  private void checkFlushThreadException() throws IOException {
    IOException e = flushException.get();
    if (e != null) {
      throw e;
    }
  }
}
