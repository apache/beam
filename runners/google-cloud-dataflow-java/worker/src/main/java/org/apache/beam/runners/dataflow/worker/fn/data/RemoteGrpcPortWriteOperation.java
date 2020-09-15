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
package org.apache.beam.runners.dataflow.worker.fn.data;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link Operation} that uses the Beam Fn Data API to send messages.
 *
 * <p>This {@link Operation} supports restart.
 */
public class RemoteGrpcPortWriteOperation<T> extends ReceivingOperation {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteGrpcPortWriteOperation.class);

  private static final OutputReceiver[] EMPTY_RECEIVER_ARRAY = new OutputReceiver[0];
  private final Coder<WindowedValue<T>> coder;
  private final FnDataService beamFnDataService;
  private final IdGenerator bundleIdSupplier;
  // Should only be set and cleared once per start/finish cycle in the start method and
  // finish method respectively.
  private String bundleId;
  private final String ptransformId;
  private CloseableFnDataReceiver<WindowedValue<T>> receiver;
  private final AtomicInteger elementsSent = new AtomicInteger();

  private boolean usingElementsProcessed = false;
  private AtomicInteger elementsProcessed = new AtomicInteger();
  private int elementsFlushed;
  private int targetElementsSent;

  private final Supplier<Long> currentTimeMillis;
  private long firstElementSentMillis;
  private long secondElementSentMillis;

  @VisibleForTesting static final long MAX_BUFFER_MILLIS = 5000;

  public RemoteGrpcPortWriteOperation(
      FnDataService beamFnDataService,
      String ptransformId,
      IdGenerator bundleIdSupplier,
      Coder<WindowedValue<T>> coder,
      OperationContext context) {
    this(
        beamFnDataService,
        ptransformId,
        bundleIdSupplier,
        coder,
        context,
        System::currentTimeMillis);
  }

  public RemoteGrpcPortWriteOperation(
      FnDataService beamFnDataService,
      String ptransformId,
      IdGenerator bundleIdSupplier,
      Coder<WindowedValue<T>> coder,
      OperationContext context,
      Supplier<Long> currentTimeMillis) {
    super(EMPTY_RECEIVER_ARRAY, context);
    this.coder = coder;
    this.beamFnDataService = beamFnDataService;
    this.bundleIdSupplier = bundleIdSupplier;
    this.ptransformId = ptransformId;
    this.currentTimeMillis = currentTimeMillis;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      elementsSent.set(0);
      elementsProcessed.set(0);
      targetElementsSent = 1;
      elementsFlushed = 0;
      super.start();
      bundleId = bundleIdSupplier.getId();
      receiver = beamFnDataService.send(LogicalEndpoint.data(bundleId, ptransformId), coder);
    }
  }

  /** Attempt to bound the amount of unconsumed data written to the buffer in absolute time. */
  @VisibleForTesting
  boolean shouldWait() throws Exception {
    if (!usingElementsProcessed) {
      return false;
    }
    int numSent = elementsSent.get();
    if (numSent >= targetElementsSent) {
      if (elementsFlushed < numSent) {
        receiver.flush();
        elementsFlushed = numSent;
      }
      int numProcessed = elementsProcessed.get();
      // A negative value indicates that obtaining numProcessed is not supported.
      // Otherwise, wait until the SDK has processed at least one element before continuing.
      if (numProcessed < 0) {
        targetElementsSent = Integer.MAX_VALUE;
      } else if (numProcessed == 0) {
        targetElementsSent = 1;
      } else {
        double rate;
        if (numProcessed == 1) {
          rate = (double) numProcessed / (currentTimeMillis.get() - firstElementSentMillis);
        } else {
          rate = ((double) numProcessed - 1) / (currentTimeMillis.get() - secondElementSentMillis);
        }
        // Note that numProcessed is always increasing up to numSent, and rate is always positive,
        // so eventually we'll return True.
        targetElementsSent =
            (int) Math.min(numProcessed + rate * MAX_BUFFER_MILLIS + 1, Integer.MAX_VALUE);
      }
    }
    return numSent >= targetElementsSent;
  }

  public Consumer<Integer> processedElementsConsumer() {
    usingElementsProcessed = true;
    return elementsProcessed -> {
      try {
        lock.lock();
        this.elementsProcessed.set(elementsProcessed);
        condition.signal();
      } finally {
        lock.unlock();
      }
    };
  }

  Lock lock = new ReentrantLock();
  Condition condition = lock.newCondition();

  private void maybeWait() throws Exception {
    if (shouldWait()) {
      try {
        lock.lock();
        while (shouldWait()) {
          LOG.debug(
              "Throttling elements at {} until more than {} elements been processed.",
              elementsSent.get(),
              elementsProcessed.get());
          condition.await();
        }
      } finally {
        lock.unlock();
      }
    }
  }

  public void abortWait() {
    usingElementsProcessed = false;
    try {
      lock.lock();
      condition.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void process(Object outputElem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
      maybeWait();
      int numSent = elementsSent.incrementAndGet();
      if (numSent == 1) {
        firstElementSentMillis = currentTimeMillis.get();
      } else if (numSent == 2) {
        secondElementSentMillis = currentTimeMillis.get();
      }
      receiver.accept((WindowedValue<T>) outputElem);
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      receiver.close();
      bundleId = null;
      super.finish();
    }
  }

  @Override
  public void abort() throws Exception {
    try (Closeable scope = context.enterAbort()) {
      abortWait();
      receiver.close();
      super.abort();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }

  /**
   * Returns the number of elements that have been written to the data stream.
   *
   * <p>Will throw an exception if this operation is not started (or after being finished/aborted).
   */
  public int getElementsSent() {
    synchronized (initializationStateLock) {
      if (initializationState != InitializationState.STARTED) {
        throw new IllegalStateException("Cannot be called on unstarted operation.");
      }
    }
    return elementsSent.get();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("ptransformId", ptransformId)
        .add("coder", coder)
        .add("bundleId", bundleId)
        .toString();
  }
}
