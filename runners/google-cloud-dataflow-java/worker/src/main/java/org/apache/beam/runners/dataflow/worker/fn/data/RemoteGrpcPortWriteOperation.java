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

import com.google.common.base.MoreObjects;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Target;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReceivingOperation;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * An {@link Operation} that uses the Beam Fn Data API to send messages.
 *
 * <p>This {@link Operation} supports restart.
 */
public class RemoteGrpcPortWriteOperation<T> extends ReceivingOperation {
  private static final OutputReceiver[] EMPTY_RECEIVER_ARRAY = new OutputReceiver[0];
  private final Coder<WindowedValue<T>> coder;
  private final FnDataService beamFnDataService;
  private final Supplier<String> bundleIdSupplier;
  // Should only be set and cleared once per start/finish cycle in the start method and
  // finish method respectively.
  private String bundleId;
  private final Target target;
  private CloseableFnDataReceiver<WindowedValue<T>> receiver;
  private final AtomicInteger elementsSent = new AtomicInteger();

  public RemoteGrpcPortWriteOperation(
      FnDataService beamFnDataService,
      Target target,
      Supplier<String> bundleIdSupplier,
      Coder<WindowedValue<T>> coder,
      OperationContext context) {
    super(EMPTY_RECEIVER_ARRAY, context);
    this.coder = coder;
    this.beamFnDataService = beamFnDataService;
    this.bundleIdSupplier = bundleIdSupplier;
    this.target = target;
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      elementsSent.set(0);
      super.start();
      bundleId = bundleIdSupplier.get();
      receiver = beamFnDataService.send(LogicalEndpoint.of(bundleId, target), coder);
    }
  }

  @Override
  public void process(Object outputElem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
      elementsSent.incrementAndGet();
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
        .add("target", target)
        .add("coder", coder)
        .add("bundleId", bundleId)
        .toString();
  }
}
