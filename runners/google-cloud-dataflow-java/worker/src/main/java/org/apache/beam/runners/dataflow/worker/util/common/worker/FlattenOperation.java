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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.Closeable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A flatten operation. */
public class FlattenOperation extends ReceivingOperation {
  public FlattenOperation(OutputReceiver[] receivers, OperationContext context) {
    super(receivers, context);
  }

  /** Invoked by tests. */
  @VisibleForTesting
  public FlattenOperation(OutputReceiver outputReceiver, OperationContext context) {
    this(new OutputReceiver[] {outputReceiver}, context);
  }

  @Override
  public void process(Object elem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
      checkStarted();
      Receiver receiver = receivers[0];
      if (receiver != null) {
        receiver.process(elem);
      }
    }
  }

  /**
   * Returns the downstream receiver when this {@link FlattenOperation} is started and acts as a
   * pure single-output pass-through, allowing upstream {@link OutputReceiver}s to bypass {@code
   * FlattenOperation.process} stack frames while keeping {@code Flatten}'s {@link OutputReceiver}
   * counters intact.
   */
  @Nullable Receiver passThroughReceiverIfStarted() {
    if (initializationState == InitializationState.STARTED && receivers.length == 1) {
      return receivers[0];
    }
    return null;
  }

  @Override
  public void finishKey(@Nullable Object key) throws Exception {}

  @Override
  public boolean supportsRestart() {
    return true;
  }
}
