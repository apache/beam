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
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A ParDo mapping function. */
public class ParDoOperation extends ReceivingOperation {
  private final ParDoFn fn;
  // If non-empty, this ParDo should report the amount of bytes read using these per-output-receiver
  // coders to serialize elements.
  private final Map<OutputReceiver, Coder<?>> bytesReadCoders;

  public ParDoOperation(ParDoFn fn, OutputReceiver[] outputReceivers, OperationContext context) {
    this(fn, outputReceivers, context, ImmutableMap.of());
  }

  public ParDoOperation(
      ParDoFn fn,
      OutputReceiver[] outputReceivers,
      OperationContext context,
      @Nullable Map<OutputReceiver, Coder<?>> bytesReadCoders) {
    super(outputReceivers, context);
    this.fn = fn;
    this.bytesReadCoders =
        bytesReadCoders != null ? ImmutableMap.copyOf(bytesReadCoders) : ImmutableMap.of();
  }

  @Override
  public void start() throws Exception {
    try (Closeable scope = context.enterStart()) {
      super.start();
      fn.startBundle(receivers);
    }
  }

  @Override
  public void process(Object elem) throws Exception {
    try (Closeable scope = context.enterProcess()) {
      checkStarted();
      fn.processElement(elem);
    }
  }

  // Batch mode does not use this method and instead relies on BatchModeUngroupingParDoFn
  // to process timers per key.
  @Override
  public void finishKey(@Nullable Object key) throws Exception {
    try (Closeable scope = context.enterProcessTimers()) {
      checkStarted();
      fn.processTimers();
      fn.finishKey(key);
    }
  }

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterFinish()) {
      fn.finishBundle();
      super.finish();
    }
  }

  @Override
  public void abort() throws Exception {
    try (Closeable scope = context.enterAbort()) {
      fn.abort();
      super.abort();
    }
  }

  @Override
  public boolean supportsRestart() {
    return true;
  }

  @VisibleForTesting
  public ParDoFn getFn() throws Exception {
    return fn;
  }

  public boolean shouldReportBytesRead() {
    return !bytesReadCoders.isEmpty();
  }

  public Map<OutputReceiver, Coder<?>> getBytesReadCoders() {
    return bytesReadCoders;
  }

  public @Nullable Coder<?> getBytesReadCoder(OutputReceiver receiver) {
    return bytesReadCoders.get(receiver);
  }
}
