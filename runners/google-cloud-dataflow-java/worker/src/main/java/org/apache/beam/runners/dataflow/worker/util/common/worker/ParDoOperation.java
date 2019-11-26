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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** A ParDo mapping function. */
public class ParDoOperation extends ReceivingOperation {
  private final ParDoFn fn;

  public ParDoOperation(ParDoFn fn, OutputReceiver[] outputReceivers, OperationContext context) {
    super(outputReceivers, context);
    this.fn = fn;
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

  @Override
  public void finish() throws Exception {
    try (Closeable scope = context.enterProcessTimers()) {
      checkStarted();
      fn.processTimers();
    }

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
}
