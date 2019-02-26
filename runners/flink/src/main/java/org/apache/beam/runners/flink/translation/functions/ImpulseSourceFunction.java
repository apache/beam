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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Source function which sends a single global impulse to a downstream operator. It may keep the
 * source alive although its work is already done. It will only shutdown when the streaming job is
 * cancelled.
 */
public class ImpulseSourceFunction implements SourceFunction<WindowedValue<byte[]>> {

  /** Keep source running even after it has done all the work. */
  private final boolean keepSourceAlive;

  /** Indicates the streaming job is running and the source can produce elements. */
  private volatile boolean running;

  public ImpulseSourceFunction(boolean keepSourceAlive) {
    this.keepSourceAlive = keepSourceAlive;
    this.running = true;
  }

  @Override
  public void run(SourceContext<WindowedValue<byte[]>> sourceContext) throws Exception {
    // emit single impulse element
    sourceContext.collect(WindowedValue.valueInGlobalWindow(new byte[0]));
    // Do nothing, but still look busy ...
    // we can't return here since Flink requires that all operators stay up,
    // otherwise checkpointing would not work correctly anymore
    //
    // See https://issues.apache.org/jira/browse/FLINK-2491 for progress on this issue
    if (keepSourceAlive) {
      // wait until this is canceled
      final Object waitLock = new Object();
      while (running) {
        try {
          // Flink will interrupt us at some point
          //noinspection SynchronizationOnLocalVariableOrMethodParameter
          synchronized (waitLock) {
            // don't wait indefinitely, in case something goes horribly wrong
            waitLock.wait(1000);
          }
        } catch (InterruptedException e) {
          if (!running) {
            // restore the interrupted state, and fall through the loop
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }
}
