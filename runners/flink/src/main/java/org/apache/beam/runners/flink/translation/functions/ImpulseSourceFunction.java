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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Source function which sends a single global impulse to a downstream operator. It may keep the
 * source alive although its work is already done. It will only shutdown when the streaming job is
 * cancelled.
 */
public class ImpulseSourceFunction
    implements SourceFunction<WindowedValue<byte[]>>, CheckpointedFunction {

  /** The idle time before the source shuts down. */
  private final long idleTimeoutMs;

  /** Indicates the streaming job is running and the source can produce elements. */
  private volatile boolean running;

  /** Checkpointed state which indicates whether the impulse has finished. */
  private transient ListState<Boolean> impulseEmitted;

  public ImpulseSourceFunction(long idleTimeoutMs) {
    this.idleTimeoutMs = idleTimeoutMs;
    this.running = true;
  }

  @Override
  public void run(SourceContext<WindowedValue<byte[]>> sourceContext) throws Exception {
    if (Iterables.isEmpty(impulseEmitted.get())) {
      synchronized (sourceContext.getCheckpointLock()) {
        // emit single impulse element
        sourceContext.collect(WindowedValue.valueInGlobalWindow(new byte[0]));
        impulseEmitted.add(true);
      }
    }
    // Always emit a final watermark.
    // (1) In case we didn't restore the pipeline, this is important to close the global window;
    // if no operator holds back this watermark.
    // (2) In case we are restoring the pipeline, this is needed to initialize the operators with
    // the current watermark and trigger execution of any pending timers.
    sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
    // Wait to allow checkpoints of the pipeline
    waitToEnsureCheckpointingWorksCorrectly();
  }

  private void waitToEnsureCheckpointingWorksCorrectly() {
    // Do nothing, but still look busy ...
    // we can't return here since Flink requires that all operators stay up,
    // otherwise checkpointing would not work correctly anymore
    //
    // See https://issues.apache.org/jira/browse/FLINK-2491 for progress on this issue
    long idleStart = System.currentTimeMillis();
    // wait until this is canceled
    final Object waitLock = new Object();
    while (running && (System.currentTimeMillis() - idleStart < idleTimeoutMs)) {
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

  @Override
  public void cancel() {
    this.running = false;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {}

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    impulseEmitted =
        context
            .getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("impulse-emitted", BooleanSerializer.INSTANCE));
  }
}
