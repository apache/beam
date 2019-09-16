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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A streaming source that periodically produces an empty byte array. This is mostly useful for
 * debugging, or for triggering periodic behavior in a portable pipeline.
 *
 * @deprecated Legacy non-portable source which can be replaced by a DoFn with timers.
 */
@Deprecated
public class StreamingImpulseSource extends RichParallelSourceFunction<WindowedValue<byte[]>> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingImpulseSource.class);

  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private long count = 0;
  private final int intervalMillis;
  private final int messageCount;

  public StreamingImpulseSource(int intervalMillis, int messageCount) {
    this.intervalMillis = intervalMillis;
    this.messageCount = messageCount;
  }

  @Override
  public void run(SourceContext<WindowedValue<byte[]>> ctx) {
    // in order to produce messageCount messages across all parallel subtasks, we divide by
    // the total number of subtasks
    int subtaskCount = messageCount / getRuntimeContext().getNumberOfParallelSubtasks();
    // if the message count is not evenly divisible by the number of subtasks, add an estra
    // message to the first (messageCount % subtasksCount) subtasks
    if (getRuntimeContext().getIndexOfThisSubtask()
        < (messageCount % getRuntimeContext().getNumberOfParallelSubtasks())) {
      subtaskCount++;
    }

    while (!cancelled.get() && (messageCount == 0 || count < subtaskCount)) {
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(WindowedValue.valueInGlobalWindow(new byte[] {}));
        count++;
      }

      try {
        if (intervalMillis > 0) {
          Thread.sleep(intervalMillis);
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while sleeping", e);
      }
    }
  }

  @Override
  public void cancel() {
    this.cancelled.set(true);
  }
}
