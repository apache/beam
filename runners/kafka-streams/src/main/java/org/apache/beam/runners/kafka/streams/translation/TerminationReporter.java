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
package org.apache.beam.runners.kafka.streams.translation;

import java.time.Duration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The part of every watermark-emitting processor that reports it has finished, so a bounded
 * pipeline can stop itself; see {@link TerminationTracker} for why that has to be worked out at
 * all. A processor calls {@link #init} from {@code Processor#init}, passes every watermark it emits
 * to {@link #watermarkEmitted}, and calls {@link #close} from {@code Processor#close}.
 *
 * <p>The report is scheduled rather than made inline, because reporting from inside {@code
 * process()} would announce the processor finished while it is still handling the record that
 * carried the terminal watermark; deferring it lets flushing, forwarding and committing happen
 * first.
 *
 * <p>It uses {@link PunctuationType#WALL_CLOCK_TIME}: no records arrive after the terminal
 * watermark, so stream time would never advance and a stream-time punctuator would never fire. The
 * interval is 1ms, the smallest Kafka Streams accepts.
 */
class TerminationReporter {

  /** Kafka Streams rejects any scheduling interval below this. */
  private static final Duration IMMEDIATELY = Duration.ofMillis(1);

  private final TerminationTracker tracker;
  private final String transformId;

  private @Nullable String instanceId;
  private @Nullable Cancellable scheduled;
  private boolean reported;

  TerminationReporter(TerminationTracker tracker, String transformId) {
    this.tracker = tracker;
    this.transformId = transformId;
  }

  /** Registers this processor instance as something the pipeline is waiting on. */
  void init(ProcessorContext<?, ?> context) {
    // The task is what makes the id unique: one processor node runs as one instance per task.
    this.instanceId = transformId + "#" + context.taskId();
    tracker.register(instanceId);
  }

  /**
   * Called with every watermark the processor emits. Once that watermark is terminal, schedules the
   * report that this processor has no further work.
   */
  void watermarkEmitted(ProcessorContext<?, ?> context, long watermarkMillis) {
    if (reported || watermarkMillis < BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
      return;
    }
    reported = true;
    scheduled =
        context.schedule(
            IMMEDIATELY,
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> {
              cancelSchedule();
              String id = instanceId;
              if (id != null) {
                tracker.terminate(id);
              }
            });
  }

  /** Stops the pipeline waiting on this processor, e.g. when its task migrates on a rebalance. */
  void close() {
    cancelSchedule();
    String id = instanceId;
    if (id != null) {
      tracker.unregister(id);
      instanceId = null;
    }
  }

  private void cancelSchedule() {
    Cancellable handle = scheduled;
    if (handle != null) {
      handle.cancel();
      scheduled = null;
    }
  }
}
