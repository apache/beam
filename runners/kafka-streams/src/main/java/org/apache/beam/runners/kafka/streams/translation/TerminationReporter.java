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
 * The bit of every watermark-emitting processor that reports it has finished, so a bounded pipeline
 * can stop itself. See {@link TerminationTracker} for why the runner has to work this out at all.
 *
 * <p>A processor creates one of these, calls {@link #init} from {@code Processor#init}, passes
 * every watermark it emits to {@link #watermarkEmitted}, and calls {@link #close} from {@code
 * Processor#close}.
 *
 * <h3>Why termination is scheduled rather than reported inline</h3>
 *
 * <p>Reporting from inside {@code process()} would announce the processor as finished while it is
 * still in the middle of handling the record that carried the terminal watermark. Scheduling a
 * punctuator instead defers the report until the current processing has completed, so anything that
 * has to happen after the final watermark — flushing a bundle, forwarding downstream, committing —
 * still runs first.
 *
 * <p>The punctuator is {@link PunctuationType#WALL_CLOCK_TIME} rather than stream time: no further
 * records arrive after the terminal watermark, so stream time would never advance and a stream-time
 * punctuator would never fire. The interval is the smallest Kafka Streams accepts — it rejects
 * anything below a millisecond with "The minimum supported scheduling interval is 1 millisecond."
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
