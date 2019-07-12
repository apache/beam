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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** Helpers to use {@link ReduceFnRunner}. */
public class ReduceFnRunnerHelpers<K, InputT, W extends BoundedWindow> {
  public static <K, InputT, W extends BoundedWindow> void fireEligibleTimers(
      InMemoryTimerInternals timerInternals,
      ReduceFnRunner<K, InputT, Iterable<InputT>, W> reduceFnRunner)
      throws Exception {
    List<TimerInternals.TimerData> timers = new ArrayList<>();
    while (true) {
      TimerInternals.TimerData timer;
      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        timers.add(timer);
      }
      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        timers.add(timer);
      }
      if (timers.isEmpty()) {
        break;
      }
      reduceFnRunner.onTimers(timers);
      timers.clear();
    }
  }

  /** {@link OutputWindowedValue} for ReduceFnRunner. */
  public static class GABWOutputWindowedValue<K, V>
      implements OutputWindowedValue<KV<K, Iterable<V>>> {
    private final List<WindowedValue<KV<K, Iterable<V>>>> outputs = new ArrayList<>();

    @Override
    public void outputWindowedValue(
        KV<K, Iterable<V>> output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      outputs.add(WindowedValue.of(output, timestamp, windows, pane));
    }

    @Override
    public <AdditionalOutputT> void outputWindowedValue(
        TupleTag<AdditionalOutputT> tag,
        AdditionalOutputT output,
        Instant timestamp,
        Collection<? extends BoundedWindow> windows,
        PaneInfo pane) {
      throw new UnsupportedOperationException("GroupAlsoByWindow should not use tagged outputs.");
    }

    public Iterable<WindowedValue<KV<K, Iterable<V>>>> getOutputs() {
      return outputs;
    }
  }
}
