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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.CausedByDrain;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.util.TaskCompletionListener;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/**
 * Runs a stateful {@link DoFn} over the key groups of a {@code flatMapSortedGroups}, where the
 * elements of each group are already ordered by event time.
 *
 * <p>State is a plain heap object scoped to the key being processed and is dropped when that key is
 * done. This is what makes batch state cheap: there is no state store to bridge onto, so every Beam
 * state type is already implemented by {@link InMemoryStateInternals}. Memory is bounded by the
 * state a single key holds, not by the number of elements that key received.
 *
 * <p>The {@link DoFn} is set up <b>once per task</b> and torn down from a task completion listener,
 * not once per key. Spark calls {@link #call} per key group, but {@code @Setup}/{@code @Teardown}
 * bracket the lifetime of the {@link DoFn} instance, and there is a single instance per
 * deserialized closure, so tearing it down between keys would violate the contract that no method
 * runs after {@code @Teardown} (and would re-run expensive setup for every key). Each key gets its
 * own <em>bundle</em>, which is the level the model does allow to vary, and its own state and
 * timers via {@link MutableStepContext}.
 *
 * <p>Outputs are pulled lazily: the {@link DoFn} pushes into a buffer and the returned iterator
 * drains it, advancing the input only when the buffer runs dry, so neither a key with many elements
 * nor one with many timers is ever materialized.
 */
abstract class StatefulDoFnGroupFunction<K, InT extends KV<K, ?>, OutT>
    implements FlatMapGroupsFunction<K, WindowedValue<InT>, OutT> {

  private final Supplier<PipelineOptions> options;
  private final MetricsAccumulator metrics;
  private final DoFnRunnerFactory<InT, ?> factory;

  private transient @Nullable Deque<OutT> buffer;
  private transient @Nullable MutableStepContext stepContext;
  private transient @Nullable DoFnRunnerWithTeardown<InT, ?> doFnRunner;
  private transient boolean needsBundleStart;
  private transient boolean isTornDown;

  private StatefulDoFnGroupFunction(
      Supplier<PipelineOptions> options,
      MetricsAccumulator metrics,
      DoFnRunnerFactory<InT, ?> factory) {
    this.options = options;
    this.metrics = metrics;
    this.factory = factory;
  }

  /**
   * {@link StatefulDoFnGroupFunction} emitting a single output of type {@link WindowedValue} of
   * {@link FnOutT}.
   */
  static <K, InT extends KV<K, ?>, FnOutT>
      StatefulDoFnGroupFunction<K, InT, WindowedValue<FnOutT>> singleOutput(
          Supplier<PipelineOptions> options,
          MetricsAccumulator metrics,
          DoFnRunnerFactory<InT, FnOutT> factory) {
    return new SingleOut<>(options, metrics, factory);
  }

  /**
   * {@link StatefulDoFnGroupFunction} emitting multiple outputs encoded as tuple of column index
   * and {@link WindowedValue} of {@link OutT}, where column index corresponds to the index of a
   * {@link TupleTag#getId()} in {@code tagColIdx}.
   */
  static <K, InT extends KV<K, ?>, FnOutT, OutT>
      StatefulDoFnGroupFunction<K, InT, Tuple2<Integer, WindowedValue<OutT>>> multiOutput(
          Supplier<PipelineOptions> options,
          MetricsAccumulator metrics,
          DoFnRunnerFactory<InT, FnOutT> factory,
          Map<String, Integer> tagColIdx) {
    return new MultiOut<>(options, metrics, factory, tagColIdx);
  }

  @Override
  public Iterator<OutT> call(K key, Iterator<WindowedValue<InT>> values) {
    DoFnRunnerWithTeardown<InT, ?> runner = runner();
    // Fresh state and timers for this key; the DoFn instance itself is untouched.
    stepContext().reset(key);
    if (needsBundleStart) {
      needsBundleStart = false;
      runner.startBundle();
    }
    return new StatefulGroupIt(key, values, runner);
  }

  /**
   * The runner for this task, created on first use. {@code factory.create} invokes {@code @Setup}
   * and opens the first bundle, so this happens exactly once per task rather than once per key.
   */
  private DoFnRunnerWithTeardown<InT, ?> runner() {
    DoFnRunnerWithTeardown<InT, ?> runner = doFnRunner;
    if (runner == null) {
      MutableStepContext ctx = new MutableStepContext();
      Deque<OutT> buf = new ArrayDeque<>();
      buffer = buf;
      stepContext = ctx;
      runner = factory.create(options.get(), metrics, outputManager(buf), ctx);
      doFnRunner = runner;
      // Spark is free to abandon an iterator part way through (a downstream limit, a task kill, an
      // exception elsewhere in the stage). Tearing down from the task completion listener is the
      // only way to guarantee @Teardown runs and DoFn resources are released.
      TaskContext taskContext = TaskContext.get();
      if (taskContext != null) {
        // An explicit listener rather than a lambda: TaskContext overloads this for both the Scala
        // function and the Java interface, so a lambda is ambiguous.
        taskContext.addTaskCompletionListener(
            new TaskCompletionListener() {
              @Override
              public void onTaskCompletion(TaskContext context) {
                teardownOnce();
              }
            });
      }
    }
    return runner;
  }

  private MutableStepContext stepContext() {
    MutableStepContext ctx = stepContext;
    if (ctx == null) {
      throw new IllegalStateException("StepContext requested before the runner was created");
    }
    return ctx;
  }

  private Deque<OutT> buffer() {
    Deque<OutT> buf = buffer;
    if (buf == null) {
      throw new IllegalStateException("Buffer requested before the runner was created");
    }
    return buf;
  }

  private void teardownOnce() {
    DoFnRunnerWithTeardown<InT, ?> runner = doFnRunner;
    if (runner != null && !isTornDown) {
      isTornDown = true;
      runner.teardown();
    }
  }

  /** Output manager emitting outputs of type {@link OutT} to the buffer. */
  abstract WindowedValueMultiReceiver outputManager(Deque<OutT> buffer);

  /**
   * {@link StatefulDoFnGroupFunction} emitting a single output of type {@link WindowedValue} of
   * {@link FnOutT}.
   */
  private static class SingleOut<K, InT extends KV<K, ?>, FnOutT>
      extends StatefulDoFnGroupFunction<K, InT, WindowedValue<FnOutT>> {
    private SingleOut(
        Supplier<PipelineOptions> options,
        MetricsAccumulator metrics,
        DoFnRunnerFactory<InT, FnOutT> factory) {
      super(options, metrics, factory);
    }

    @Override
    WindowedValueMultiReceiver outputManager(Deque<WindowedValue<FnOutT>> buffer) {
      return new WindowedValueMultiReceiver() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          buffer.add((WindowedValue<FnOutT>) output);
        }
      };
    }
  }

  /**
   * {@link StatefulDoFnGroupFunction} emitting multiple outputs encoded as tuple of column index
   * and {@link WindowedValue} of {@link OutT}, where column index corresponds to the index of a
   * {@link TupleTag#getId()} in {@link #tagColIdx}.
   */
  private static class MultiOut<K, InT extends KV<K, ?>, FnOutT, OutT>
      extends StatefulDoFnGroupFunction<K, InT, Tuple2<Integer, WindowedValue<OutT>>> {
    private final Map<String, Integer> tagColIdx;

    private MultiOut(
        Supplier<PipelineOptions> options,
        MetricsAccumulator metrics,
        DoFnRunnerFactory<InT, FnOutT> factory,
        Map<String, Integer> tagColIdx) {
      super(options, metrics, factory);
      this.tagColIdx = tagColIdx;
    }

    @Override
    WindowedValueMultiReceiver outputManager(Deque<Tuple2<Integer, WindowedValue<OutT>>> buffer) {
      return new WindowedValueMultiReceiver() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          // Additional unused outputs can be skipped here. In that case columnIdx is null.
          Integer columnIdx = tagColIdx.get(tag.getId());
          if (columnIdx != null) {
            buffer.add(tuple(columnIdx, (WindowedValue<OutT>) output));
          }
        }
      };
    }
  }

  /**
   * A {@link StepContext} whose state and timers are swapped per key, so that one {@link DoFn} and
   * one {@link org.apache.beam.runners.core.DoFnRunner DoFnRunner} can serve every key of a task.
   *
   * <p>{@code SimpleDoFnRunner} re-reads {@code stateInternals()} on each access rather than
   * caching it, which is what makes rebinding safe.
   */
  private static class MutableStepContext implements StepContext {
    private @Nullable StateInternals stateInternals;
    private @Nullable InMemoryTimerInternals timerInternals;

    void reset(@Nullable Object key) {
      stateInternals = InMemoryStateInternals.forKey(key);
      timerInternals = new InMemoryTimerInternals();
    }

    InMemoryTimerInternals timers() {
      InMemoryTimerInternals timers = timerInternals;
      if (timers == null) {
        throw new IllegalStateException("StepContext used before reset");
      }
      return timers;
    }

    @Override
    public StateInternals stateInternals() {
      StateInternals state = stateInternals;
      if (state == null) {
        throw new IllegalStateException("StepContext used before reset");
      }
      return state;
    }

    @Override
    public TimerInternals timerInternals() {
      return timers();
    }
  }

  private class StatefulGroupIt extends AbstractIterator<OutT> {
    private final Iterator<WindowedValue<InT>> groupIt;
    private final K key;
    private final DoFnRunnerWithTeardown<InT, ?> runner;
    private final InMemoryTimerInternals timerInternals;

    private boolean areTimersDrained;
    private boolean clocksAdvanced;
    private boolean isBundleFinished;

    private StatefulGroupIt(
        K key, Iterator<WindowedValue<InT>> groupIt, DoFnRunnerWithTeardown<InT, ?> runner) {
      this.key = key;
      this.groupIt = groupIt;
      this.runner = runner;
      this.timerInternals = stepContext().timers();
    }

    @Override
    protected @CheckForNull OutT computeNext() {
      Deque<OutT> buffer = buffer();
      try {
        while (true) {
          if (!buffer.isEmpty()) {
            return buffer.remove();
          }
          if (groupIt.hasNext()) {
            runner.processElement(groupIt.next());
          } else if (!areTimersDrained) {
            // Timers fire while the bundle is still open (the model processes a key's timers
            // before finishBundle) and one at a time, so their output is pulled lazily too.
            areTimersDrained = !fireNextTimer();
          } else if (!isBundleFinished) {
            isBundleFinished = true;
            needsBundleStart = true; // the next key opens a fresh bundle
            runner.finishBundle(); // may produce more output
          } else {
            return endOfData(); // teardown is task scoped, not per key
          }
        }
      } catch (RuntimeException re) {
        teardownOnce();
        throw re;
      } catch (Exception e) {
        teardownOnce();
        throw new RuntimeException(e);
      }
    }

    /**
     * Fires at most one pending timer, returning whether one fired.
     *
     * <p>Polled once per {@code computeNext} rather than drained in a loop for two reasons. An
     * {@code OnTimer} method may set further event time timers, and those must fire too: draining a
     * snapshot silently truncates timer chains, the failure mode recorded for the RDD based runner
     * in <a href="https://issues.apache.org/jira/browse/BEAM-12712">BEAM-12712</a>. And firing one
     * at a time keeps a timer heavy key from having to buffer all of its output at once.
     *
     * <p>The clocks only need advancing once: {@code removeNext*} reads them live. As both clocks
     * are pinned at {@code TIMESTAMP_MAX_VALUE}, a processing time timer re-armed from {@code
     * OnTimer} targets a time past the pinned clock and never becomes eligible; batch mode makes no
     * processing time guarantees (the RDD based runner drains the same way).
     */
    private boolean fireNextTimer() throws Exception {
      if (!clocksAdvanced) {
        clocksAdvanced = true;
        timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);
        timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
        timerInternals.advanceSynchronizedProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);
      }
      TimerData timer = nextTimer();
      if (timer == null) {
        return false;
      }
      fire(timer);
      return true;
    }

    private @Nullable TimerData nextTimer() {
      TimerData timer = timerInternals.removeNextEventTimer();
      if (timer == null) {
        timer = timerInternals.removeNextProcessingTimer();
      }
      if (timer == null) {
        timer = timerInternals.removeNextSynchronizedProcessingTimer();
      }
      return timer;
    }

    private void fire(TimerData timer) {
      BoundedWindow window =
          ((StateNamespaces.WindowNamespace<?>) timer.getNamespace()).getWindow();
      runner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          key,
          window,
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain(),
          CausedByDrain.NORMAL);
    }
  }
}
