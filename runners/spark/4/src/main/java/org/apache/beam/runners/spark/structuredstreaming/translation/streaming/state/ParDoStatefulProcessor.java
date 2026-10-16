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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import java.util.List;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.spark.stateful.SparkStateInternals;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.StatefulTaskRunner;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import scala.collection.Iterator;

/** Runs a stateful {@link DoFn} within Spark 4 Structured Streaming {@code transformWithState}. */
public class ParDoStatefulProcessor<K, V, OutputT> extends BeamStatefulProcessor<K, V, OutputT> {

  private final DoFn<KV<K, V>, OutputT> doFn;
  private final DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory;
  private final Supplier<PipelineOptions> optionsSupplier;
  private final MetricsAccumulator metrics;

  private transient boolean needsBundleStart;

  public ParDoStatefulProcessor(
      DoFn<KV<K, V>, OutputT> doFn,
      DoFnRunnerFactory<KV<K, V>, OutputT> runnerFactory,
      KvCoder<K, V> inputCoder,
      WindowingStrategy<?, ?> windowingStrategy,
      Supplier<PipelineOptions> optionsSupplier,
      MetricsAccumulator metrics) {
    super(inputCoder, windowingStrategy);
    this.doFn = doFn;
    this.runnerFactory = runnerFactory;
    this.optionsSupplier = optionsSupplier;
    this.metrics = metrics;
  }

  @Override
  protected void processElements(
      K key, Iterator<WindowedValue<KV<K, V>>> rows, SparkTimerInternals timerInternals) {
    DoFnRunner<KV<K, V>, OutputT> runner = keyRunner(key, timerInternals);
    while (rows.hasNext()) {
      runner.processElement(rows.next());
    }
    // The next bundle opens in keyRunner.
    runner.finishBundle();
    needsBundleStart = true;
  }

  @Override
  protected void processTimers(K key, SparkTimerInternals timerInternals) {
    if (dueEventTimeTimers(timerInternals).isEmpty()) {
      return;
    }
    DoFnRunner<KV<K, V>, OutputT> runner = keyRunner(key, timerInternals);
    fireDueTimers(key, timerInternals, runner);
    // The next bundle opens in keyRunner.
    runner.finishBundle();
    needsBundleStart = true;
  }

  private void fireDueTimers(
      K key, SparkTimerInternals timerInternals, DoFnRunner<KV<K, V>, OutputT> runner) {
    while (true) {
      List<TimerData> due = dueEventTimeTimers(timerInternals);
      if (due.isEmpty()) {
        break;
      }
      TimerData next = due.get(0);
      timerInternals.deleteTimer(next);
      StatefulTaskRunner.fireTimer(runner, key, next);
    }
  }

  private DoFnRunnerWithTeardown<KV<K, V>, OutputT> baseRunner(
      SparkStateInternals<K> stateInternals, SparkTimerInternals timerInternals) {
    return taskRunner.getOrCreate(
        ctx -> {
          ctx.set(stateInternals, timerInternals);
          return runnerFactory.create(optionsSupplier.get(), metrics, outputReceiver(), ctx);
        });
  }

  private DoFnRunner<KV<K, V>, OutputT> keyRunner(K key, SparkTimerInternals timerInternals) {
    SparkStateInternals<K> stateInternals = stateInternals(key);
    DoFnRunnerWithTeardown<KV<K, V>, OutputT> base = baseRunner(stateInternals, timerInternals);
    taskRunner.stepContext().set(stateInternals, timerInternals);

    // Closed in processElements and processTimers.
    if (needsBundleStart) {
      needsBundleStart = false;
      base.startBundle();
    }

    StatefulDoFnRunner.CleanupTimer<KV<K, V>> cleanupTimer =
        new StatefulDoFnRunner.TimeInternalsCleanupTimer<KV<K, V>>(
            timerInternals, windowingStrategy) {
          @Override
          public void setForWindow(KV<K, V> input, BoundedWindow window) {
            // GlobalWindow state is never garbage collected, as in the Flink runner.
            if (!window.equals(GlobalWindow.INSTANCE)) {
              super.setForWindow(input, window);
            }
          }
        };
    StatefulDoFnRunner.StateCleaner<BoundedWindow> stateCleaner =
        new StatefulDoFnRunner.StateInternalsStateCleaner<>(doFn, stateInternals, windowCoder);

    return DoFnRunners.defaultStatefulDoFnRunner(
        doFn,
        inputCoder,
        base,
        taskRunner.stepContext(),
        windowingStrategy,
        cleanupTimer,
        stateCleaner);
  }
}
