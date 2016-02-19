/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.DoFnRunners.OutputManager;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import org.joda.time.Instant;

import java.util.Collection;
import java.util.Map;

class ParDoInProcessEvaluator<T> {
  private final DoFnRunner<T, ?> fnRunner;
  private final AppliedPTransform<PCollection<T>, ?, ?> transform;
  private final CounterSet counters;
  private final Collection<UncommittedBundle<?>> outputBundles;

  public ParDoInProcessEvaluator(DoFnRunner<T, ?> fnRunner,
      AppliedPTransform<PCollection<T>, ?, ?> transform, CounterSet counters,
      Collection<UncommittedBundle<?>> outputBundles) {
    this.fnRunner = fnRunner;
    this.transform = transform;
    this.counters = counters;
    this.outputBundles = outputBundles;
  }

  public void processElement(WindowedValue<T> element) {
    fnRunner.processElement(element);
  }

  public InProcessTransformResult finishBundle() {
    fnRunner.finishBundle();
    // TODO Use a real value
    Instant hold = BoundedWindow.TIMESTAMP_MAX_VALUE;
    return StepTransformResult.withHold(transform, hold)
        .addOutput(outputBundles)
        .withCounters(counters)
        .build();
  }

  static class BundleOutputManager implements OutputManager {
    private final Map<TupleTag<?>, UncommittedBundle<?>> bundles;

    public static BundleOutputManager create(Map<TupleTag<?>, UncommittedBundle<?>> outputBundles) {
      return new BundleOutputManager(outputBundles);
    }

    private BundleOutputManager(Map<TupleTag<?>, UncommittedBundle<?>> bundles) {
      this.bundles = bundles;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      @SuppressWarnings("rawtypes")
      UncommittedBundle bundle = bundles.get(tag);
      bundle.add(output);
    }
  }
}

