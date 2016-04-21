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
package org.apache.beam.sdk.runners.inprocess;

import org.apache.beam.sdk.runners.inprocess.InProcessExecutionContext.InProcessStepContext;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.DoFnRunner;
import org.apache.beam.sdk.util.DoFnRunners;
import org.apache.beam.sdk.util.DoFnRunners.OutputManager;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.CounterSet;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ParDoInProcessEvaluator<T> implements TransformEvaluator<T> {
  public static <InputT, OutputT> ParDoInProcessEvaluator<InputT> create(
      InProcessEvaluationContext evaluationContext,
      CommittedBundle<InputT> inputBundle,
      AppliedPTransform<PCollection<InputT>, ?, ?> application,
      DoFn<InputT, OutputT> fn,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      Map<TupleTag<?>, PCollection<?>> outputs) {
    InProcessExecutionContext executionContext =
        evaluationContext.getExecutionContext(application, inputBundle.getKey());
    String stepName = evaluationContext.getStepName(application);
    InProcessStepContext stepContext =
        executionContext.getOrCreateStepContext(stepName, stepName);

    CounterSet counters = evaluationContext.createCounterSet();

    Map<TupleTag<?>, UncommittedBundle<?>> outputBundles = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      outputBundles.put(
          outputEntry.getKey(),
          evaluationContext.createBundle(inputBundle, outputEntry.getValue()));
    }

    DoFnRunner<InputT, OutputT> runner =
        DoFnRunners.createDefault(
            evaluationContext.getPipelineOptions(),
            SerializableUtils.clone(fn),
            evaluationContext.createSideInputReader(sideInputs),
            BundleOutputManager.create(outputBundles),
            mainOutputTag,
            sideOutputTags,
            stepContext,
            counters.getAddCounterMutator(),
            application.getInput().getWindowingStrategy());

    try {
      runner.startBundle();
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }

    return new ParDoInProcessEvaluator<>(
        runner, application, counters, outputBundles.values(), stepContext);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private final DoFnRunner<T, ?> fnRunner;
  private final AppliedPTransform<PCollection<T>, ?, ?> transform;
  private final CounterSet counters;
  private final Collection<UncommittedBundle<?>> outputBundles;
  private final InProcessStepContext stepContext;

  private ParDoInProcessEvaluator(
      DoFnRunner<T, ?> fnRunner,
      AppliedPTransform<PCollection<T>, ?, ?> transform,
      CounterSet counters,
      Collection<UncommittedBundle<?>> outputBundles,
      InProcessStepContext stepContext) {
    this.fnRunner = fnRunner;
    this.transform = transform;
    this.counters = counters;
    this.outputBundles = outputBundles;
    this.stepContext = stepContext;
  }

  @Override
  public void processElement(WindowedValue<T> element) {
    try {
      fnRunner.processElement(element);
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
  }

  @Override
  public InProcessTransformResult finishBundle() {
    try {
      fnRunner.finishBundle();
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
    StepTransformResult.Builder resultBuilder;
    CopyOnAccessInMemoryStateInternals<?> state = stepContext.commitState();
    if (state != null) {
      resultBuilder =
          StepTransformResult.withHold(transform, state.getEarliestWatermarkHold())
              .withState(state);
    } else {
      resultBuilder = StepTransformResult.withoutHold(transform);
    }
    return resultBuilder
        .addOutput(outputBundles)
        .withTimerUpdate(stepContext.getTimerUpdate())
        .withCounters(counters)
        .build();
  }

  static class BundleOutputManager implements OutputManager {
    private final Map<TupleTag<?>, UncommittedBundle<?>> bundles;
    private final Map<TupleTag<?>, List<?>> undeclaredOutputs;

    public static BundleOutputManager create(Map<TupleTag<?>, UncommittedBundle<?>> outputBundles) {
      return new BundleOutputManager(outputBundles);
    }

    private BundleOutputManager(Map<TupleTag<?>, UncommittedBundle<?>> bundles) {
      this.bundles = bundles;
      undeclaredOutputs = new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      @SuppressWarnings("rawtypes")
      UncommittedBundle bundle = bundles.get(tag);
      if (bundle == null) {
        List undeclaredContents = undeclaredOutputs.get(tag);
        if (undeclaredContents == null) {
          undeclaredContents = new ArrayList<T>();
          undeclaredOutputs.put(tag, undeclaredContents);
        }
        undeclaredContents.add(output);
      } else {
        bundle.add(output);
      }
    }
  }
}
