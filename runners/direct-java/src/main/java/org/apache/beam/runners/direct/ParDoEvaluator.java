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
package org.apache.beam.runners.direct;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.CopyOnAccessInMemoryStateInternals;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

class ParDoEvaluator<T> implements TransformEvaluator<T> {
  public static <InputT, OutputT> ParDoEvaluator<InputT> create(
      EvaluationContext evaluationContext,
      DirectStepContext stepContext,
      CommittedBundle<InputT> inputBundle,
      AppliedPTransform<PCollection<InputT>, ?, ?> application,
      OldDoFn<InputT, OutputT> fn,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      Map<TupleTag<?>, PCollection<?>> outputs) {
    DirectExecutionContext executionContext =
        evaluationContext.getExecutionContext(application, inputBundle.getKey());

    AggregatorContainer.Mutator aggregatorChanges = evaluationContext.getAggregatorMutator();

    Map<TupleTag<?>, UncommittedBundle<?>> outputBundles = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      outputBundles.put(
          outputEntry.getKey(),
          evaluationContext.createBundle(outputEntry.getValue()));
    }

    ReadyCheckingSideInputReader sideInputReader =
        evaluationContext.createSideInputReader(sideInputs);
    DoFnRunner<InputT, OutputT> underlying =
        DoFnRunners.createDefault(
            evaluationContext.getPipelineOptions(),
            fn,
            sideInputReader,
            BundleOutputManager.create(outputBundles),
            mainOutputTag,
            sideOutputTags,
            stepContext,
            aggregatorChanges,
            application.getInput().getWindowingStrategy());
    PushbackSideInputDoFnRunner<InputT, OutputT> runner =
        PushbackSideInputDoFnRunner.create(underlying, sideInputs, sideInputReader);

    try {
      runner.startBundle();
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }

    return new ParDoEvaluator<>(
        runner, application, aggregatorChanges, outputBundles.values(), stepContext);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private final PushbackSideInputDoFnRunner<T, ?> fnRunner;
  private final AppliedPTransform<PCollection<T>, ?, ?> transform;
  private final AggregatorContainer.Mutator aggregatorChanges;
  private final Collection<UncommittedBundle<?>> outputBundles;
  private final DirectStepContext stepContext;

  private final ImmutableList.Builder<WindowedValue<T>> unprocessedElements;

  private ParDoEvaluator(
      PushbackSideInputDoFnRunner<T, ?> fnRunner,
      AppliedPTransform<PCollection<T>, ?, ?> transform,
      AggregatorContainer.Mutator aggregatorChanges,
      Collection<UncommittedBundle<?>> outputBundles,
      DirectStepContext stepContext) {
    this.fnRunner = fnRunner;
    this.transform = transform;
    this.outputBundles = outputBundles;
    this.stepContext = stepContext;
    this.aggregatorChanges = aggregatorChanges;
    this.unprocessedElements = ImmutableList.builder();
  }

  @Override
  public void processElement(WindowedValue<T> element) {
    try {
      Iterable<WindowedValue<T>> unprocessed = fnRunner.processElementInReadyWindows(element);
      unprocessedElements.addAll(unprocessed);
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
  }

  @Override
  public TransformResult finishBundle() {
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
        .withAggregatorChanges(aggregatorChanges)
        .addUnprocessedElements(unprocessedElements.build())
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
