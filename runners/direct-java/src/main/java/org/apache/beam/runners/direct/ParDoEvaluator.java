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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.PushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunner;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

class ParDoEvaluator<InputT> implements TransformEvaluator<InputT> {

  public interface DoFnRunnerFactory<InputT, OutputT> {
    PushbackSideInputDoFnRunner<InputT, OutputT> createRunner(
        PipelineOptions options,
        DoFn<InputT, OutputT> fn,
        List<PCollectionView<?>> sideInputs,
        ReadyCheckingSideInputReader sideInputReader,
        OutputManager outputManager,
        TupleTag<OutputT> mainOutputTag,
        List<TupleTag<?>> additionalOutputTags,
        DirectStepContext stepContext,
        WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy);
  }

  public static <InputT, OutputT> DoFnRunnerFactory<InputT, OutputT> defaultRunnerFactory() {
    return new DoFnRunnerFactory<InputT, OutputT>() {
      @Override
      public PushbackSideInputDoFnRunner<InputT, OutputT> createRunner(
          PipelineOptions options,
          DoFn<InputT, OutputT> fn,
          List<PCollectionView<?>> sideInputs,
          ReadyCheckingSideInputReader sideInputReader,
          OutputManager outputManager,
          TupleTag<OutputT> mainOutputTag,
          List<TupleTag<?>> additionalOutputTags,
          DirectStepContext stepContext,
          WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy) {
        DoFnRunner<InputT, OutputT> underlying =
            DoFnRunners.simpleRunner(
                options,
                fn,
                sideInputReader,
                outputManager,
                mainOutputTag,
                additionalOutputTags,
                stepContext,
                windowingStrategy);
        return SimplePushbackSideInputDoFnRunner.create(underlying, sideInputs, sideInputReader);
      }
    };
  }

  public static <InputT, OutputT> ParDoEvaluator<InputT> create(
      EvaluationContext evaluationContext,
      DirectStepContext stepContext,
      AppliedPTransform<?, ?, ?> application,
      WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy,
      DoFn<InputT, OutputT> fn,
      StructuralKey<?> key,
      List<PCollectionView<?>> sideInputs,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Map<TupleTag<?>, PCollection<?>> outputs,
      DoFnRunnerFactory<InputT, OutputT> runnerFactory) {

    BundleOutputManager outputManager = createOutputManager(evaluationContext, key, outputs);

    ReadyCheckingSideInputReader sideInputReader =
        evaluationContext.createSideInputReader(sideInputs);

    PushbackSideInputDoFnRunner<InputT, OutputT> runner = runnerFactory.createRunner(
        evaluationContext.getPipelineOptions(),
        fn,
        sideInputs,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        stepContext,
        windowingStrategy);

    return create(runner, stepContext, application, outputManager);
  }

  public static <InputT, OutputT> ParDoEvaluator<InputT> create(
      PushbackSideInputDoFnRunner<InputT, OutputT> runner,
      DirectStepContext stepContext,
      AppliedPTransform<?, ?, ?> application,
      BundleOutputManager outputManager) {
    return new ParDoEvaluator<>(runner, application, outputManager, stepContext);
  }

  static BundleOutputManager createOutputManager(
      EvaluationContext evaluationContext,
      StructuralKey<?> key,
      Map<TupleTag<?>, PCollection<?>> outputs) {
    Map<TupleTag<?>, UncommittedBundle<?>> outputBundles = new HashMap<>();
    for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
      // Just trust the context's decision as to whether the output should be keyed.
      // The logic for whether this ParDo is key-preserving and whether the input
      // is keyed lives elsewhere.
      if (evaluationContext.isKeyed(outputEntry.getValue())) {
        outputBundles.put(
            outputEntry.getKey(), evaluationContext.createKeyedBundle(key, outputEntry.getValue()));
      } else {
        outputBundles.put(
            outputEntry.getKey(), evaluationContext.createBundle(outputEntry.getValue()));
      }
    }
    return BundleOutputManager.create(outputBundles);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  private final PushbackSideInputDoFnRunner<InputT, ?> fnRunner;
  private final AppliedPTransform<?, ?, ?> transform;
  private final BundleOutputManager outputManager;
  private final DirectStepContext stepContext;

  private final ImmutableList.Builder<WindowedValue<InputT>> unprocessedElements;

  private ParDoEvaluator(
      PushbackSideInputDoFnRunner<InputT, ?> fnRunner,
      AppliedPTransform<?, ?, ?> transform,
      BundleOutputManager outputManager,
      DirectStepContext stepContext) {
    this.fnRunner = fnRunner;
    this.transform = transform;
    this.outputManager = outputManager;
    this.stepContext = stepContext;
    this.unprocessedElements = ImmutableList.builder();

    try {
      fnRunner.startBundle();
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
  }

  public BundleOutputManager getOutputManager() {
    return outputManager;
  }

  @Override
  public void processElement(WindowedValue<InputT> element) {
    try {
      Iterable<WindowedValue<InputT>> unprocessed = fnRunner.processElementInReadyWindows(element);
      unprocessedElements.addAll(unprocessed);
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
  }

  public void onTimer(TimerData timer, BoundedWindow window) {
    try {
      fnRunner.onTimer(timer.getTimerId(), window, timer.getTimestamp(), timer.getDomain());
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
  }

  @Override
  public TransformResult<InputT> finishBundle() {
    try {
      fnRunner.finishBundle();
    } catch (Exception e) {
      throw UserCodeException.wrap(e);
    }
    StepTransformResult.Builder<InputT> resultBuilder;
    CopyOnAccessInMemoryStateInternals state = stepContext.commitState();
    if (state != null) {
      resultBuilder =
          StepTransformResult.<InputT>withHold(transform, state.getEarliestWatermarkHold())
              .withState(state);
    } else {
      resultBuilder = StepTransformResult.withoutHold(transform);
    }
    return resultBuilder
        .addOutput(outputManager.bundles.values())
        .withTimerUpdate(stepContext.getTimerUpdate())
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      UncommittedBundle bundle = bundles.get(tag);
      if (bundle == null) {
        List<WindowedValue<T>> undeclaredContents = (List) undeclaredOutputs.get(tag);
        if (undeclaredContents == null) {
          undeclaredContents = new ArrayList<>();
          undeclaredOutputs.put(tag, undeclaredContents);
        }
        undeclaredContents.add(output);
      } else {
        bundle.add(output);
      }
    }
  }
}
