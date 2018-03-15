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

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.runners.core.ActiveWindowSet;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.MergingActiveWindowSet;
import org.apache.beam.runners.core.NonMergingActiveWindowSet;
import org.apache.beam.runners.core.ReduceFn;
import org.apache.beam.runners.core.ReduceFnRunner.OnMergeCallback;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory.StatefulParDo;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/** A {@link TransformEvaluatorFactory} for stateful {@link ParDo}. */
final class StatefulParDoEvaluatorFactory<K, InputT, OutputT> implements TransformEvaluatorFactory {

  private final EvaluationContext evaluationContext;
  private final LoadingCache<AppliedPTransformKeyAndWindow<K, InputT, OutputT>, Runnable>
      cleanupRegistry;
  private final Cache<AppliedPTransformAndKey<K, InputT, OutputT>, ActiveWindowSet> activeWindows;

  private final ParDoEvaluatorFactory<KV<K, InputT>, OutputT> delegateFactory;

  StatefulParDoEvaluatorFactory(EvaluationContext evaluationContext) {
    this.delegateFactory =
        new ParDoEvaluatorFactory<>(
            evaluationContext,
            ParDoEvaluator.<KV<K, InputT>, OutputT>defaultRunnerFactory(),
            new CacheLoader<AppliedPTransform<?, ?, ?>, DoFnLifecycleManager>() {
              @Override
              public DoFnLifecycleManager load(AppliedPTransform<?, ?, ?> appliedStatefulParDo)
                  throws Exception {
                // StatefulParDo is overridden after the portable pipeline is received, so we
                // do not go through the portability translation layers
                StatefulParDo<?, ?, ?> statefulParDo =
                    (StatefulParDo<?, ?, ?>) appliedStatefulParDo.getTransform();
                return DoFnLifecycleManager.of(statefulParDo.getDoFn());
              }
            });
    this.evaluationContext = evaluationContext;
    this.cleanupRegistry =
        CacheBuilder.newBuilder()
            .weakValues()
            .build(new CleanupSchedulingLoader(evaluationContext));
    this.activeWindows =
        CacheBuilder.newBuilder()
        .weakValues()
        .build(new CacheLoader<AppliedPTransformAndKey<K, InputT, OutputT>, ActiveWindowSet>() {
          @Override
          public ActiveWindowSet<BoundedWindow> load(
              AppliedPTransformAndKey<K, InputT, OutputT> transformAndKey) {
            WindowingStrategy<InputT, BoundedWindow> windowingStrategy = getWindowingStrategy(
                transformAndKey.getTransform());
            final DirectStepContext stepContext = getStepContext(
                transformAndKey.getTransform(), transformAndKey.getKey());

            return windowingStrategy.getWindowFn().isNonMerging()
                ?  new NonMergingActiveWindowSet<>()
                :  new MergingActiveWindowSet<>(
                    windowingStrategy.getWindowFn(), stepContext.stateInternals());
          }
        });
  }

  @Override
  public <T> TransformEvaluator<T> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    TransformEvaluator<T> evaluator =
        (TransformEvaluator<T>)
            createEvaluator((AppliedPTransform) application, (CommittedBundle) inputBundle);
    return evaluator;
  }

  @Override
  public void cleanup() throws Exception {
    delegateFactory.cleanup();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TransformEvaluator<KeyedWorkItem<K, KV<K, InputT>>> createEvaluator(
      AppliedPTransform<
              PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
              StatefulParDo<K, InputT, OutputT>>
          application,
      CommittedBundle<KeyedWorkItem<K, KV<K, InputT>>> inputBundle)
      throws Exception {

    final DoFn<KV<K, InputT>, OutputT> doFn =
        application.getTransform().getDoFn();
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    // If the DoFn is stateful, schedule state clearing.
    // It is semantically correct to schedule any number of redundant clear tasks; the
    // cache is used to limit the number of tasks to avoid performance degradation.
    if (signature.stateDeclarations().size() > 0) {
      for (final WindowedValue<?> element : inputBundle.getElements()) {
        for (final BoundedWindow window : element.getWindows()) {
          cleanupRegistry.get(
              AppliedPTransformKeyAndWindow.create(
                  application, (StructuralKey<K>) inputBundle.getKey(), window));
        }
      }
    }

    DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator =
        delegateFactory.createEvaluator(
            (AppliedPTransform) application,
            (PCollection) inputBundle.getPCollection(),
            inputBundle.getKey(),
            application.getTransform().getSideInputs(),
            application.getTransform().getMainOutputTag(),
            application.getTransform().getAdditionalOutputTags().getAll());

    return new StatefulParDoEvaluator<>(delegateEvaluator);
  }

  private <K, InputT, OutputT, W extends BoundedWindow> WindowingStrategy<InputT, W> getWindowingStrategy(
      AppliedPTransform<PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
      StatefulParDo<K, InputT, OutputT>> transform) {
    return ParDoTranslation.getMainInput(transform).getWindowingStrategy();
    Map<TupleTag<?>, PCollection<OutputT>> taggedValues = new HashMap<>();
    for (Entry<TupleTag<?>, PValue> pv : transform.getOutputs().entrySet()) {
      taggedValues.put(pv.getKey(), (PCollection<OutputT>) pv.getValue());
    }
    PCollection<OutputT> pc = taggedValues.get(transform.getTransform().getMainOutputTag());
    return (WindowingStrategy<InputT, W>) pc.getWindowingStrategy();
  }

  private <K> DirectStepContext getStepContext(
      AppliedPTransform<PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
          StatefulParDo<K, InputT, OutputT>> transform, StructuralKey<K> key) {
    String stepName = evaluationContext.getStepName(transform);
    return evaluationContext
        .getExecutionContext(transform, key)
        .getStepContext(stepName);
  }

  private class CleanupSchedulingLoader
      extends CacheLoader<AppliedPTransformKeyAndWindow<K, InputT, OutputT>, Runnable> {

    private final EvaluationContext evaluationContext;

    public CleanupSchedulingLoader(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Runnable load(
        final AppliedPTransformKeyAndWindow<K, InputT, OutputT> transformOutputWindow) {
      WindowingStrategy<?, ?> windowingStrategy = getWindowingStrategy(
          transformOutputWindow.getTransform());
      BoundedWindow window = transformOutputWindow.getWindow();
      final DoFn<?, ?> doFn =
          transformOutputWindow.getTransform().getTransform().getDoFn();
      final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

      final DirectStepContext stepContext = getStepContext(
          transformOutputWindow.getTransform(), transformOutputWindow.getKey());

      final StateNamespace namespace =
          StateNamespaces.window(
              (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(), window);

      Runnable cleanup =
          () -> {
            for (StateDeclaration stateDecl : signature.stateDeclarations().values()) {
              StateTag<?> tag;
              try {
                tag = StateTags.tagForSpec(stateDecl.id(), (StateSpec) stateDecl.field().get(doFn));
              } catch (IllegalAccessException e) {
                throw new RuntimeException(
                    String.format(
                        "Error accessing %s for %s",
                        StateSpec.class.getName(), doFn.getClass().getName()),
                    e);
              }
              stepContext.stateInternals().state(namespace, tag).clear();
            }
            cleanupRegistry.invalidate(transformOutputWindow);
          };

      evaluationContext.scheduleAfterWindowExpiration(
          transformOutputWindow.getTransform(), window, windowingStrategy, cleanup);
      return cleanup;
    }
  }

  @AutoValue
  abstract static class AppliedPTransformAndKey<K, InputT, OutputT> {
    abstract AppliedPTransform<
        PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
        StatefulParDo<K, InputT, OutputT>>
    getTransform();

    abstract StructuralKey<K> getKey();

    static <K, InputT, OutputT> AppliedPTransformAndKey<K, InputT, OutputT> create(
        AppliedPTransform<
            PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
            StatefulParDo<K, InputT, OutputT>>
            transform,
        StructuralKey<K> key) {
      return new AutoValue_StatefulParDoEvaluatorFactory_AppliedPTransformAndKey<>(
          transform, key);
    }
  }

  }
  @AutoValue
  abstract static class AppliedPTransformKeyAndWindow<K, InputT, OutputT> {
    abstract AppliedPTransform<
            PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
            StatefulParDo<K, InputT, OutputT>>
        getTransform();

    abstract StructuralKey<K> getKey();

    abstract BoundedWindow getWindow();

    static <K, InputT, OutputT> AppliedPTransformKeyAndWindow<K, InputT, OutputT> create(
        AppliedPTransform<
                PCollection<? extends KeyedWorkItem<K, KV<K, InputT>>>, PCollectionTuple,
                StatefulParDo<K, InputT, OutputT>>
            transform,
        StructuralKey<K> key,
        BoundedWindow w) {
      return new AutoValue_StatefulParDoEvaluatorFactory_AppliedPTransformKeyAndWindow<>(
          transform, key, w);
    }
  }

  private static class StatefulParDoEvaluator<K, InputT>
      implements TransformEvaluator<KeyedWorkItem<K, KV<K, InputT>>> {

    private final DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator;

    public StatefulParDoEvaluator(
        DoFnLifecycleManagerRemovingTransformEvaluator<KV<K, InputT>> delegateEvaluator) {
      this.delegateEvaluator = delegateEvaluator;
    }

    /**
     * Extract the windows associated with the values.
     */
    private Set<BoundedWindow> collectWindows(
        Iterable<WindowedValue<KV<K, InputT>>> values) throws Exception {
      Set<BoundedWindow> windows = new HashSet<>();
      for (WindowedValue<?> value : values) {
        for (BoundedWindow window : value.getWindows()) {
          windows.add(window);
        }
      }
      return windows;
    }

    private Map<W, W> mergeWindows(Set<BoundedWindow> windows) throws Exception {
      if (windowingStrategy.getWindowFn().isNonMerging()) {
        // Return an empty map, indicating that every window is not merged.
        return Collections.emptyMap();
      }

      Map<BoundedWindow, BoundedWindow> windowToMergeResult = new HashMap<>();
      // Collect the windows from all elements (except those which are too late) and
      // make sure they are already in the active window set or are added as NEW windows.
      for (BoundedWindow window : windows) {
        // For backwards compat with pre 1.4 only.
        // We may still have ACTIVE windows with multiple state addresses, representing
        // a window who's state has not yet been eagerly merged.
        // We'll go ahead and merge that state now so that we don't have to worry about
        // this legacy case anywhere else.
        if (activeWindows.isActive(window)) {
          Set<W> stateAddressWindows = activeWindows.readStateAddresses(window);
          if (stateAddressWindows.size() > 1) {
            // This is a legacy window who's state has not been eagerly merged.
            // Do that now.
            ReduceFn<K, InputT, OutputT, W>.OnMergeContext premergeContext =
                contextFactory.forPremerge(window);
            reduceFn.onMerge(premergeContext);
            watermarkHold.onMerge(premergeContext);
            activeWindows.merged(window);
          }
        }

        // Add this window as NEW if it is not currently ACTIVE.
        // If we had already seen this window and closed its trigger, then the
        // window will not be currently ACTIVE. It will then be added as NEW here,
        // and fall into the merging logic as usual.
        activeWindows.ensureWindowExists(window);
      }

      // Merge all of the active windows and retain a mapping from source windows to result windows.
      activeWindows.merge(new OnMergeCallback(windowToMergeResult));
      return windowToMergeResult;
    }
    @Override
    public void processElement(WindowedValue<KeyedWorkItem<K, KV<K, InputT>>> gbkResult)
        throws Exception {
      Set<BoundedWindow> windows = collectWindows(gbkResult.getValue().elementsIterable());

      for (WindowedValue<KV<K, InputT>> windowedValue : gbkResult.getValue().elementsIterable()) {
        delegateEvaluator.processElement(windowedValue);
      }

      for (TimerData timer : gbkResult.getValue().timersIterable()) {
        checkState(
            timer.getNamespace() instanceof WindowNamespace,
            "Expected Timer %s to be in a %s, but got %s",
            timer,
            WindowNamespace.class.getSimpleName(),
            timer.getNamespace().getClass().getName());
        WindowNamespace<?> windowNamespace = (WindowNamespace) timer.getNamespace();
        BoundedWindow timerWindow = windowNamespace.getWindow();
        delegateEvaluator.onTimer(timer, timerWindow);
      }
    }

    @Override
    public TransformResult<KeyedWorkItem<K, KV<K, InputT>>> finishBundle() throws Exception {
      TransformResult<KV<K, InputT>> delegateResult = delegateEvaluator.finishBundle();

      StepTransformResult.Builder<KeyedWorkItem<K, KV<K, InputT>>> regroupedResult =
          StepTransformResult.<KeyedWorkItem<K, KV<K, InputT>>>withHold(
                  delegateResult.getTransform(), delegateResult.getWatermarkHold())
              .withTimerUpdate(delegateResult.getTimerUpdate())
              .withState(delegateResult.getState())
              .withMetricUpdates(delegateResult.getLogicalMetricUpdates())
              .addOutput(Lists.newArrayList(delegateResult.getOutputBundles()));

      // The delegate may have pushed back unprocessed elements across multiple keys and windows.
      // Since processing is single-threaded per key and window, we don't need to regroup the
      // outputs, but just make a bunch of singletons
      for (WindowedValue<?> untypedUnprocessed : delegateResult.getUnprocessedElements()) {
        WindowedValue<KV<K, InputT>> windowedKv = (WindowedValue<KV<K, InputT>>) untypedUnprocessed;
        WindowedValue<KeyedWorkItem<K, KV<K, InputT>>> pushedBack =
            windowedKv.withValue(
                KeyedWorkItems.elementsWorkItem(
                    windowedKv.getValue().getKey(), Collections.singleton(windowedKv)));

        regroupedResult.addUnprocessedElements(pushedBack);
      }

      return regroupedResult.build();
    }
  }
}
