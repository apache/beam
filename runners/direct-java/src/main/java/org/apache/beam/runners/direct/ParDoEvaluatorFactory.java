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

import com.google.auto.value.AutoValue;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.beam.runners.direct.DirectExecutionContext.DirectStepContext;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.state.StateNamespace;
import org.apache.beam.sdk.util.state.StateNamespaces;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.util.state.StateTag;
import org.apache.beam.sdk.util.state.StateTags;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} for {@link ParDo}-like primitive {@link PTransform
 * PTransforms}, parameterized by some {@link TransformHooks transform-specific handling}.
 */
final class ParDoEvaluatorFactory<
        InputT,
        OutputT,
        TransformOutputT extends POutput,
        TransformT extends PTransform<PCollection<? extends InputT>, TransformOutputT>>
    implements TransformEvaluatorFactory {
  interface TransformHooks<
      InputT,
      OutputT,
      TransformOutputT extends POutput,
      TransformT extends PTransform<PCollection<? extends InputT>, TransformOutputT>> {
    /** Returns the {@link DoFn} contained in the given {@link ParDo} transform. */
    DoFn<InputT, OutputT> getDoFn(TransformT transform);

    /** Configures and creates a {@link ParDoEvaluator} for the given {@link DoFn}. */
    ParDoEvaluator<InputT, OutputT> createParDoEvaluator(
        EvaluationContext evaluationContext,
        AppliedPTransform<PCollection<InputT>, TransformOutputT, TransformT> application,
        DirectStepContext stepContext,
        DoFn<InputT, OutputT> fnLocal);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParDoEvaluatorFactory.class);
  private final LoadingCache<DoFn<?, ?>, DoFnLifecycleManager> fnClones;
  private final EvaluationContext evaluationContext;
  private final TransformHooks<InputT, OutputT, TransformOutputT, TransformT> hooks;
  private final LoadingCache<AppliedPTransformOutputKeyAndWindow, Runnable> cleanupRegistry;

  ParDoEvaluatorFactory(
      EvaluationContext evaluationContext,
      TransformHooks<InputT, OutputT, TransformOutputT, TransformT> hooks) {
    this.evaluationContext = evaluationContext;
    this.hooks = hooks;
    this.cleanupRegistry =
        CacheBuilder.newBuilder()
            .weakValues()
            .build(new CleanupSchedulingLoader(evaluationContext));
    fnClones =
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<DoFn<?, ?>, DoFnLifecycleManager>() {
                  @Override
                  public DoFnLifecycleManager load(DoFn<?, ?> key) throws Exception {
                    return DoFnLifecycleManager.of(key);
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
    DoFnLifecycleManagers.removeAllFromManagers(fnClones.asMap().values());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private TransformEvaluator<InputT> createEvaluator(
      AppliedPTransform<PCollection<InputT>, TransformOutputT, TransformT> application,
      CommittedBundle<InputT> inputBundle)
      throws Exception {
    String stepName = evaluationContext.getStepName(application);
    final DirectStepContext stepContext =
        evaluationContext
            .getExecutionContext(application, inputBundle.getKey())
            .getOrCreateStepContext(stepName, stepName);

    final DoFn<?, ?> doFn = hooks.getDoFn(application.getTransform());
    DoFnLifecycleManager fnManager = fnClones.getUnchecked(doFn);
    final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

    // If the DoFn is stateful, schedule state clearing.
    // It is semantically correct to schedule any number of redundant clear tasks; the
    // cache is used to limit the number of tasks to avoid performance degradation.
    if (signature.stateDeclarations().size() > 0) {
      for (final WindowedValue<?> element : inputBundle.getElements()) {
        for (final BoundedWindow window : element.getWindows()) {
          cleanupRegistry.get(
              AppliedPTransformOutputKeyAndWindow.create(
                  application, inputBundle.getKey(), window));
        }
      }
    }

    try {
      return DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(
          hooks.createParDoEvaluator(
              evaluationContext, application, stepContext, (DoFn<InputT, OutputT>) fnManager.get()),
          fnManager);
    } catch (Exception e) {
      try {
        fnManager.remove();
      } catch (Exception removalException) {
        LOG.error(
            "Exception encountered while cleaning up in ParDo evaluator construction",
            removalException);
        e.addSuppressed(removalException);
      }
      throw e;
    }
  }

  private class CleanupSchedulingLoader
      extends CacheLoader<AppliedPTransformOutputKeyAndWindow, Runnable> {

    private final EvaluationContext evaluationContext;

    public CleanupSchedulingLoader(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Runnable load(final AppliedPTransformOutputKeyAndWindow transformOutputWindow) {
      String stepName = evaluationContext.getStepName(transformOutputWindow.getTransform());
      PCollection<?> pc = (PCollection<?>) transformOutputWindow.getTransform().getOutput();
      WindowingStrategy<?, ?> windowingStrategy = pc.getWindowingStrategy();
      BoundedWindow window = transformOutputWindow.getWindow();
      final DoFn<?, ?> doFn =
          hooks.getDoFn((TransformT) transformOutputWindow.getTransform().getTransform());
      final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());

      final DirectStepContext stepContext =
          evaluationContext
              .getExecutionContext(
                  transformOutputWindow.getTransform(), transformOutputWindow.getKey())
              .getOrCreateStepContext(stepName, stepName);

      final StateNamespace namespace =
          StateNamespaces.window(
              (Coder<BoundedWindow>) windowingStrategy.getWindowFn().windowCoder(), window);

      Runnable cleanup =
          new Runnable() {
            @Override
            public void run() {
              for (StateDeclaration stateDecl : signature.stateDeclarations().values()) {
                StateTag<Object, ?> tag;
                try {
                  tag =
                      StateTags.tagForSpec(stateDecl.id(), (StateSpec) stateDecl.field().get(doFn));
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
            }
          };

      evaluationContext.scheduleAfterWindowExpiration(pc, window, windowingStrategy, cleanup);
      return cleanup;
    }
  }

  @AutoValue
  abstract static class AppliedPTransformOutputKeyAndWindow {
    abstract AppliedPTransform<?, ?, ?> getTransform();

    abstract StructuralKey<?> getKey();

    abstract BoundedWindow getWindow();

    static AppliedPTransformOutputKeyAndWindow create(
        AppliedPTransform<?, ?, ?> transform, StructuralKey<?> key, BoundedWindow w) {
      return new AutoValue_ParDoEvaluatorFactory_AppliedPTransformOutputKeyAndWindow(
          transform, key, w);
    }
  }
}
