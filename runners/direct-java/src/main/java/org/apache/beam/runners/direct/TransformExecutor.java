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

import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.util.WindowedValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * A {@link Callable} responsible for constructing a {@link TransformEvaluator} from a
 * {@link TransformEvaluatorFactory} and evaluating it on some bundle of input, and registering
 * the result using a registered {@link CompletionCallback}.
 *
 * <p>A {@link TransformExecutor} that is currently executing also provides access to the thread
 * that it is being executed on.
 */
class TransformExecutor<T> implements Runnable {
  public static <T> TransformExecutor<T> create(
      TransformEvaluatorFactory factory,
      Iterable<? extends ModelEnforcementFactory> modelEnforcements,
      EvaluationContext evaluationContext,
      CommittedBundle<T> inputBundle,
      AppliedPTransform<?, ?, ?> transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    return new TransformExecutor<>(
        factory,
        modelEnforcements,
        evaluationContext,
        inputBundle,
        transform,
        completionCallback,
        transformEvaluationState);
  }

  private final TransformEvaluatorFactory evaluatorFactory;
  private final Iterable<? extends ModelEnforcementFactory> modelEnforcements;

  private final EvaluationContext evaluationContext;

  /** The transform that will be evaluated. */
  private final AppliedPTransform<?, ?, ?> transform;
  /** The inputs this {@link TransformExecutor} will deliver to the transform. */
  private final CommittedBundle<T> inputBundle;

  private final CompletionCallback onComplete;
  private final TransformExecutorService transformEvaluationState;

  private final AtomicReference<Thread> thread;

  private TransformExecutor(
      TransformEvaluatorFactory factory,
      Iterable<? extends ModelEnforcementFactory> modelEnforcements,
      EvaluationContext evaluationContext,
      CommittedBundle<T> inputBundle,
      AppliedPTransform<?, ?, ?> transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    this.evaluatorFactory = factory;
    this.modelEnforcements = modelEnforcements;
    this.evaluationContext = evaluationContext;

    this.inputBundle = inputBundle;
    this.transform = transform;

    this.onComplete = completionCallback;

    this.transformEvaluationState = transformEvaluationState;
    this.thread = new AtomicReference<>();
  }

  @Override
  public void run() {
    checkState(
        thread.compareAndSet(null, Thread.currentThread()),
        "Tried to execute %s for %s on thread %s, but is already executing on thread %s",
        TransformExecutor.class.getSimpleName(),
        transform.getFullName(),
        Thread.currentThread(),
        thread.get());
    try {
      Collection<ModelEnforcement<T>> enforcements = new ArrayList<>();
      for (ModelEnforcementFactory enforcementFactory : modelEnforcements) {
        ModelEnforcement<T> enforcement = enforcementFactory.forBundle(inputBundle, transform);
        enforcements.add(enforcement);
      }
      TransformEvaluator<T> evaluator =
          evaluatorFactory.forApplication(transform, inputBundle, evaluationContext);
      if (evaluator == null) {
        onComplete.handleEmpty(transform);
        // Nothing to do
        return;
      }

      processElements(evaluator, enforcements);

      TransformResult result = finishBundle(evaluator, enforcements);
    } catch (Throwable t) {
      onComplete.handleThrowable(inputBundle, t);
      if (t instanceof RuntimeException) {
        throw (RuntimeException) t;
      }
      throw new RuntimeException(t);
    } finally {
      transformEvaluationState.complete(this);
    }
  }

  /**
   * Processes all the elements in the input bundle using the transform evaluator, applying any
   * necessary {@link ModelEnforcement ModelEnforcements}.
   */
  private void processElements(
      TransformEvaluator<T> evaluator, Collection<ModelEnforcement<T>> enforcements)
      throws Exception {
    if (inputBundle != null) {
      for (WindowedValue<T> value : inputBundle.getElements()) {
        for (ModelEnforcement<T> enforcement : enforcements) {
          enforcement.beforeElement(value);
        }

        evaluator.processElement(value);

        for (ModelEnforcement<T> enforcement : enforcements) {
          enforcement.afterElement(value);
        }
      }
    }
  }

  /**
   * Finishes processing the input bundle and commit the result using the
   * {@link CompletionCallback}, applying any {@link ModelEnforcement} if necessary.
   *
   * @return the {@link TransformResult} produced by
   *         {@link TransformEvaluator#finishBundle()}
   */
  private TransformResult finishBundle(
      TransformEvaluator<T> evaluator, Collection<ModelEnforcement<T>> enforcements)
      throws Exception {
    TransformResult result = evaluator.finishBundle();
    CommittedResult outputs = onComplete.handleResult(inputBundle, result);
    for (ModelEnforcement<T> enforcement : enforcements) {
      enforcement.afterFinish(inputBundle, result, outputs.getOutputs());
    }
    return result;
  }

  /**
   * If this {@link TransformExecutor} is currently executing, return the thread it is executing in.
   * Otherwise, return null.
   */
  @Nullable
  public Thread getThread() {
    return thread.get();
  }
}
