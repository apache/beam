/*
 * Copyright (C) 2016 Google Inc.
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

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.common.base.Throwables;

import java.util.concurrent.Callable;

import javax.annotation.Nullable;

/**
 * A {@link Callable} responsible for constructing a {@link TransformEvaluator} from a
 * {@link TransformEvaluatorFactory} and evaluating it on some bundle of input, and registering
 * the result using a registered {@link CompletionCallback}.
 *
 * <p>A {@link TransformExecutor} that is currently executing also provides access to the thread
 * that it is being executed on.
 */
class TransformExecutor<T> implements Callable<InProcessTransformResult> {
  public static <T> TransformExecutor<T> create(
      TransformEvaluatorFactory factory,
      InProcessEvaluationContext evaluationContext,
      CommittedBundle<T> inputBundle,
      AppliedPTransform<?, ?, ?> transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    return new TransformExecutor<>(
        factory,
        evaluationContext,
        inputBundle,
        transform,
        completionCallback,
        transformEvaluationState);
  }

  private final TransformEvaluatorFactory evaluatorFactory;
  private final InProcessEvaluationContext evaluationContext;

  /** The transform that will be evaluated. */
  private final AppliedPTransform<?, ?, ?> transform;
  /** The inputs this {@link TransformExecutor} will deliver to the transform. */
  private final CommittedBundle<T> inputBundle;

  private final CompletionCallback onComplete;
  private final TransformExecutorService transformEvaluationState;

  private Thread thread;

  private TransformExecutor(
      TransformEvaluatorFactory factory,
      InProcessEvaluationContext evaluationContext,
      CommittedBundle<T> inputBundle,
      AppliedPTransform<?, ?, ?> transform,
      CompletionCallback completionCallback,
      TransformExecutorService transformEvaluationState) {
    this.evaluatorFactory = factory;
    this.evaluationContext = evaluationContext;

    this.inputBundle = inputBundle;
    this.transform = transform;

    this.onComplete = completionCallback;

    this.transformEvaluationState = transformEvaluationState;
  }

  @Override
  public InProcessTransformResult call() {
    this.thread = Thread.currentThread();
    try {
      TransformEvaluator<T> evaluator =
          evaluatorFactory.forApplication(transform, inputBundle, evaluationContext);
      if (inputBundle != null) {
        for (WindowedValue<T> value : inputBundle.getElements()) {
          evaluator.processElement(value);
        }
      }
      InProcessTransformResult result = evaluator.finishBundle();
      onComplete.handleResult(inputBundle, result);
      return result;
    } catch (Throwable t) {
      onComplete.handleThrowable(inputBundle, t);
      throw Throwables.propagate(t);
    } finally {
      this.thread = null;
      transformEvaluationState.complete(this);
    }
  }

  /**
   * If this {@link TransformExecutor} is currently executing, return the thread it is executing in.
   * Otherwise, return null.
   */
  @Nullable
  public Thread getThread() {
    return this.thread;
  }
}
