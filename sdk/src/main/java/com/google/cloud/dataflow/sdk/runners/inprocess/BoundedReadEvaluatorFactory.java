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

import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.io.Read.Bounded;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Bounded Read.Bounded} primitive {@link PTransform}.
 */
final class BoundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  /*
   * An evaluator for a Source is stateful, to ensure data is not read multiple times.
   * Evaluators are cached here to ensure that the reader is not restarted if the evaluator is
   * retriggered.
   */
  private final ConcurrentMap<EvaluatorKey, Queue<? extends BoundedReadEvaluator<?>>>
      sourceEvaluators = new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws IOException {
    return getTransformEvaluator((AppliedPTransform) application, evaluationContext);
  }

  private <OutputT> TransformEvaluator<?> getTransformEvaluator(
      final AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    BoundedReadEvaluator<?> evaluator =
        getTransformEvaluatorQueue(transform, evaluationContext).poll();
    if (evaluator == null) {
      return EmptyTransformEvaluator.create(transform);
    }
    return evaluator;
  }

  /**
   * Get the queue of {@link TransformEvaluator TransformEvaluators} that produce elements for the
   * provided application of {@link Bounded Read.Bounded}, initializing it if required.
   *
   * <p>This method is thread-safe, and will only produce new evaluators if no other invocation has
   * already done so.
   */
  @SuppressWarnings("unchecked")
  private <OutputT> Queue<BoundedReadEvaluator<OutputT>> getTransformEvaluatorQueue(
      final AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    // Key by the application and the context the evaluation is occurring in (which call to
    // Pipeline#run).
    EvaluatorKey key = new EvaluatorKey(transform, evaluationContext);
    Queue<BoundedReadEvaluator<OutputT>> evaluatorQueue =
        (Queue<BoundedReadEvaluator<OutputT>>) sourceEvaluators.get(key);
    if (evaluatorQueue == null) {
      evaluatorQueue = new ConcurrentLinkedQueue<>();
      if (sourceEvaluators.putIfAbsent(key, evaluatorQueue) == null) {
        // If no queue existed in the evaluators, add an evaluator to initialize the evaluator
        // factory for this transform
        BoundedSource<OutputT> source = transform.getTransform().getSource();
        BoundedReadEvaluator<OutputT> evaluator =
            new BoundedReadEvaluator<OutputT>(transform, evaluationContext, source);
        evaluatorQueue.offer(evaluator);
      } else {
        // otherwise return the existing Queue that arrived before us
        evaluatorQueue = (Queue<BoundedReadEvaluator<OutputT>>) sourceEvaluators.get(key);
      }
    }
    return evaluatorQueue;
  }

  /**
   * A {@link BoundedReadEvaluator} produces elements from an underlying {@link BoundedSource},
   * discarding all input elements. Within the call to {@link #finishBundle()}, the evaluator
   * creates the {@link BoundedReader} and consumes all available input.
   *
   * <p>A {@link BoundedReadEvaluator} should only be created once per {@link BoundedSource}, and
   * each evaluator should only be called once per evaluation of the pipeline. Otherwise, the source
   * may produce duplicate elements.
   */
  private static class BoundedReadEvaluator<OutputT> implements TransformEvaluator<Object> {
    private final AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform;
    private final InProcessEvaluationContext evaluationContext;
    /**
     * The source being read from by this {@link BoundedReadEvaluator}. This may not be the same
     * as the source derived from {@link #transform} due to splitting.
     */
    private BoundedSource<OutputT> source;

    public BoundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, Bounded<OutputT>> transform,
        InProcessEvaluationContext evaluationContext,
        BoundedSource<OutputT> source) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.source = source;
    }

    @Override
    public void processElement(WindowedValue<Object> element) {}

    @Override
    public InProcessTransformResult finishBundle() throws IOException {
      try (final BoundedReader<OutputT> reader =
              source.createReader(evaluationContext.getPipelineOptions());) {
        boolean contentsRemaining = reader.start();
        UncommittedBundle<OutputT> output =
            evaluationContext.createRootBundle(transform.getOutput());
        while (contentsRemaining) {
          output.add(
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp()));
          contentsRemaining = reader.advance();
        }
        reader.close();
        return StepTransformResult.withHold(transform, BoundedWindow.TIMESTAMP_MAX_VALUE)
            .addOutput(output)
            .build();
      }
    }
  }
}
