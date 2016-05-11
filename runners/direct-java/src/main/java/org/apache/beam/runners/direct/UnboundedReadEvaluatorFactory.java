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

import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
class UnboundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  /*
   * An evaluator for a Source is stateful, to ensure the CheckpointMark is properly persisted.
   * Evaluators are cached here to ensure that the checkpoint mark is appropriately reused
   * and any splits are honored.
   *
   * <p>The Queue storing available evaluators must enforce a happens-before relationship for
   * elements being added to the queue to accesses after it, to ensure that updates performed to the
   * state of an evaluator are properly visible. ConcurrentLinkedQueue provides this relation, but
   * an arbitrary Queue implementation does not, so the concrete type is used explicitly.
   */
  private final ConcurrentMap<
      EvaluatorKey, ConcurrentLinkedQueue<? extends UnboundedReadEvaluator<?>>>
      sourceEvaluators = new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  @Nullable
  public <InputT> TransformEvaluator<InputT> forApplication(AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle, InProcessEvaluationContext evaluationContext) {
    return getTransformEvaluator((AppliedPTransform) application, evaluationContext);
  }

  private <OutputT> TransformEvaluator<?> getTransformEvaluator(
      final AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    return getTransformEvaluatorQueue(transform, evaluationContext).poll();
  }

  /**
   * Get the queue of {@link TransformEvaluator TransformEvaluators} that produce elements for the
   * provided application of {@link Unbounded Read.Unbounded}, initializing it if required.
   *
   * <p>This method is thread-safe, and will only produce new evaluators if no other invocation has
   * already done so.
   */
  @SuppressWarnings("unchecked")
  private <OutputT> Queue<UnboundedReadEvaluator<OutputT>> getTransformEvaluatorQueue(
      final AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform,
      final InProcessEvaluationContext evaluationContext) {
    // Key by the application and the context the evaluation is occurring in (which call to
    // Pipeline#run).
    EvaluatorKey key = new EvaluatorKey(transform, evaluationContext);
    @SuppressWarnings("unchecked")
    ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT>> evaluatorQueue =
        (ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT>>) sourceEvaluators.get(key);
    if (evaluatorQueue == null) {
      evaluatorQueue = new ConcurrentLinkedQueue<>();
      if (sourceEvaluators.putIfAbsent(key, evaluatorQueue) == null) {
        // If no queue existed in the evaluators, add an evaluator to initialize the evaluator
        // factory for this transform
        UnboundedSource<OutputT, ?> source = transform.getTransform().getSource();
        UnboundedReadEvaluator<OutputT> evaluator =
            new UnboundedReadEvaluator<OutputT>(
                transform, evaluationContext, source, evaluatorQueue);
        evaluatorQueue.offer(evaluator);
      } else {
        // otherwise return the existing Queue that arrived before us
        evaluatorQueue =
            (ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT>>) sourceEvaluators.get(key);
      }
    }
    return evaluatorQueue;
  }

  /**
   * A {@link UnboundedReadEvaluator} produces elements from an underlying {@link UnboundedSource},
   * discarding all input elements. Within the call to {@link #finishBundle()}, the evaluator
   * creates the {@link UnboundedReader} and consumes some currently available input.
   *
   * <p>Calls to {@link UnboundedReadEvaluator} are not internally thread-safe, and should only be
   * used by a single thread at a time. Each {@link UnboundedReadEvaluator} maintains its own
   * checkpoint, and constructs its reader from the current checkpoint in each call to
   * {@link #finishBundle()}.
   */
  private static class UnboundedReadEvaluator<OutputT> implements TransformEvaluator<Object> {
    private static final int ARBITRARY_MAX_ELEMENTS = 10;
    private final AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform;
    private final InProcessEvaluationContext evaluationContext;
    private final ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT>> evaluatorQueue;
    /**
     * The source being read from by this {@link UnboundedReadEvaluator}. This may not be the same
     * source as derived from {@link #transform} due to splitting.
     */
    private final UnboundedSource<OutputT, ?> source;
    private CheckpointMark checkpointMark;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, Unbounded<OutputT>> transform,
        InProcessEvaluationContext evaluationContext,
        UnboundedSource<OutputT, ?> source,
        ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT>> evaluatorQueue) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.evaluatorQueue = evaluatorQueue;
      this.source = source;
      this.checkpointMark = null;
    }

    @Override
    public void processElement(WindowedValue<Object> element) {}

    @Override
    public InProcessTransformResult finishBundle() throws IOException {
      UncommittedBundle<OutputT> output = evaluationContext.createRootBundle(transform.getOutput());
      try (UnboundedReader<OutputT> reader =
              createReader(source, evaluationContext.getPipelineOptions());) {
        int numElements = 0;
        if (reader.start()) {
          do {
            output.add(
                WindowedValue.timestampedValueInGlobalWindow(
                    reader.getCurrent(), reader.getCurrentTimestamp()));
            numElements++;
          } while (numElements < ARBITRARY_MAX_ELEMENTS && reader.advance());
        }
        checkpointMark = reader.getCheckpointMark();
        checkpointMark.finalizeCheckpoint();
        // TODO: When exercising create initial splits, make this the minimum watermark across all
        // existing readers
        StepTransformResult result =
            StepTransformResult.withHold(transform, reader.getWatermark())
                .addOutput(output)
                .build();
        evaluatorQueue.offer(this);
        return result;
      }
    }

    private <CheckpointMarkT extends CheckpointMark> UnboundedReader<OutputT> createReader(
        UnboundedSource<OutputT, CheckpointMarkT> source, PipelineOptions options) {
      @SuppressWarnings("unchecked")
      CheckpointMarkT mark = (CheckpointMarkT) checkpointMark;
      return source.createReader(options, mark);
    }
  }
}
