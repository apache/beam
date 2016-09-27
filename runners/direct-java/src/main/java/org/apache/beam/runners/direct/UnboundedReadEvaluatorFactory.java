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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
class UnboundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  // Resume from a checkpoint every nth invocation, to ensure close-and-resume is exercised
  @VisibleForTesting static final int MAX_READER_REUSE_COUNT = 20;

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
          AppliedPTransform<?, ?, ?>, ConcurrentLinkedQueue<? extends UnboundedReadEvaluator<?, ?>>>
      sourceEvaluators;
  private final EvaluationContext evaluationContext;

  UnboundedReadEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
    sourceEvaluators = new ConcurrentHashMap<>();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  @Nullable
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, @Nullable CommittedBundle<?> inputBundle) {
    return getTransformEvaluator((AppliedPTransform) application);
  }

  /**
   * Get a {@link TransformEvaluator} that produces elements for the provided application of {@link
   * Unbounded Read.Unbounded}, initializing the queue of evaluators if required.
   *
   * <p>This method is thread-safe, and will only produce new evaluators if no other invocation has
   * already done so.
   */
  private <OutputT, CheckpointMarkT extends CheckpointMark>
      TransformEvaluator<?> getTransformEvaluator(
          final AppliedPTransform<?, PCollection<OutputT>, ?> transform) {
    ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT, CheckpointMarkT>> evaluatorQueue =
        (ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT, CheckpointMarkT>>)
            sourceEvaluators.get(transform);
    if (evaluatorQueue == null) {
      evaluatorQueue = new ConcurrentLinkedQueue<>();
      if (sourceEvaluators.putIfAbsent(transform, evaluatorQueue) == null) {
        // If no queue existed in the evaluators, add an evaluator to initialize the evaluator
        // factory for this transform
        Unbounded<OutputT> unbounded = (Unbounded<OutputT>) transform.getTransform();
        UnboundedSource<OutputT, CheckpointMarkT> source =
            (UnboundedSource<OutputT, CheckpointMarkT>) unbounded.getSource();
        UnboundedReadDeduplicator deduplicator;
        if (source.requiresDeduping()) {
          deduplicator = UnboundedReadDeduplicator.CachedIdDeduplicator.create();
        } else {
          deduplicator = UnboundedReadDeduplicator.NeverDeduplicator.create();
        }
        UnboundedReadEvaluator<OutputT, CheckpointMarkT> evaluator =
            new UnboundedReadEvaluator<>(
                transform, evaluationContext, source, deduplicator, evaluatorQueue);
        evaluatorQueue.offer(evaluator);
      } else {
        // otherwise return the existing Queue that arrived before us
        evaluatorQueue =
            (ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT, CheckpointMarkT>>)
                sourceEvaluators.get(transform);
      }
    }
    return evaluatorQueue.poll();
  }

  @Override
  public void cleanup() {}

  /**
   * A {@link UnboundedReadEvaluator} produces elements from an underlying {@link UnboundedSource},
   * discarding all input elements. Within the call to {@link #finishBundle()}, the evaluator
   * creates the {@link UnboundedReader} and consumes some currently available input.
   *
   * <p>Calls to {@link UnboundedReadEvaluator} are not internally thread-safe, and should only be
   * used by a single thread at a time. Each {@link UnboundedReadEvaluator} maintains its own
   * checkpoint, and constructs its reader from the current checkpoint in each call to {@link
   * #finishBundle()}.
   */
  private static class UnboundedReadEvaluator<OutputT, CheckpointMarkT extends CheckpointMark>
      implements TransformEvaluator<Object> {
    private static final int ARBITRARY_MAX_ELEMENTS = 10;

    private final AppliedPTransform<?, PCollection<OutputT>, ?> transform;
    private final EvaluationContext evaluationContext;
    private final ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT, CheckpointMarkT>>
        evaluatorQueue;
    /**
     * The source being read from by this {@link UnboundedReadEvaluator}. This may not be the same
     * source as derived from {@link #transform} due to splitting.
     */
    private final UnboundedSource<OutputT, CheckpointMarkT> source;

    private final UnboundedReadDeduplicator deduplicator;
    private UnboundedReader<OutputT> currentReader;
    private CheckpointMarkT checkpointMark;

    /**
     * The count of bundles output from this {@link UnboundedReadEvaluator}. Used to exercise {@link
     * UnboundedReader#close()}.
     */
    private int outputBundles = 0;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, ?> transform,
        EvaluationContext evaluationContext,
        UnboundedSource<OutputT, CheckpointMarkT> source,
        UnboundedReadDeduplicator deduplicator,
        ConcurrentLinkedQueue<UnboundedReadEvaluator<OutputT, CheckpointMarkT>> evaluatorQueue) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.evaluatorQueue = evaluatorQueue;
      this.source = source;
      this.currentReader = null;
      this.deduplicator = deduplicator;
      this.checkpointMark = null;
    }

    @Override
    public void processElement(WindowedValue<Object> element) {}

    @Override
    public TransformResult finishBundle() throws IOException {
      UncommittedBundle<OutputT> output = evaluationContext.createBundle(transform.getOutput());
      try {
        boolean elementAvailable = startReader();

        Instant watermark = currentReader.getWatermark();
        if (elementAvailable) {
          int numElements = 0;
          do {
            if (deduplicator.shouldOutput(currentReader.getCurrentRecordId())) {
              output.add(
                  WindowedValue.timestampedValueInGlobalWindow(
                      currentReader.getCurrent(), currentReader.getCurrentTimestamp()));
            }
            numElements++;
          } while (numElements < ARBITRARY_MAX_ELEMENTS && currentReader.advance());
          watermark = currentReader.getWatermark();
          // Only take a checkpoint if we did any work
          finishRead();
        }
        // TODO: When exercising create initial splits, make this the minimum watermark across all
        // existing readers
        StepTransformResult result =
            StepTransformResult.withHold(transform, watermark).addOutput(output).build();
        evaluatorQueue.offer(this);
        return result;
      } catch (IOException e) {
        closeReader();
        throw e;
      }
    }

    private boolean startReader() throws IOException {
      if (currentReader == null) {
        if (checkpointMark != null) {
          checkpointMark.finalizeCheckpoint();
        }
        currentReader = source.createReader(evaluationContext.getPipelineOptions(), checkpointMark);
        checkpointMark = null;
        return currentReader.start();
      } else {
        return currentReader.advance();
      }
    }

    /**
     * Checkpoint the current reader, finalize the previous checkpoint, and update the state of this
     * evaluator.
     */
    private void finishRead() throws IOException {
      final CheckpointMark oldMark = checkpointMark;
      @SuppressWarnings("unchecked")
      final CheckpointMarkT mark = (CheckpointMarkT) currentReader.getCheckpointMark();
      checkpointMark = mark;
      if (oldMark != null) {
        oldMark.finalizeCheckpoint();
      }

      // If the watermark is the max value, this source may not be invoked again. Finalize after
      // committing the output.
      if (!currentReader.getWatermark().isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        evaluationContext.scheduleAfterOutputWouldBeProduced(
            transform.getOutput(),
            GlobalWindow.INSTANCE,
            transform.getOutput().getWindowingStrategy(),
            new Runnable() {
              @Override
              public void run() {
                try {
                  mark.finalizeCheckpoint();
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Couldn't finalize checkpoint after the end of the Global Window", e);
                }
              }
            });
      }
      // Sometimes resume from a checkpoint even if it's not required

      if (outputBundles >= MAX_READER_REUSE_COUNT) {
        closeReader();
        outputBundles = 0;
      } else {
        outputBundles++;
      }
    }

    private void closeReader() throws IOException {
      if (currentReader != null) {
        currentReader.close();
        currentReader = null;
      }
    }
  }
}
