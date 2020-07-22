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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.direct.UnboundedReadDeduplicator.NeverDeduplicator;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Unbounded Read.Unbounded} primitive {@link PTransform}.
 */
class UnboundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  // Occasionally close an existing reader and resume from checkpoint, to exercise close-and-resume
  private static final double DEFAULT_READER_REUSE_CHANCE = 0.95;

  private final EvaluationContext evaluationContext;
  private final PipelineOptions options;
  private final double readerReuseChance;

  UnboundedReadEvaluatorFactory(EvaluationContext evaluationContext, PipelineOptions options) {
    this(evaluationContext, options, DEFAULT_READER_REUSE_CHANCE);
  }

  @VisibleForTesting
  UnboundedReadEvaluatorFactory(
      EvaluationContext evaluationContext, PipelineOptions options, double readerReuseChance) {
    this.evaluationContext = evaluationContext;
    this.options = options;
    this.readerReuseChance = readerReuseChance;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public @Nullable <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) {
    return createEvaluator((AppliedPTransform) application);
  }

  private <OutputT> TransformEvaluator<?> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, Read.Unbounded<OutputT>> application) {
    return new UnboundedReadEvaluator<>(application, evaluationContext, options, readerReuseChance);
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
      implements TransformEvaluator<UnboundedSourceShard<OutputT, CheckpointMarkT>> {
    private static final int ARBITRARY_MAX_ELEMENTS = 10;

    private final AppliedPTransform<?, PCollection<OutputT>, ?> transform;
    private final EvaluationContext evaluationContext;
    private final PipelineOptions options;
    private final double readerReuseChance;
    private final StepTransformResult.Builder resultBuilder;

    public UnboundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, ?> transform,
        EvaluationContext evaluationContext,
        PipelineOptions options,
        double readerReuseChance) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      this.options = options;
      this.readerReuseChance = readerReuseChance;
      resultBuilder = StepTransformResult.withoutHold(transform);
    }

    @Override
    @SuppressWarnings("Finally") // Cannot use try-with-resources in order to ensure we don't
    // double-close the reader.
    public void processElement(
        WindowedValue<UnboundedSourceShard<OutputT, CheckpointMarkT>> element) throws IOException {
      UncommittedBundle<OutputT> output =
          evaluationContext.createBundle(
              (PCollection<OutputT>) getOnlyElement(transform.getOutputs().values()));
      UnboundedSourceShard<OutputT, CheckpointMarkT> shard = element.getValue();
      UnboundedReader<OutputT> reader = null;
      try {
        reader = getReader(shard);
        boolean elementAvailable = startReader(reader, shard);

        if (elementAvailable) {
          UnboundedReadDeduplicator deduplicator = shard.getDeduplicator();
          int numElements = 0;
          do {
            if (deduplicator.shouldOutput(reader.getCurrentRecordId())) {
              output.add(
                  WindowedValue.timestampedValueInGlobalWindow(
                      reader.getCurrent(), reader.getCurrentTimestamp()));
            }
            numElements++;
          } while (numElements < ARBITRARY_MAX_ELEMENTS && reader.advance());
          Instant watermark = reader.getWatermark();

          CheckpointMarkT finishedCheckpoint = finishRead(reader, watermark, shard);
          // Sometimes resume from a checkpoint even if it's not required
          if (ThreadLocalRandom.current().nextDouble(1.0) >= readerReuseChance) {
            UnboundedReader<OutputT> toClose = reader;
            // Prevent double-close. UnboundedReader is AutoCloseable, which does not require
            // idempotency of close. Nulling out the reader here prevents trying to re-close it
            // if the call to close throws an IOException.
            reader = null;
            toClose.close();
          }
          UnboundedSourceShard<OutputT, CheckpointMarkT> residual =
              UnboundedSourceShard.of(
                  shard.getSource(), shard.getDeduplicator(), reader, finishedCheckpoint);

          resultBuilder
              .addOutput(output)
              .addUnprocessedElements(
                  Collections.singleton(
                      WindowedValue.timestampedValueInGlobalWindow(residual, watermark)));
        } else {
          Instant watermark = reader.getWatermark();
          if (watermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            // If the reader had no elements available, but the shard is not done, reuse it later
            // Might be better to finalize old checkpoint.
            resultBuilder.addUnprocessedElements(
                Collections.<WindowedValue<?>>singleton(
                    WindowedValue.timestampedValueInGlobalWindow(
                        UnboundedSourceShard.of(
                            shard.getSource(),
                            shard.getDeduplicator(),
                            reader,
                            shard.getCheckpoint()),
                        watermark)));
          } else {
            // End of input. Close the reader after finalizing old checkpoint.
            // note: can be null for empty datasets so ensure to null check the checkpoint
            final CheckpointMarkT checkpoint = shard.getCheckpoint();
            IOException ioe = null;
            try {
              if (checkpoint != null) {
                checkpoint.finalizeCheckpoint();
              }
            } catch (final IOException finalizeCheckpointException) {
              ioe = finalizeCheckpointException;
            } finally {
              try {
                UnboundedReader<?> toClose = reader;
                reader = null; // Avoid double close below in case of an exception.
                toClose.close();
              } catch (final IOException closeEx) {
                if (ioe != null) {
                  ioe.addSuppressed(closeEx);
                } else {
                  throw closeEx;
                }
              }
            }
            if (ioe != null) {
              throw ioe;
            }
          }
        }
      } catch (IOException e) {
        if (reader != null) {
          reader.close();
        }
        throw e;
      }
    }

    private UnboundedReader<OutputT> getReader(UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      UnboundedReader<OutputT> existing = shard.getExistingReader();
      if (existing == null) {
        CheckpointMarkT checkpoint = shard.getCheckpoint();
        if (checkpoint != null) {
          checkpoint = CoderUtils.clone(shard.getSource().getCheckpointMarkCoder(), checkpoint);
        }
        return shard.getSource().createReader(options, checkpoint);
      } else {
        return existing;
      }
    }

    private boolean startReader(
        UnboundedReader<OutputT> reader, UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      if (shard.getExistingReader() == null) {
        return reader.start();
      } else {
        return shard.getExistingReader().advance();
      }
    }

    /**
     * Checkpoint the current reader, finalize the previous checkpoint, and return the current
     * checkpoint.
     */
    private CheckpointMarkT finishRead(
        UnboundedReader<OutputT> reader,
        Instant watermark,
        UnboundedSourceShard<OutputT, CheckpointMarkT> shard)
        throws IOException {
      final CheckpointMark oldMark = shard.getCheckpoint();
      @SuppressWarnings("unchecked")
      final CheckpointMarkT mark = (CheckpointMarkT) reader.getCheckpointMark();
      if (oldMark != null) {
        oldMark.finalizeCheckpoint();
      }

      // If the watermark is the max value, this source may not be invoked again. Finalize after
      // committing the output.
      if (!watermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        PCollection<OutputT> outputPc =
            (PCollection<OutputT>) getOnlyElement(transform.getOutputs().values());
        evaluationContext.scheduleAfterOutputWouldBeProduced(
            outputPc,
            GlobalWindow.INSTANCE,
            outputPc.getWindowingStrategy(),
            () -> {
              try {
                mark.finalizeCheckpoint();
              } catch (IOException e) {
                throw new RuntimeException(
                    "Couldn't finalize checkpoint after the end of the Global Window", e);
              }
            });
      }
      return mark;
    }

    @Override
    public TransformResult<UnboundedSourceShard<OutputT, CheckpointMarkT>> finishBundle()
        throws IOException {
      return resultBuilder.build();
    }
  }

  @AutoValue
  abstract static class UnboundedSourceShard<T, CheckpointT extends CheckpointMark>
      implements SourceShard<T> {
    static <T, CheckpointT extends CheckpointMark> UnboundedSourceShard<T, CheckpointT> unstarted(
        UnboundedSource<T, CheckpointT> source, UnboundedReadDeduplicator deduplicator) {
      return of(source, deduplicator, null, null);
    }

    static <T, CheckpointT extends CheckpointMark> UnboundedSourceShard<T, CheckpointT> of(
        UnboundedSource<T, CheckpointT> source,
        UnboundedReadDeduplicator deduplicator,
        @Nullable UnboundedReader<T> reader,
        @Nullable CheckpointT checkpoint) {
      return new AutoValue_UnboundedReadEvaluatorFactory_UnboundedSourceShard<>(
          source, deduplicator, reader, checkpoint);
    }

    @Override
    public abstract UnboundedSource<T, CheckpointT> getSource();

    abstract UnboundedReadDeduplicator getDeduplicator();

    abstract @Nullable UnboundedReader<T> getExistingReader();

    abstract @Nullable CheckpointT getCheckpoint();
  }

  static class InputProvider<T>
      implements RootInputProvider<T, UnboundedSourceShard<T, ?>, PBegin> {
    private final EvaluationContext evaluationContext;
    private final PipelineOptions options;

    InputProvider(EvaluationContext evaluationContext, PipelineOptions options) {
      this.evaluationContext = evaluationContext;
      this.options = options;
    }

    @Override
    public Collection<CommittedBundle<UnboundedSourceShard<T, ?>>> getInitialInputs(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform,
        int targetParallelism)
        throws Exception {
      UnboundedSource<T, ?> source = ReadTranslation.unboundedSourceFromTransform(transform);
      List<? extends UnboundedSource<T, ?>> splits = source.split(targetParallelism, options);
      UnboundedReadDeduplicator deduplicator =
          source.requiresDeduping()
              ? UnboundedReadDeduplicator.CachedIdDeduplicator.create()
              : NeverDeduplicator.create();

      ImmutableList.Builder<CommittedBundle<UnboundedSourceShard<T, ?>>> initialShards =
          ImmutableList.builder();
      for (UnboundedSource<T, ?> split : splits) {
        UnboundedSourceShard<T, ?> shard = UnboundedSourceShard.unstarted(split, deduplicator);
        initialShards.add(
            evaluationContext
                .<UnboundedSourceShard<T, ?>>createRootBundle()
                .add(WindowedValue.valueInGlobalWindow(shard))
                .commit(BoundedWindow.TIMESTAMP_MAX_VALUE));
      }
      return initialShards.build();
    }
  }
}
