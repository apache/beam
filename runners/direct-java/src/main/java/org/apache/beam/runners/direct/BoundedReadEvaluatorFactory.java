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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Bounded Read.Bounded} primitive {@link PTransform}.
 */
final class BoundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  /**
   * The required minimum size of a source to dynamically split. Produced {@link TransformEvaluator
   * TransformEvaluators} will attempt to dynamically split all sources larger than the minimum
   * dynamic split size.
   */
  private static final long REQUIRED_DYNAMIC_SPLIT_ORIGINAL_SIZE = 0;
  private final EvaluationContext evaluationContext;
  @VisibleForTesting final ExecutorService executor = Executors.newCachedThreadPool();

  private final long minimumDynamicSplitSize;

  BoundedReadEvaluatorFactory(EvaluationContext evaluationContext) {
    this(evaluationContext, REQUIRED_DYNAMIC_SPLIT_ORIGINAL_SIZE);
  }

  @VisibleForTesting
  BoundedReadEvaluatorFactory(EvaluationContext evaluationContext, long minimumDynamicSplitSize) {
    this.evaluationContext = evaluationContext;
    this.minimumDynamicSplitSize = minimumDynamicSplitSize;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  @Nullable
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws IOException {
    return createEvaluator((AppliedPTransform) application);
  }

  private <OutputT> TransformEvaluator<?> createEvaluator(
      final AppliedPTransform<?, PCollection<OutputT>, ?> transform) {
    return new BoundedReadEvaluator<>(
        transform, evaluationContext, minimumDynamicSplitSize, executor);
  }

  @Override
  public void cleanup() {
    executor.shutdown();
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
  private static class BoundedReadEvaluator<OutputT>
      implements TransformEvaluator<BoundedSourceShard<OutputT>> {
    private final PCollection<OutputT> outputPCollection;
    private final EvaluationContext evaluationContext;
    private Builder resultBuilder;

    private final long minimumDynamicSplitSize;
    private final ExecutorService produceSplitExecutor;

    public BoundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, ?> transform,
        EvaluationContext evaluationContext,
        long minimumDynamicSplitSize,
        ExecutorService executor) {
      this.evaluationContext = evaluationContext;
      this.outputPCollection =
          (PCollection<OutputT>) Iterables.getOnlyElement(transform.getOutputs().values());
      this.resultBuilder = StepTransformResult.withoutHold(transform);
      this.minimumDynamicSplitSize = minimumDynamicSplitSize;
      this.produceSplitExecutor = executor;
    }

    @Override
    public void processElement(WindowedValue<BoundedSourceShard<OutputT>> element)
        throws Exception {
      BoundedSource<OutputT> source = element.getValue().getSource();
      try (final BoundedReader<OutputT> reader =
          source.createReader(evaluationContext.getPipelineOptions())) {
        boolean contentsRemaining = reader.start();
        Future<BoundedSource<OutputT>> residualFuture = startDynamicSplitThread(source, reader);
        UncommittedBundle<OutputT> output = evaluationContext.createBundle(outputPCollection);
        while (contentsRemaining) {
          output.add(
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp()));
          contentsRemaining = reader.advance();
        }
        resultBuilder.addOutput(output);
        try {
          BoundedSource<OutputT> residual = residualFuture.get();
          if (residual != null) {
            resultBuilder.addUnprocessedElements(
                element.withValue(BoundedSourceShard.of(residual)));
          }
        } catch (ExecutionException exex) {
          // Un-and-rewrap the exception thrown by attempting to split
          throw UserCodeException.wrap(exex.getCause());
        }
      }
    }

    private Future<BoundedSource<OutputT>> startDynamicSplitThread(
        BoundedSource<OutputT> source, BoundedReader<OutputT> reader) throws Exception {
      if (source.getEstimatedSizeBytes(evaluationContext.getPipelineOptions())
          > minimumDynamicSplitSize) {
        return produceSplitExecutor.submit(new GenerateSplitAtHalfwayPoint<>(reader));
      } else {
        SettableFuture<BoundedSource<OutputT>> emptyFuture = SettableFuture.create();
        emptyFuture.set(null);
        return emptyFuture;
      }
    }

    @Override
    public TransformResult<BoundedSourceShard<OutputT>> finishBundle()  {
      return resultBuilder.build();
    }
  }

  @AutoValue
  abstract static class BoundedSourceShard<T> {
    static <T> BoundedSourceShard<T> of(BoundedSource<T> source) {
      return new AutoValue_BoundedReadEvaluatorFactory_BoundedSourceShard<>(source);
    }

    abstract BoundedSource<T> getSource();
  }

  static class InputProvider<T>
      implements RootInputProvider<T, BoundedSourceShard<T>, PBegin, Read.Bounded<T>> {
    private final EvaluationContext evaluationContext;

    InputProvider(EvaluationContext evaluationContext) {
      this.evaluationContext = evaluationContext;
    }

    @Override
    public Collection<CommittedBundle<BoundedSourceShard<T>>> getInitialInputs(
        AppliedPTransform<PBegin, PCollection<T>, Read.Bounded<T>> transform, int targetParallelism)
        throws Exception {
      BoundedSource<T> source = transform.getTransform().getSource();
      PipelineOptions options = evaluationContext.getPipelineOptions();
      long estimatedBytes = source.getEstimatedSizeBytes(options);
      long bytesPerBundle = estimatedBytes / targetParallelism;
      List<? extends BoundedSource<T>> bundles =
          source.split(bytesPerBundle, options);
      ImmutableList.Builder<CommittedBundle<BoundedSourceShard<T>>> shards =
          ImmutableList.builder();
      for (BoundedSource<T> bundle : bundles) {
        CommittedBundle<BoundedSourceShard<T>> inputShard =
            evaluationContext
                .<BoundedSourceShard<T>>createRootBundle()
                .add(WindowedValue.valueInGlobalWindow(BoundedSourceShard.of(bundle)))
                .commit(BoundedWindow.TIMESTAMP_MAX_VALUE);
        shards.add(inputShard);
      }
      return shards.build();
    }
  }

  private static class GenerateSplitAtHalfwayPoint<T> implements Callable<BoundedSource<T>> {
    private final BoundedReader<T> reader;

    private GenerateSplitAtHalfwayPoint(BoundedReader<T> reader) {
      this.reader = reader;
    }

    @Override
    public BoundedSource<T> call() throws Exception {
      // Splits at halfway of the remaining work.
      Double currentlyConsumed = reader.getFractionConsumed();
      if (currentlyConsumed == null || currentlyConsumed == 1.0) {
        return null;
      }
      double halfwayBetweenCurrentAndCompletion = 0.5 + (currentlyConsumed / 2);
      return reader.splitAtFraction(halfwayBetweenCurrentAndCompletion);
    }
  }
}
