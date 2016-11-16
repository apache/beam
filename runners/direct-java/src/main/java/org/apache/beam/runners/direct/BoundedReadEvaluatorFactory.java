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
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.runners.direct.StepTransformResult.Builder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Read.Bounded;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Bounded Read.Bounded} primitive {@link PTransform}.
 */
final class BoundedReadEvaluatorFactory implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedReadEvaluatorFactory.class);
  private final EvaluationContext evaluationContext;

  BoundedReadEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
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
    return new BoundedReadEvaluator<>(transform, evaluationContext);
  }

  @Override
  public void cleanup() {}

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
    private final AppliedPTransform<?, PCollection<OutputT>, ?> transform;
    private final EvaluationContext evaluationContext;
    private Builder resultBuilder;

    public BoundedReadEvaluator(
        AppliedPTransform<?, PCollection<OutputT>, ?> transform,
        EvaluationContext evaluationContext) {
      this.transform = transform;
      this.evaluationContext = evaluationContext;
      resultBuilder = StepTransformResult.withoutHold(transform);
    }

    @Override
    public void processElement(WindowedValue<BoundedSourceShard<OutputT>> element)
        throws IOException {
      BoundedSource<OutputT> source = element.getValue().getSource();
      try (final BoundedReader<OutputT> reader =
          source.createReader(evaluationContext.getPipelineOptions())) {
        boolean contentsRemaining = reader.start();
        UncommittedBundle<OutputT> output = evaluationContext.createBundle(transform.getOutput());
        while (contentsRemaining) {
          output.add(
              WindowedValue.timestampedValueInGlobalWindow(
                  reader.getCurrent(), reader.getCurrentTimestamp()));
          contentsRemaining = reader.advance();
        }
        resultBuilder.addOutput(output);
      }
    }

    @Override
    public TransformResult finishBundle()  {
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
          source.splitIntoBundles(bytesPerBundle, options);
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
}
