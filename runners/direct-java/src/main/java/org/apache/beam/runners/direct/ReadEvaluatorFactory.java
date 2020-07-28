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

import java.util.Collection;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Read Read} primitives, whether bounded or unbounded.
 */
final class ReadEvaluatorFactory implements TransformEvaluatorFactory {

  final BoundedReadEvaluatorFactory boundedFactory;
  final UnboundedReadEvaluatorFactory unboundedFactory;

  public ReadEvaluatorFactory(EvaluationContext context, PipelineOptions options) {
    boundedFactory = new BoundedReadEvaluatorFactory(context, options);
    unboundedFactory = new UnboundedReadEvaluatorFactory(context, options);
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    switch (ReadTranslation.sourceIsBounded(application)) {
      case BOUNDED:
        return boundedFactory.forApplication(application, inputBundle);
      case UNBOUNDED:
        return unboundedFactory.forApplication(application, inputBundle);
      default:
        throw new IllegalArgumentException("PCollection is neither bounded nor unbounded?!?");
    }
  }

  @Override
  public void cleanup() throws Exception {
    boundedFactory.cleanup();
    unboundedFactory.cleanup();
  }

  static <T> InputProvider<T> inputProvider(EvaluationContext context, PipelineOptions options) {
    return new InputProvider<>(context, options);
  }

  private static class InputProvider<T> implements RootInputProvider<T, SourceShard<T>, PBegin> {

    private final UnboundedReadEvaluatorFactory.InputProvider<T> unboundedInputProvider;
    private final BoundedReadEvaluatorFactory.InputProvider<T> boundedInputProvider;

    InputProvider(EvaluationContext context, PipelineOptions options) {
      this.unboundedInputProvider =
          new UnboundedReadEvaluatorFactory.InputProvider<>(context, options);
      this.boundedInputProvider = new BoundedReadEvaluatorFactory.InputProvider<>(context, options);
    }

    @Override
    public Collection<CommittedBundle<SourceShard<T>>> getInitialInputs(
        AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>
            appliedTransform,
        int targetParallelism)
        throws Exception {
      switch (ReadTranslation.sourceIsBounded(appliedTransform)) {
        case BOUNDED:
          // This cast could be made unnecessary, but too much bounded polymorphism
          return (Collection)
              boundedInputProvider.getInitialInputs(appliedTransform, targetParallelism);
        case UNBOUNDED:
          // This cast could be made unnecessary, but too much bounded polymorphism
          return (Collection)
              unboundedInputProvider.getInitialInputs(appliedTransform, targetParallelism);
        default:
          throw new IllegalArgumentException("PCollection is neither bounded nor unbounded?!?");
      }
    }
  }
}
