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

import com.google.common.collect.Iterables;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * A {@link TransformEvaluatorFactory} that produces {@link TransformEvaluator TransformEvaluators}
 * for the {@link Read Read} primitives, whether bounded or unbounded.
 */
final class ReadEvaluatorFactory implements TransformEvaluatorFactory {

  final BoundedReadEvaluatorFactory boundedFactory;
  final UnboundedReadEvaluatorFactory unboundedFactory;

  public ReadEvaluatorFactory(EvaluationContext context) {
    boundedFactory = new BoundedReadEvaluatorFactory(context);
    unboundedFactory = new UnboundedReadEvaluatorFactory(context);
  }

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {

    PCollection<?> output =
        (PCollection) Iterables.getOnlyElement(application.getOutputs().values());
    switch (output.isBounded()) {
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
}
