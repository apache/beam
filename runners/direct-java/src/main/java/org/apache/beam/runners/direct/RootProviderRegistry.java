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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.runners.core.construction.PTransformTranslation.FLATTEN_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.IMPULSE_TRANSFORM_URN;
import static org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DIRECT_TEST_STREAM_URN;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link RootInputProvider} that delegates to primitive {@link RootInputProvider} implementations
 * based on the type of {@link PTransform} of the application.
 */
class RootProviderRegistry {
  public static RootProviderRegistry defaultRegistry(EvaluationContext context) {
    ImmutableMap.Builder<String, RootInputProvider<?, ?, ?>>
        defaultProviders = ImmutableMap.builder();
    defaultProviders
        .put(IMPULSE_TRANSFORM_URN, new ImpulseEvaluatorFactory.ImpulseRootProvider(context))
        .put(PTransformTranslation.READ_TRANSFORM_URN, ReadEvaluatorFactory.inputProvider(context))
        .put(DIRECT_TEST_STREAM_URN, new TestStreamEvaluatorFactory.InputProvider(context))
        .put(FLATTEN_TRANSFORM_URN, new EmptyInputProvider());
    return new RootProviderRegistry(defaultProviders.build());
  }

  private final Map<String, RootInputProvider<?, ?, ?>> providers;

  private RootProviderRegistry(
      Map<String, RootInputProvider<?, ?, ?>> providers) {
    this.providers = providers;
  }

  public Collection<CommittedBundle<?>> getInitialInputs(
      AppliedPTransform<?, ?, ?> transform, int targetParallelism) throws Exception {
    String transformUrn = PTransformTranslation.urnForTransform(transform.getTransform());
    RootInputProvider provider =
        checkNotNull(
            providers.get(transformUrn),
            "Tried to get a %s for a transform \"%s\", but there is no such provider",
            RootInputProvider.class.getSimpleName(),
            transformUrn);
    return provider.getInitialInputs(transform, targetParallelism);
  }
}
