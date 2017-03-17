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

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link RootInputProvider} that delegates to primitive {@link RootInputProvider} implementations
 * based on the type of {@link PTransform} of the application.
 */
class RootProviderRegistry {
  public static RootProviderRegistry defaultRegistry(EvaluationContext context) {
    ImmutableMap.Builder<Class<? extends PTransform>, RootInputProvider<?, ?, ?, ?>>
        defaultProviders = ImmutableMap.builder();
    defaultProviders
        .put(Read.Bounded.class, new BoundedReadEvaluatorFactory.InputProvider(context))
        .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory.InputProvider(context))
        .put(
            TestStreamEvaluatorFactory.DirectTestStreamFactory.DirectTestStream.class,
            new TestStreamEvaluatorFactory.InputProvider(context))
        .put(PCollections.class, new EmptyInputProvider());
    return new RootProviderRegistry(defaultProviders.build());
  }

  private final Map<Class<? extends PTransform>, RootInputProvider<?, ?, ?, ?>> providers;

  private RootProviderRegistry(
      Map<Class<? extends PTransform>, RootInputProvider<?, ?, ?, ?>> providers) {
    this.providers = providers;
  }

  public Collection<CommittedBundle<?>> getInitialInputs(
      AppliedPTransform<?, ?, ?> transform, int targetParallelism) throws Exception {
    Class<? extends PTransform> transformClass = transform.getTransform().getClass();
    RootInputProvider provider =
        checkNotNull(
            providers.get(transformClass),
            "Tried to get a %s for a Transform of type %s, but there is no such provider",
            RootInputProvider.class.getSimpleName(),
            transformClass.getSimpleName());
    return provider.getInitialInputs(transform, targetParallelism);
  }
}
