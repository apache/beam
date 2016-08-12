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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten.FlattenPCollectionList;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEvaluatorRegistry.class);
  public static TransformEvaluatorRegistry defaultRegistry() {
    @SuppressWarnings("rawtypes")
    ImmutableMap<Class<? extends PTransform>, TransformEvaluatorFactory> primitives =
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .put(Read.Bounded.class, new BoundedReadEvaluatorFactory())
            .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory())
            .put(ParDo.Bound.class, new ParDoSingleEvaluatorFactory())
            .put(ParDo.BoundMulti.class, new ParDoMultiEvaluatorFactory())
            .put(FlattenPCollectionList.class, new FlattenEvaluatorFactory())
            .put(ViewEvaluatorFactory.WriteView.class, new ViewEvaluatorFactory())
            .put(Window.Bound.class, new WindowEvaluatorFactory())
            // Runner-specific primitives used in expansion of GroupByKey
            .put(DirectGroupByKeyOnly.class, new GroupByKeyOnlyEvaluatorFactory())
            .put(DirectGroupAlsoByWindow.class, new GroupAlsoByWindowEvaluatorFactory())
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  @SuppressWarnings("rawtypes")
  private final Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes")
      Map<Class<? extends PTransform>, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext)
      throws Exception {
    checkState(
        !finished.get(), "Tried to get an evaluator for a finished TransformEvaluatorRegistry");
    TransformEvaluatorFactory factory = factories.get(application.getTransform().getClass());
    return factory.forApplication(application, inputBundle, evaluationContext);
  }

  @Override
  public void cleanup() throws Exception {
    Collection<Exception> thrownInCleanup = new ArrayList<>();
    for (TransformEvaluatorFactory factory : factories.values()) {
      try {
        factory.cleanup();
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        thrownInCleanup.add(e);
      }
    }
    finished.set(true);
    if (!thrownInCleanup.isEmpty()) {
      LOG.error("Exceptions {} thrown while cleaning up evaluators", thrownInCleanup);
      Exception toThrow = null;
      for (Exception e : thrownInCleanup) {
        if (toThrow == null) {
          toThrow = e;
        } else {
          toThrow.addSuppressed(e);
        }
      }
      throw toThrow;
    }
  }
}
