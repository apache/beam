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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupAlsoByWindow;
import org.apache.beam.runners.direct.DirectGroupByKey.DirectGroupByKeyOnly;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.ParDoMultiOverrideFactory.StatefulParDo;
import org.apache.beam.runners.direct.ViewOverrideFactory.WriteView;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry implements TransformEvaluatorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEvaluatorRegistry.class);
  public static TransformEvaluatorRegistry defaultRegistry(EvaluationContext ctxt) {
    @SuppressWarnings({"rawtypes"})
    ImmutableMap<Class<? extends PTransform>, TransformEvaluatorFactory> primitives =
        ImmutableMap.<Class<? extends PTransform>, TransformEvaluatorFactory>builder()
            .put(Read.Bounded.class, new BoundedReadEvaluatorFactory(ctxt))
            .put(Read.Unbounded.class, new UnboundedReadEvaluatorFactory(ctxt))
            .put(
                ParDo.MultiOutput.class,
                new ParDoEvaluatorFactory<>(ctxt, ParDoEvaluator.defaultRunnerFactory()))
            .put(StatefulParDo.class, new StatefulParDoEvaluatorFactory<>(ctxt))
            .put(PCollections.class, new FlattenEvaluatorFactory(ctxt))
            .put(WriteView.class, new ViewEvaluatorFactory(ctxt))
            .put(Window.Assign.class, new WindowEvaluatorFactory(ctxt))
            // Runner-specific primitives used in expansion of GroupByKey
            .put(DirectGroupByKeyOnly.class, new GroupByKeyOnlyEvaluatorFactory(ctxt))
            .put(DirectGroupAlsoByWindow.class, new GroupAlsoByWindowEvaluatorFactory(ctxt))
            .put(
                TestStreamEvaluatorFactory.DirectTestStreamFactory.DirectTestStream.class,
                new TestStreamEvaluatorFactory(ctxt))
            // Runner-specific primitive used in expansion of SplittableParDo
            .put(
                SplittableParDo.ProcessElements.class,
                new SplittableProcessElementsEvaluatorFactory<>(ctxt))
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
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle)
      throws Exception {
    checkState(
        !finished.get(), "Tried to get an evaluator for a finished TransformEvaluatorRegistry");
    Class<? extends PTransform> transformClass = application.getTransform().getClass();
    TransformEvaluatorFactory factory =
        checkNotNull(
            factories.get(transformClass), "No evaluator for PTransform type %s", transformClass);
    return factory.forApplication(application, inputBundle);
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
