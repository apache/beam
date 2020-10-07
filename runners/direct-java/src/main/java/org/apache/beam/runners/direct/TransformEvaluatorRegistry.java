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

import static org.apache.beam.runners.core.construction.PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.CREATE_VIEW_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.FLATTEN_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.IMPULSE_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.PAR_DO_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.PTransformTranslation.READ_TRANSFORM_URN;
import static org.apache.beam.runners.core.construction.SplittableParDo.SPLITTABLE_PROCESS_URN;
import static org.apache.beam.runners.direct.DirectGroupByKey.DIRECT_GABW_URN;
import static org.apache.beam.runners.direct.DirectGroupByKey.DIRECT_GBKO_URN;
import static org.apache.beam.runners.direct.DirectWriteViewVisitor.DIRECT_WRITE_VIEW_URN;
import static org.apache.beam.runners.direct.MultiStepCombine.DIRECT_MERGE_ACCUMULATORS_EXTRACT_OUTPUT_URN;
import static org.apache.beam.runners.direct.ParDoMultiOverrideFactory.DIRECT_STATEFUL_PAR_DO_URN;
import static org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DIRECT_TEST_STREAM_URN;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems.ProcessElements;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory.DirectTestStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TransformEvaluatorFactory} that delegates to primitive {@link TransformEvaluatorFactory}
 * implementations based on the type of {@link PTransform} of the application.
 */
class TransformEvaluatorRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TransformEvaluatorRegistry.class);

  /**
   * Create the {@link TransformEvaluatorRegistry} that executes a {@link Pipeline} written with the
   * Java SDK by directly invoking the classes of that SDK.
   *
   * <p>This is the legacy implementation of the {@link DirectRunner} engine.
   */
  public static TransformEvaluatorRegistry javaSdkNativeRegistry(
      EvaluationContext ctxt, PipelineOptions options) {
    ImmutableMap<String, TransformEvaluatorFactory> primitives =
        ImmutableMap.<String, TransformEvaluatorFactory>builder()
            // Beam primitives
            .put(READ_TRANSFORM_URN, new ReadEvaluatorFactory(ctxt, options))
            .put(
                PAR_DO_TRANSFORM_URN,
                new ParDoEvaluatorFactory<>(
                    ctxt,
                    ParDoEvaluator.defaultRunnerFactory(),
                    ParDoEvaluatorFactory.basicDoFnCacheLoader(),
                    options))
            .put(FLATTEN_TRANSFORM_URN, new FlattenEvaluatorFactory(ctxt))
            .put(ASSIGN_WINDOWS_TRANSFORM_URN, new WindowEvaluatorFactory(ctxt))
            .put(IMPULSE_TRANSFORM_URN, new ImpulseEvaluatorFactory(ctxt))

            // Runner-specific primitives
            .put(DIRECT_WRITE_VIEW_URN, new ViewEvaluatorFactory(ctxt))
            .put(DIRECT_STATEFUL_PAR_DO_URN, new StatefulParDoEvaluatorFactory<>(ctxt, options))
            .put(DIRECT_GBKO_URN, new GroupByKeyOnlyEvaluatorFactory(ctxt))
            .put(DIRECT_GABW_URN, new GroupAlsoByWindowEvaluatorFactory(ctxt, options))
            .put(DIRECT_TEST_STREAM_URN, new TestStreamEvaluatorFactory(ctxt))
            .put(
                DIRECT_MERGE_ACCUMULATORS_EXTRACT_OUTPUT_URN,
                new MultiStepCombine.MergeAndExtractAccumulatorOutputEvaluatorFactory(ctxt))

            // Runners-core primitives
            .put(
                SPLITTABLE_PROCESS_URN,
                new SplittableProcessElementsEvaluatorFactory<>(ctxt, options))

            // Deprecated primitives already handled
            .put(CREATE_VIEW_TRANSFORM_URN, new CreateViewNoopEvaluatorFactory())
            .build();
    return new TransformEvaluatorRegistry(primitives);
  }

  /** Registers classes specialized to the direct runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class DirectTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap
          .<Class<? extends PTransform>, PTransformTranslation.TransformPayloadTranslator>builder()
          .put(
              DirectGroupByKey.DirectGroupByKeyOnly.class,
              TransformPayloadTranslator.NotSerializable.forUrn(DIRECT_GBKO_URN))
          .put(
              DirectGroupByKey.DirectGroupAlsoByWindow.class,
              TransformPayloadTranslator.NotSerializable.forUrn(DIRECT_GABW_URN))
          .put(
              ParDoMultiOverrideFactory.StatefulParDo.class,
              TransformPayloadTranslator.NotSerializable.forUrn(DIRECT_STATEFUL_PAR_DO_URN))
          .put(
              DirectWriteViewVisitor.WriteView.class,
              TransformPayloadTranslator.NotSerializable.forUrn(DIRECT_WRITE_VIEW_URN))
          .put(
              DirectTestStream.class,
              TransformPayloadTranslator.NotSerializable.forUrn(DIRECT_TEST_STREAM_URN))
          .put(
              SplittableParDoViaKeyedWorkItems.ProcessElements.class,
              TransformPayloadTranslator.NotSerializable.forUrn(SPLITTABLE_PROCESS_URN))
          .build();
    }
  }

  /**
   * A translator just to vend the URN. This will need to be moved to runners-core-construction-java
   * once SDF is reorganized appropriately.
   */
  private static class SplittableParDoProcessElementsTranslator
      extends TransformPayloadTranslator.NotSerializable<ProcessElements<?, ?, ?, ?, ?>> {

    private SplittableParDoProcessElementsTranslator() {}

    @Override
    public String getUrn(ProcessElements<?, ?, ?, ?, ?> transform) {
      return SPLITTABLE_PROCESS_URN;
    }
  }

  // the TransformEvaluatorFactories can construct instances of all generic types of transform,
  // so all instances of a primitive can be handled with the same evaluator factory.
  private final Map<String, TransformEvaluatorFactory> factories;

  private final AtomicBoolean finished = new AtomicBoolean(false);

  private TransformEvaluatorRegistry(
      @SuppressWarnings("rawtypes") Map<String, TransformEvaluatorFactory> factories) {
    this.factories = factories;
  }

  public <InputT> @Nullable TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, CommittedBundle<?> inputBundle) throws Exception {
    checkState(
        !finished.get(), "Tried to get an evaluator for a finished TransformEvaluatorRegistry");

    String urn = PTransformTranslation.urnForTransform(application.getTransform());

    TransformEvaluatorFactory factory =
        checkNotNull(factories.get(urn), "No evaluator for PTransform \"%s\"", urn);
    return factory.forApplication(application, inputBundle);
  }

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
