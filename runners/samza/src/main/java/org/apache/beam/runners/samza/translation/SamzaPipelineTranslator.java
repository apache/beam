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
package org.apache.beam.runners.samza.translation;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.runners.samza.metrics.SamzaMetricOpFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** This class knows all the translators from a primitive BEAM transform to a Samza operator. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SamzaPipelineTranslator {

  private static final Map<String, TransformTranslator<?>> TRANSLATORS = loadTranslators();

  private static Map<String, TransformTranslator<?>> loadTranslators() {
    Map<String, TransformTranslator<?>> translators = new HashMap<>();
    for (SamzaTranslatorRegistrar registrar : ServiceLoader.load(SamzaTranslatorRegistrar.class)) {
      translators.putAll(registrar.getTransformTranslators());
    }
    return ImmutableMap.copyOf(translators);
  }

  private SamzaPipelineTranslator() {}

  public static void translate(Pipeline pipeline, TranslationContext ctx) {
    final TransformVisitorFn translateFn =
        new TransformVisitorFn() {

          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {
            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));
            ctx.attachTransformMetricOp(
                (PTransform<? extends PValue, ? extends PValue>) transform,
                node,
                SamzaMetricOpFactory.OpType.INPUT);

            translator.translate(transform, node, ctx);

            ctx.attachTransformMetricOp(
                (PTransform<? extends PValue, ? extends PValue>) transform,
                node,
                SamzaMetricOpFactory.OpType.OUTPUT);
            ctx.clearCurrentTransform();
          }
        };
    final SamzaPipelineVisitor visitor = new SamzaPipelineVisitor(translateFn);
    pipeline.traverseTopologically(visitor);
  }

  public static void createConfig(
      Pipeline pipeline, ConfigContext ctx, ConfigBuilder configBuilder) {

    final TransformVisitorFn configFn =
        new TransformVisitorFn() {
          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {

            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));

            if (translator instanceof TransformConfigGenerator) {
              TransformConfigGenerator<T> configGenerator =
                  (TransformConfigGenerator<T>) translator;
              configBuilder.putAll(configGenerator.createConfig(transform, node, ctx));
            }

            ctx.clearCurrentTransform();
          }
        };
    final SamzaPipelineVisitor visitor = new SamzaPipelineVisitor(configFn);
    pipeline.traverseTopologically(visitor);
  }

  public interface TransformVisitorFn {
    <T extends PTransform<?, ?>> void apply(
        T transform,
        TransformHierarchy.Node node,
        Pipeline pipeline,
        TransformTranslator<T> translator);
  }

  public static class SamzaPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final TransformVisitorFn visitorFn;

    public SamzaPipelineVisitor(TransformVisitorFn visitorFn) {
      this.visitorFn = visitorFn;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
      final PTransform<?, ?> transform = node.getTransform();
      final String urn = getUrnForTransform(transform);
      if (canTranslate(urn, transform)) {
        applyTransform(transform, node, TRANSLATORS.get(urn));
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void visitPrimitiveTransform(TransformHierarchy.Node node) {
      final PTransform<?, ?> transform = node.getTransform();
      final String urn = getUrnForTransform(transform);
      checkArgument(
          canTranslate(urn, transform),
          String.format("Unsupported transform class: %s. Node: %s", transform, node));

      applyTransform(transform, node, TRANSLATORS.get(urn));
    }

    private <T extends PTransform<?, ?>> void applyTransform(
        T transform, TransformHierarchy.Node node, TransformTranslator<?> translator) {

      @SuppressWarnings("unchecked")
      final TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
      visitorFn.apply(transform, node, getPipeline(), typedTranslator);
    }

    private static boolean canTranslate(String urn, PTransform<?, ?> transform) {
      if (!TRANSLATORS.containsKey(urn)) {
        return false;
      } else if (urn.equals(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN)) {
        // According to BEAM, Combines with side inputs are translated as generic composites
        return ((Combine.PerKey) transform).getSideInputs().isEmpty();
      } else {
        return true;
      }
    }

    private static String getUrnForTransform(PTransform<?, ?> transform) {
      return transform == null ? null : PTransformTranslation.urnForTransformOrNull(transform);
    }
  }

  /** Registers Samza translators. */
  @AutoService(SamzaTranslatorRegistrar.class)
  public static class SamzaTranslators implements SamzaTranslatorRegistrar {

    @Override
    public Map<String, TransformTranslator<?>> getTransformTranslators() {
      return ImmutableMap.<String, TransformTranslator<?>>builder()
          .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator<>())
          .put(PTransformTranslation.RESHUFFLE_URN, new ReshuffleTranslator<>())
          .put(PTransformTranslation.REDISTRIBUTE_BY_KEY_URN, new RedistributeByKeyTranslator<>())
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoBoundMultiTranslator<>())
          .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>())
          .put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new GroupByKeyTranslator<>())
          .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator<>())
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionsTranslator<>())
          .put(SamzaPublishView.SAMZA_PUBLISH_VIEW_URN, new SamzaPublishViewTranslator<>())
          .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator())
          .put(ExecutableStage.URN, new ParDoBoundMultiTranslator<>())
          .put(PTransformTranslation.TEST_STREAM_TRANSFORM_URN, new SamzaTestStreamTranslator())
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
              new SplittableParDoTranslators.ProcessKeyedElements<>())
          .build();
    }
  }

  /** Registers classes specialized to the Samza runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class SamzaTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<
            ? extends Class<? extends PTransform>,
            ? extends PTransformTranslation.TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return ImmutableMap.of(
          SamzaPublishView.class, new SamzaPublishView.SamzaPublishViewPayloadTranslator());
    }
  }
}
