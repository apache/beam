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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class knows all the translators from a primitive BEAM transform to a Samza operator. */
public class SamzaPipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineTranslator.class);

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
    checkState(
        ctx.getPipelineOptions().getMaxBundleSize() <= 1,
        "bundling is not supported for non portable mode. Please disable bundling (by setting max bundle size to 1).");
    final TransformVisitorFn translateFn =
        new TransformVisitorFn() {

          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {
            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));

            translator.translate(transform, node, ctx);

            ctx.clearCurrentTransform();
          }
        };
    final SamzaPipelineVisitor visitor = new SamzaPipelineVisitor(translateFn);
    pipeline.traverseTopologically(visitor);
  }

  public static void createConfig(
      Pipeline pipeline,
      SamzaPipelineOptions options,
      Map<PValue, String> idMap,
      ConfigBuilder configBuilder) {
    final ConfigContext ctx = new ConfigContext(idMap, options);

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

  private interface TransformVisitorFn {
    <T extends PTransform<?, ?>> void apply(
        T transform,
        TransformHierarchy.Node node,
        Pipeline pipeline,
        TransformTranslator<T> translator);
  }

  private static class SamzaPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {
    private TransformVisitorFn visitorFn;

    private SamzaPipelineVisitor(TransformVisitorFn visitorFn) {
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
          .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator())
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoBoundMultiTranslator())
          .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator())
          .put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, new GroupByKeyTranslator())
          .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator())
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionsTranslator())
          .put(SamzaPublishView.SAMZA_PUBLISH_VIEW_URN, new SamzaPublishViewTranslator())
          .put(PTransformTranslation.IMPULSE_TRANSFORM_URN, new ImpulseTranslator())
          .put(
              PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
              new SplittableParDoTranslators.ProcessKeyedElements<>())
          .put(ExecutableStage.URN, new ParDoBoundMultiTranslator())
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
