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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.TransformPayloadTranslatorRegistrar;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.samza.operators.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows all the translators from a primitive BEAM transform to a Samza operator.
 */
public class SamzaPipelineTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaPipelineTranslator.class);

  private static final Map<String, TransformTranslator<?>> TRANSLATORS =
      ImmutableMap.<String, TransformTranslator<?>>builder()
          .put(PTransformTranslation.READ_TRANSFORM_URN, new ReadTranslator())
          .put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new ParDoBoundMultiTranslator())
          .put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new GroupByKeyTranslator())
          .put(PTransformTranslation.COMBINE_TRANSFORM_URN, new GroupByKeyTranslator())
          .put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new WindowAssignTranslator())
          .put(PTransformTranslation.FLATTEN_TRANSFORM_URN, new FlattenPCollectionsTranslator())
          .put(SamzaPublishView.SAMZA_PUBLISH_VIEW_URN, new SamzaPublishViewTranslator())
          .build();

  private SamzaPipelineTranslator() {}

  public static void translate(Pipeline pipeline,
                               SamzaPipelineOptions options,
                               StreamGraph graph,
                               Map<PValue, String> idMap,
                               PValue dummySource) {


    final TranslationContext ctx = new TranslationContext(graph, idMap, options, dummySource);
    final TranslationVisitor visitor = new TranslationVisitor(ctx);
    pipeline.traverseTopologically(visitor);
  }

  private static class TranslationVisitor extends Pipeline.PipelineVisitor.Defaults {
    private final TranslationContext ctx;
    private int topologicalId = 0;

    private TranslationVisitor(TranslationContext ctx) {
      this.ctx = ctx;
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
      checkArgument(canTranslate(urn, transform),
          String.format("Unsupported transform class: %s. Node: %s", transform, node));

      applyTransform(transform, node, TRANSLATORS.get(urn));
    }

    private <T extends PTransform<?, ?>> void applyTransform(
        T transform,
        TransformHierarchy.Node node,
        TransformTranslator<?> translator) {

      ctx.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
      ctx.setCurrentTopologicalId(topologicalId++);

      @SuppressWarnings("unchecked")
      final TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
      typedTranslator.translate(transform, node, ctx);

      ctx.clearCurrentTransform();
    }

    private static boolean canTranslate(String urn, PTransform<?, ?> transform) {
      if (!TRANSLATORS.containsKey(urn)) {
        return false;
      } else if (urn.equals(PTransformTranslation.COMBINE_TRANSFORM_URN)) {
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

  /** Registers classes specialized to the Samza runner. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class SamzaTransformsRegistrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>,
               ? extends PTransformTranslation.TransformPayloadTranslator>
    getTransformPayloadTranslators() {
      return ImmutableMap.of(SamzaPublishView.class,
          new SamzaPublishView.SamzaPublishViewPayloadTranslator());
    }

    @Override
    public Map<String, PTransformTranslation.TransformPayloadTranslator> getTransformRehydrators() {
      return Collections.emptyMap();
    }
  }
}
