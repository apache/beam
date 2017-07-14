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
package org.apache.beam.runners.jstorm.translation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.runners.jstorm.translation.translator.TransformTranslator;
import org.apache.beam.runners.jstorm.translation.translator.ViewTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pipleline translator of JStorm.
 */
public class JStormPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {
  private static final Logger LOG = LoggerFactory.getLogger(JStormPipelineTranslator.class);
  private TranslationContext context;
  private int depth = 0;

  public JStormPipelineTranslator(TranslationContext context) {
    this.context = context;
  }

  public void translate(Pipeline pipeline) {
    List<PTransformOverride> transformOverrides =
        ImmutableList.<PTransformOverride>builder()
            .add(PTransformOverride.of(PTransformMatchers.classEqualTo(View.AsIterable.class),
                new ReflectiveOneToOneOverrideFactory(ViewTranslator.ViewAsIterable.class)))
            .add(PTransformOverride.of(PTransformMatchers.classEqualTo(View.AsList.class),
                new ReflectiveOneToOneOverrideFactory(ViewTranslator.ViewAsList.class)))
            .add(PTransformOverride.of(PTransformMatchers.classEqualTo(View.AsMap.class),
                new ReflectiveOneToOneOverrideFactory(ViewTranslator.ViewAsMap.class)))
            .add(PTransformOverride.of(PTransformMatchers.classEqualTo(View.AsMultimap.class),
                new ReflectiveOneToOneOverrideFactory(ViewTranslator.ViewAsMultimap.class)))
            .add(PTransformOverride.of(PTransformMatchers.classEqualTo(View.AsSingleton.class),
                new ReflectiveOneToOneOverrideFactory(ViewTranslator.ViewAsSingleton.class)))
            .add(PTransformOverride.of(
                PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
                new ReflectiveOneToOneOverrideFactory(
                    (ViewTranslator.CombineGloballyAsSingletonView.class))))
            .build();
    pipeline.replaceAll(transformOverrides);
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info(genSpaces(this.depth) + "enterCompositeTransform- " + node);
    this.depth++;

    // check if current composite transforms need to be translated.
    // If not, all sub transforms will be translated in visitPrimitiveTransform.
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyStreamingTransform(transform, node, translator);
        LOG.info(genSpaces(this.depth) + "translated-" + node);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    this.depth--;
    LOG.info(genSpaces(this.depth) + "leaveCompositeTransform- " + node);
  }

  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.info(genSpaces(this.depth) + "visitPrimitiveTransform- " + node);

    if (!node.isRootNode()) {
      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);
      if (translator == null || !applyCanTranslate(transform, node, translator)) {
        LOG.info(node.getTransform().getClass().toString());
        throw new UnsupportedOperationException(
            "The transform " + transform + " is currently not supported.");
      }
      applyStreamingTransform(transform, node, translator);
    }
  }

  public void visitValue(PValue value, TransformHierarchy.Node node) {
    LOG.info(genSpaces(this.depth) + "visiting value {}", value);
  }

  private <T extends PTransform<?, ?>> void applyStreamingTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;

    context.getUserGraphContext().setCurrentTransform(node.toAppliedPTransform());
    typedTranslator.translateNode(typedTransform, context);

    // Maintain PValue to TupleTag map for side inputs translation.
    context.getUserGraphContext().recordOutputTaggedPValue();
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;

    context.getUserGraphContext().setCurrentTransform(node.toAppliedPTransform());

    return typedTranslator.canTranslate(typedTransform, context);
  }

  /**
   * Utility formatting method.
   *
   * @param n number of spaces to generate
   * @return String with "|" followed by n spaces
   */
  protected static String genSpaces(int n) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < n; i++) {
      builder.append("|   ");
    }
    return builder.toString();
  }

  private static class ReflectiveOneToOneOverrideFactory<
      InputT extends PValue,
      OutputT extends PValue,
      TransformT extends PTransform<InputT, OutputT>>
      extends SingleInputOutputOverrideFactory<InputT, OutputT, TransformT> {
    private final Class<PTransform<InputT, OutputT>> replacement;

    private ReflectiveOneToOneOverrideFactory(
        Class<PTransform<InputT, OutputT>> replacement) {
      this.replacement = replacement;
    }

    @Override
    public PTransformReplacement<InputT, OutputT> getReplacementTransform(
        AppliedPTransform<InputT, OutputT, TransformT> appliedPTransform) {
      PTransform<InputT, OutputT> originalPTransform = appliedPTransform.getTransform();
      PTransform<InputT, OutputT> replacedPTransform = InstanceBuilder.ofType(replacement)
          .withArg(
              (Class<PTransform<InputT, OutputT>>) originalPTransform.getClass(),
              originalPTransform)
          .build();
      InputT inputT = (InputT) Iterables.getOnlyElement(appliedPTransform.getInputs().values());
      return PTransformReplacement.of(inputT, replacedPTransform);
    }
  }
}
