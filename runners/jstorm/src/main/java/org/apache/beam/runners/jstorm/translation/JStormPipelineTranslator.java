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

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.runners.jstorm.translation.translator.TransformTranslator;
import org.apache.beam.runners.jstorm.translation.translator.TranslatorRegistry;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Pipeline.PipelineVisitor} that translates {@link Pipeline Pipelines} to JStorm jobs.
 */
public class JStormPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  private static final Logger LOG = LoggerFactory.getLogger(JStormPipelineTranslator.class);

  private final TranslationContext context;
  private int depth = 0;

  public JStormPipelineTranslator(TranslationContext context) {
    this.context = context;
  }

  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.debug(genLogPrefix(this.depth) + "enterCompositeTransform- " + node);
    this.depth++;

    // check if current composite transforms need to be translated.
    // If not, all sub transforms will be translated in visitPrimitiveTransform.
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyTransform(transform, node, translator);
        LOG.debug(genLogPrefix(this.depth) + "translated-" + node);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    this.depth--;
    LOG.debug(genLogPrefix(this.depth) + "leaveCompositeTransform- " + node);
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.debug(genLogPrefix(this.depth) + "visitPrimitiveTransform- " + node);

    if (!node.isRootNode()) {
      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);
      checkState(
          translator != null && applyCanTranslate(transform, node, translator),
          "The primitive transform " + transform + " is currently not supported.");
      applyTransform(transform, node, translator);
    }
  }

  private <T extends PTransform<?, ?>> void applyTransform(
      PTransform<?, ?> transform, TransformHierarchy.Node node, TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;

    typedTranslator.translateNode(typedTransform, context);
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform, TransformHierarchy.Node node, TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;

    return typedTranslator.canTranslate(typedTransform, context);
  }

  /**
   * Generates the log prefix with the given depth.
   *
   * @param depth depth of the log prefix.
   * @return the log prefix which is the concatenation of "|    " for {@code depth} times.
   */
  protected static String genLogPrefix(int depth) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      builder.append("|   ");
    }
    return builder.toString();
  }
}
