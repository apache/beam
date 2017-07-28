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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.runners.mapreduce.MapReduceRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Pipeline translator for {@link MapReduceRunner}.
 */
public class GraphConverter extends Pipeline.PipelineVisitor.Defaults {

  private final TranslationContext context;
  private final Map<PValue, TupleTag<?>> pValueToTupleTag;

  public GraphConverter(TranslationContext context) {
    this.context = checkNotNull(context, "context");
    this.pValueToTupleTag = Maps.newHashMap();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    // check if current composite transforms need to be translated.
    // If not, all sub transforms will be translated in visitPrimitiveTransform.
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);

      if (translator != null && applyCanTranslate(transform, node, translator)) {
        applyTransform(transform, node, translator);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (!node.isRootNode()) {
      PTransform<?, ?> transform = node.getTransform();
      TransformTranslator translator = TranslatorRegistry.getTranslator(transform);
      if (translator == null || !applyCanTranslate(transform, node, translator)) {
        throw new UnsupportedOperationException(
            "The transform " + transform + " is currently not supported.");
      }
      applyTransform(transform, node, translator);
    }
  }

  private <T extends PTransform<?, ?>> void applyTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
    context.getUserGraphContext().setCurrentNode(node);
    typedTranslator.translateNode(typedTransform, context);
  }

  private <T extends PTransform<?, ?>> boolean applyCanTranslate(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      TransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;
    @SuppressWarnings("unchecked")
    TransformTranslator<T> typedTranslator = (TransformTranslator<T>) translator;
    context.getUserGraphContext().setCurrentNode(node);
    return typedTranslator.canTranslate(typedTransform, context);
  }
}
