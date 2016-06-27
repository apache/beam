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

package org.apache.beam.runners.apex;

import org.apache.beam.runners.apex.translators.CreateValuesTranslator;
import org.apache.beam.runners.apex.translators.FlattenPCollectionTranslator;
import org.apache.beam.runners.apex.translators.GroupByKeyTranslator;
import org.apache.beam.runners.apex.translators.ParDoBoundTranslator;
import org.apache.beam.runners.apex.translators.ReadUnboundedTranslator;
import org.apache.beam.runners.apex.translators.TransformTranslator;
import org.apache.beam.runners.apex.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link ApexPipelineTranslator} translates {@link Pipeline} objects
 * into Apex logical plan {@link DAG}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ApexPipelineTranslator implements Pipeline.PipelineVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(
      ApexPipelineTranslator.class);

  /**
   * A map from {@link PTransform} subclass to the corresponding
   * {@link TransformTranslator} to use to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, TransformTranslator>
      transformTranslators = new HashMap<>();

  private final TranslationContext translationContext;

  static {
    // register TransformTranslators
    registerTransformTranslator(ParDo.Bound.class, new ParDoBoundTranslator());
    registerTransformTranslator(Read.Unbounded.class, new ReadUnboundedTranslator());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator());
    registerTransformTranslator(Flatten.FlattenPCollectionList.class,
        new FlattenPCollectionTranslator());
    registerTransformTranslator(Create.Values.class, new CreateValuesTranslator());
  }

  public ApexPipelineTranslator(TranslationContext translationContext) {
    this.translationContext = translationContext;
  }

  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
    LOG.debug("entering composite transform {}", node.getTransform());
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
    LOG.debug("leaving composite transform {}", node.getTransform());
  }

  @Override
  public void visitPrimitiveTransform(TransformTreeNode node) {
    LOG.debug("visiting transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    TransformTranslator translator = getTransformTranslator(transform.getClass());
    if (null == translator) {
      throw new IllegalStateException(
          "no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node);
    translator.translate(transform, translationContext);
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    LOG.debug("visiting value {}", value);
  }

  /**
   * Records that instances of the specified PTransform class
   * should be translated by default by the corresponding
   * {@link TransformTranslator}.
   */
  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (transformTranslators.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException(
          "defining multiple translators for " + transformClass);
    }
  }

  /**
   * Returns the {@link TransformTranslator} to use for instances of the
   * specified PTransform class, or null if none registered.
   */
  private <TransformT extends PTransform<?,?>>
  TransformTranslator<TransformT> getTransformTranslator(Class<TransformT> transformClass) {
    return transformTranslators.get(transformClass);
  }


}
