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
package org.apache.beam.runners.jet;

import com.hazelcast.jet.core.DAG;
import java.util.function.Function;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.values.PValue;

/** Logic that specifies how to apply translations when traversing the nodes of a Beam pipeline. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class JetGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

  private final JetTranslationContext translationContext;
  private final Function<PTransform<?, ?>, JetTransformTranslator<?>> translatorProvider;

  private boolean finalized = false;

  JetGraphVisitor(
      JetPipelineOptions options,
      Function<PTransform<?, ?>, JetTransformTranslator<?>> translatorProvider) {
    this.translationContext = new JetTranslationContext(options);
    this.translatorProvider = translatorProvider;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    if (finalized) {
      throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");
    }

    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      JetTransformTranslator<?> translator = translatorProvider.apply(transform);
      if (translator != null) {
        translate(node, translator);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    if (finalized) {
      throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");
    }
    if (node.isRootNode()) {
      finalized = true;
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    PTransform<?, ?> transform = node.getTransform();
    JetTransformTranslator<?> translator = translatorProvider.apply(transform);
    if (translator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(transform);
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    translate(node, translator);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    // do nothing here
  }

  DAG getDAG() {
    return translationContext.getDagBuilder().getDag();
  }

  private <T extends PTransform<?, ?>> void translate(
      TransformHierarchy.Node node, JetTransformTranslator<?> translator) {
    @SuppressWarnings("unchecked")
    JetTransformTranslator<T> typedTranslator = (JetTransformTranslator<T>) translator;
    Pipeline pipeline = getPipeline();
    AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);
    typedTranslator.translate(pipeline, appliedTransform, node, translationContext);
  }
}
