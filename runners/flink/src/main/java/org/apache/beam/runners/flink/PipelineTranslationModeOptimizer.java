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
package org.apache.beam.runners.flink;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traverses the Pipeline to determine the translation mode (i.e. streaming or batch) for this
 * pipeline.
 */
class PipelineTranslationModeOptimizer extends FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineTranslationModeOptimizer.class);

  private boolean hasUnboundedCollections;

  static boolean hasUnboundedOutput(Pipeline p) {
    PipelineTranslationModeOptimizer optimizer = new PipelineTranslationModeOptimizer();
    optimizer.translate(p);
    return optimizer.hasUnboundedCollections;
  }

  private PipelineTranslationModeOptimizer() {}

  @Override
  public void translate(Pipeline pipeline) {
    super.translate(pipeline);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {}

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    AppliedPTransform<?, ?, ?> appliedPTransform = node.toAppliedPTransform(getPipeline());
    if (hasUnboundedOutput(appliedPTransform)) {
      Class<? extends PTransform> transformClass = node.getTransform().getClass();
      LOG.debug("Found unbounded PCollection for transform {}", transformClass);
      hasUnboundedCollections = true;
    }
  }

  private boolean hasUnboundedOutput(AppliedPTransform<?, ?, ?> transform) {
    return transform.getOutputs().values().stream()
        .filter(value -> value instanceof PCollection)
        .map(value -> (PCollection<?>) value)
        .anyMatch(collection -> collection.isBounded() == IsBounded.UNBOUNDED);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {}
}
