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

import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link Pipeline.PipelineVisitor} for executing a {@link Pipeline} as a Flink batch job. */
class FlinkBatchPipelineTranslator extends FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkBatchPipelineTranslator.class);

  /** The necessary context in the case of a batch job. */
  private final FlinkBatchTranslationContext batchContext;

  private int depth = 0;

  public FlinkBatchPipelineTranslator(ExecutionEnvironment env, PipelineOptions options) {
    this.batchContext = new FlinkBatchTranslationContext(env, options);
  }

  @Override
  @SuppressWarnings("rawtypes, unchecked")
  public void translate(Pipeline pipeline) {
    batchContext.init(pipeline);
    super.translate(pipeline);

    // terminate dangling DataSets
    for (DataSet<?> dataSet : batchContext.getDanglingDataSets().values()) {
      dataSet.output(new DiscardingOutputFormat());
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    LOG.info("{} enterCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
    this.depth++;

    BatchTransformTranslator<?> translator = getTranslator(node, batchContext);

    if (translator != null) {
      applyBatchTransform(node.getTransform(), node, translator);
      LOG.info("{} translated- {}", genSpaces(this.depth), node.getFullName());
      return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
    } else {
      return CompositeBehavior.ENTER_TRANSFORM;
    }
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    this.depth--;
    LOG.info("{} leaveCompositeTransform- {}", genSpaces(this.depth), node.getFullName());
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.info("{} visitPrimitiveTransform- {}", genSpaces(this.depth), node.getFullName());

    // get the transformation corresponding to the node we are
    // currently visiting and translate it into its Flink alternative.
    PTransform<?, ?> transform = node.getTransform();
    BatchTransformTranslator<?> translator =
        FlinkBatchTransformTranslators.getTranslator(transform, batchContext);
    if (translator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(transform);
      throw new UnsupportedOperationException(
          "The transform " + transformUrn + " is currently not supported.");
    }
    applyBatchTransform(transform, node, translator);
  }

  private <T extends PTransform<?, ?>> void applyBatchTransform(
      PTransform<?, ?> transform,
      TransformHierarchy.Node node,
      BatchTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    BatchTransformTranslator<T> typedTranslator = (BatchTransformTranslator<T>) translator;

    // create the applied PTransform on the batchContext
    batchContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    typedTranslator.translateNode(typedTransform, batchContext);
  }

  /** A translator of a {@link PTransform}. */
  public interface BatchTransformTranslator<TransformT extends PTransform> {

    default boolean canTranslate(TransformT transform, FlinkBatchTranslationContext context) {
      return true;
    }

    void translateNode(TransformT transform, FlinkBatchTranslationContext context);
  }

  /** Returns a translator for the given node, if it is possible, otherwise null. */
  private static BatchTransformTranslator<?> getTranslator(
      TransformHierarchy.Node node, FlinkBatchTranslationContext context) {
    @Nullable PTransform<?, ?> transform = node.getTransform();

    // Root of the graph is null
    if (transform == null) {
      return null;
    }

    return FlinkBatchTransformTranslators.getTranslator(transform, context);
  }
}
