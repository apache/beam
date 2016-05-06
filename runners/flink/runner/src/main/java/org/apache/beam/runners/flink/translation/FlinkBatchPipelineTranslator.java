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
package org.apache.beam.runners.flink.translation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.values.PValue;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FlinkBatchPipelineTranslator knows how to translate Pipeline objects into Flink Jobs.
 * This is based on {@link org.apache.beam.runners.dataflow.DataflowPipelineTranslator}
 */
public class FlinkBatchPipelineTranslator extends FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkBatchPipelineTranslator.class);

  /**
   * The necessary context in the case of a batch job.
   */
  private final FlinkBatchTranslationContext batchContext;

  private int depth = 0;

  /**
   * Composite transform that we want to translate before proceeding with other transforms.
   */
  private PTransform<?, ?> currentCompositeTransform;

  public FlinkBatchPipelineTranslator(ExecutionEnvironment env, PipelineOptions options) {
    this.batchContext = new FlinkBatchTranslationContext(env, options);
  }

  @Override
  @SuppressWarnings("rawtypes, unchecked")
  public void translate(Pipeline pipeline) {
    super.translate(pipeline);

    // terminate dangling DataSets
    for (DataSet<?> dataSet: batchContext.getDanglingDataSets().values()) {
      dataSet.output(new DiscardingOutputFormat());
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Pipeline Visitor Methods
  // --------------------------------------------------------------------------------------------

  @Override
  public void enterCompositeTransform(TransformTreeNode node) {
    LOG.info(genSpaces(this.depth) + "enterCompositeTransform- " + formatNodeName(node));

    PTransform<?, ?> transform = node.getTransform();
    if (transform != null && currentCompositeTransform == null) {

      BatchTransformTranslator<?> translator = FlinkBatchTransformTranslators.getTranslator(transform);
      if (translator != null) {
        currentCompositeTransform = transform;
        if (transform instanceof CoGroupByKey && node.getInput().expand().size() != 2) {
          // we can only optimize CoGroupByKey for input size 2
          currentCompositeTransform = null;
        }
      }
    }
    this.depth++;
  }

  @Override
  public void leaveCompositeTransform(TransformTreeNode node) {
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null && currentCompositeTransform == transform) {

      BatchTransformTranslator<?> translator = FlinkBatchTransformTranslators.getTranslator(transform);
      if (translator != null) {
        LOG.info(genSpaces(this.depth) + "doingCompositeTransform- " + formatNodeName(node));
        applyBatchTransform(transform, node, translator);
        currentCompositeTransform = null;
      } else {
        throw new IllegalStateException("Attempted to translate composite transform " +
            "but no translator was found: " + currentCompositeTransform);
      }
    }
    this.depth--;
    LOG.info(genSpaces(this.depth) + "leaveCompositeTransform- " + formatNodeName(node));
  }

  @Override
  public void visitTransform(TransformTreeNode node) {
    LOG.info(genSpaces(this.depth) + "visitTransform- " + formatNodeName(node));
    if (currentCompositeTransform != null) {
      // ignore it
      return;
    }

    // get the transformation corresponding to hte node we are
    // currently visiting and translate it into its Flink alternative.

    PTransform<?, ?> transform = node.getTransform();
    BatchTransformTranslator<?> translator = FlinkBatchTransformTranslators.getTranslator(transform);
    if (translator == null) {
      LOG.info(node.getTransform().getClass().toString());
      throw new UnsupportedOperationException("The transform " + transform + " is currently not supported.");
    }
    applyBatchTransform(transform, node, translator);
  }

  @Override
  public void visitValue(PValue value, TransformTreeNode producer) {
    // do nothing here
  }

  private <T extends PTransform<?, ?>> void applyBatchTransform(PTransform<?, ?> transform, TransformTreeNode node, BatchTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    T typedTransform = (T) transform;

    @SuppressWarnings("unchecked")
    BatchTransformTranslator<T> typedTranslator = (BatchTransformTranslator<T>) translator;

    // create the applied PTransform on the batchContext
    batchContext.setCurrentTransform(AppliedPTransform.of(
        node.getFullName(), node.getInput(), node.getOutput(), (PTransform) transform));
    typedTranslator.translateNode(typedTransform, batchContext);
  }

  /**
   * A translator of a {@link PTransform}.
   */
  public interface BatchTransformTranslator<Type extends PTransform> {
    void translateNode(Type transform, FlinkBatchTranslationContext context);
  }

  private static String genSpaces(int n) {
    String s = "";
    for (int i = 0; i < n; i++) {
      s += "|   ";
    }
    return s;
  }

  private static String formatNodeName(TransformTreeNode node) {
    return node.toString().split("@")[1] + node.getTransform();
  }
}
