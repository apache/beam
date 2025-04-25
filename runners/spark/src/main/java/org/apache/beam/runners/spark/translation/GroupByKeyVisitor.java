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
package org.apache.beam.runners.spark.translation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Traverses the pipeline to populate the candidates for group by key. */
public class GroupByKeyVisitor extends Pipeline.PipelineVisitor.Defaults {

  protected final EvaluationContext ctxt;
  protected final SparkPipelineTranslator translator;
  private boolean isInsideCoGBK = false;
  private long visitedGroupByKeyTransformsCount = 0;

  public GroupByKeyVisitor(
      SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
    this.ctxt = evaluationContext;
    this.translator = translator;
  }

  @Override
  public Pipeline.PipelineVisitor.CompositeBehavior enterCompositeTransform(
      TransformHierarchy.Node node) {
    if (node.getTransform() != null && node.getTransform() instanceof CoGroupByKey<?>) {
      isInsideCoGBK = true;
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    if (isInsideCoGBK && node.getTransform() instanceof CoGroupByKey<?>) {
      isInsideCoGBK = false;
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {
      String urn = PTransformTranslation.urnForTransformOrNull(transform);
      if (PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN.equals(urn)) {
        visitedGroupByKeyTransformsCount += 1;
        if (!isInsideCoGBK) {
          ctxt.getCandidatesForGroupByKeyAndWindowTranslation()
              .put((GroupByKey<?, ?>) transform, node.getFullName());
        }
      }
    }
  }

  @VisibleForTesting
  long getVisitedGroupByKeyTransformsCount() {
    return visitedGroupByKeyTransformsCount;
  }
}
