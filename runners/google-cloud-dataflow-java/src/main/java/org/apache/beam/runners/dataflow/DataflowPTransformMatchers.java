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
package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.toStringHelper;

import java.util.ArrayDeque;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A set of {@link PTransformMatcher PTransformMatchers} that are used in the Dataflow Runner and
 * not general enough to be shared between runners.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class DataflowPTransformMatchers {
  private DataflowPTransformMatchers() {}

  /**
   * Matches {@link PTransform}s of class {@link Combine.GroupedValues} that have no side inputs.
   */
  static class CombineValuesWithoutSideInputsPTransformMatcher implements PTransformMatcher {

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(Combine.GroupedValues.class)
          && ((Combine.GroupedValues<?, ?, ?>) application.getTransform())
              .getSideInputs()
              .isEmpty();
    }

    @Override
    public String toString() {
      return toStringHelper(CombineValuesWithoutSideInputsPTransformMatcher.class).toString();
    }
  }

  /**
   * Matches {@link PTransform}s of class {@link Combine.GroupedValues} that have no side inputs and
   * are direct subtransforms of a {@link Combine.PerKey}.
   */
  static class CombineValuesWithParentCheckPTransformMatcher implements PTransformMatcher {

    @Override
    public boolean matches(AppliedPTransform<?, ?, ?> application) {
      return application.getTransform().getClass().equals(Combine.GroupedValues.class)
          && ((Combine.GroupedValues<?, ?, ?>) application.getTransform()).getSideInputs().isEmpty()
          && parentIsCombinePerKey(application);
    }

    private boolean parentIsCombinePerKey(AppliedPTransform<?, ?, ?> application) {
      // We want the PipelineVisitor below to change the parent, but the parent must be final to
      // be captured in there. To work around this issue, wrap the parent in a one element array.
      final TransformHierarchy.Node[] parent = new TransformHierarchy.Node[1];

      // Traverse the pipeline to find the parent transform to application. Do this by maintaining
      // a stack of each composite transform being entered, and grabbing the top transform of the
      // stack once the target node is visited.
      Pipeline pipeline = application.getPipeline();
      pipeline.traverseTopologically(
          new Pipeline.PipelineVisitor.Defaults() {
            private ArrayDeque<TransformHierarchy.Node> parents = new ArrayDeque<>();

            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              CompositeBehavior behavior = CompositeBehavior.ENTER_TRANSFORM;

              // Combine.GroupedValues is a composite transform in the hierarchy, so when entering
              // a composite first we check if we found our target node.
              if (!node.isRootNode()
                  && node.toAppliedPTransform(getPipeline()).equals(application)) {
                // If we found the target node grab the node's parent.
                if (parents.isEmpty()) {
                  parent[0] = null;
                } else {
                  parent[0] = parents.peekFirst();
                }
                behavior = CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
              }

              // Even if we found the target node we must add it to the list to maintain parity
              // with the number of removeFirst calls.
              parents.addFirst(node);
              return behavior;
            }

            @Override
            public void leaveCompositeTransform(TransformHierarchy.Node node) {
              if (!node.isRootNode()) {
                parents.removeFirst();
              }
            }
          });

      if (parent[0] == null) {
        return false;
      }

      // If the parent transform cannot be converted to an appliedPTransform it's definitely not
      // a CombinePerKey.
      AppliedPTransform<?, ?, ?> appliedParent;
      try {
        appliedParent = parent[0].toAppliedPTransform(pipeline);
      } catch (NullPointerException e) {
        return false;
      }

      return appliedParent.getTransform().getClass().equals(Combine.PerKey.class);
    }

    @Override
    public String toString() {
      return toStringHelper(CombineValuesWithParentCheckPTransformMatcher.class).toString();
    }
  }
}
