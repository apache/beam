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

package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.PAssert;

/**
 * A {@link PipelineVisitor} that counts the number of total {@link PAssert PAsserts} in a
 * {@link Pipeline}.
 */
public class AssertionCountingVisitor extends PipelineVisitor.Defaults {

  public static AssertionCountingVisitor create() {
    return new AssertionCountingVisitor();
  }

  private int assertCount;
  private boolean pipelineVisited;

  private AssertionCountingVisitor() {
    assertCount = 0;
    pipelineVisited = false;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(Node node) {
    if (node.isRootNode()) {
      checkState(
          !pipelineVisited,
          "Tried to visit a pipeline with an already used %s",
          AssertionCountingVisitor.class.getSimpleName());
    }
    if (!node.isRootNode()
        && (node.getTransform() instanceof PAssert.OneSideInputAssert
            || node.getTransform() instanceof PAssert.GroupThenAssert
            || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton)) {
      assertCount++;
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  public void leaveCompositeTransform(Node node) {
    if (node.isRootNode()) {
      pipelineVisited = true;
    }
  }

  @Override
  public void visitPrimitiveTransform(Node node) {
    if
        (node.getTransform() instanceof PAssert.OneSideInputAssert
        || node.getTransform() instanceof PAssert.GroupThenAssert
        || node.getTransform() instanceof PAssert.GroupThenAssertForSingleton) {
      assertCount++;
    }
  }

  /**
   * Gets the number of {@link PAssert PAsserts} in the pipeline.
   */
  public int getPAssertCount() {
    checkState(pipelineVisited);
    return assertCount;
  }
}
