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
package org.apache.beam.sdk.runners;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private final Deque<TransformTreeNode> transformStack = new LinkedList<>();
  private final Map<PInput, TransformTreeNode> producingTransformNode = new HashMap<>();

  /**
   * Create a {@code TransformHierarchy} containing a root node.
   */
  public TransformHierarchy() {
    // First element in the stack is the root node, holding all child nodes.
    transformStack.add(new TransformTreeNode(null, null, "", null));
  }

  /**
   * Returns the last TransformTreeNode on the stack.
   */
  public TransformTreeNode getCurrent() {
    return transformStack.peek();
  }

  /**
   * Add a TransformTreeNode to the stack.
   */
  public void pushNode(TransformTreeNode current) {
    transformStack.push(current);
  }

  /**
   * Removes the last TransformTreeNode from the stack.
   */
  public void popNode() {
    transformStack.pop();
    checkState(!transformStack.isEmpty());
  }

  /**
   * Adds an input to the given node.
   *
   * <p>This forces the producing node to be finished.
   */
  public void addInput(TransformTreeNode node, PInput input) {
    for (PValue i : input.expand()) {
      TransformTreeNode producer = producingTransformNode.get(i);
      checkState(producer != null, "Producer unknown for input: %s", i);

      producer.finishSpecifying();
      node.addInputProducer(i, producer);
    }
  }

  /**
   * Sets the output of a transform node.
   */
  public void setOutput(TransformTreeNode producer, POutput output) {
    producer.setOutput(output);

    for (PValue o : output.expand()) {
      producingTransformNode.put(o, producer);
    }
  }

  /**
   * Visits all nodes in the transform hierarchy, in transitive order.
   */
  public void visit(Pipeline.PipelineVisitor visitor,
                    Set<PValue> visitedNodes) {
    transformStack.peekFirst().visit(visitor, visitedNodes);
  }
}
