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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * Provides internal tracking of transform relationships with helper methods
 * for initialization and ordered visitation.
 */
public class TransformTreeNode {
  private final TransformTreeNode enclosingNode;

  // The PTransform for this node, which may be a composite PTransform.
  // The root of a TransformHierarchy is represented as a TransformTreeNode
  // with a null transform field.
  private final PTransform<?, ?> transform;

  private final String fullName;

  // Nodes for sub-transforms of a composite transform.
  private final Collection<TransformTreeNode> parts = new ArrayList<>();

  // Inputs to the transform, in expanded form and mapped to the producer
  // of the input.
  private final Map<PValue, TransformTreeNode> inputs = new HashMap<>();

  // Input to the transform, in unexpanded form.
  private final PInput input;

  // TODO: track which outputs need to be exported to parent.
  // Output of the transform, in unexpanded form.
  private POutput output;

  private boolean finishedSpecifying = false;

  /**
   * Creates a new TransformTreeNode with the given parent and transform.
   *
   * <p>EnclosingNode and transform may both be null for
   * a root-level node, which holds all other nodes.
   *
   * @param enclosingNode the composite node containing this node
   * @param transform the PTransform tracked by this node
   * @param fullName the fully qualified name of the transform
   * @param input the unexpanded input to the transform
   */
  public TransformTreeNode(@Nullable TransformTreeNode enclosingNode,
                           @Nullable PTransform<?, ?> transform,
                           String fullName,
                           @Nullable PInput input) {
    this.enclosingNode = enclosingNode;
    this.transform = transform;
    checkArgument((enclosingNode == null && transform == null)
        || (enclosingNode != null && transform != null),
        "EnclosingNode and transform must both be specified, or both be null");
    this.fullName = fullName;
    this.input = input;
  }

  /**
   * Returns the transform associated with this transform node.
   */
  public PTransform<?, ?> getTransform() {
    return transform;
  }

  /**
   * Returns the enclosing composite transform node, or null if there is none.
   */
  public TransformTreeNode getEnclosingNode() {
    return enclosingNode;
  }

  /**
   * Adds a composite operation to the transform node.
   *
   * <p>As soon as a node is added, the transform node is considered a
   * composite operation instead of a primitive transform.
   */
  public void addComposite(TransformTreeNode node) {
    parts.add(node);
  }

  /**
   * Returns true if this node represents a composite transform that does not perform
   * processing of its own, but merely encapsulates a sub-pipeline (which may be empty).
   *
   * <p>Note that a node may be composite with no sub-transforms if it  returns its input directly
   * extracts a component of a tuple, or other operations that occur at pipeline assembly time.
   */
  public boolean isCompositeNode() {
    return !parts.isEmpty() || returnsOthersOutput() || isRootNode();
  }

  private boolean returnsOthersOutput() {
    PTransform<?, ?> transform = getTransform();
    for (PValue output : getExpandedOutputs()) {
      if (!output.getProducingTransformInternal().getTransform().equals(transform)) {
        return true;
      }
    }
    return false;
  }

  public boolean isRootNode() {
    return transform == null;
  }

  public String getFullName() {
    return fullName;
  }

  /**
   * Adds an input to the transform node.
   */
  public void addInputProducer(PValue expandedInput, TransformTreeNode producer) {
    checkState(!finishedSpecifying);
    inputs.put(expandedInput, producer);
  }

  /**
   * Returns the transform input, in unexpanded form.
   */
  public PInput getInput() {
    return input;
  }

  /**
   * Returns a mapping of inputs to the producing nodes for all inputs to
   * the transform.
   */
  public Map<PValue, TransformTreeNode> getInputs() {
    return Collections.unmodifiableMap(inputs);
  }

  /**
   * Adds an output to the transform node.
   */
  public void setOutput(POutput output) {
    checkState(!finishedSpecifying);
    checkState(this.output == null);
    this.output = output;
  }

  /**
   * Returns the transform output, in unexpanded form.
   */
  public POutput getOutput() {
    return output;
  }

  /**
   * Returns the transform outputs, in expanded form.
   */
  public Collection<? extends PValue> getExpandedOutputs() {
    if (output != null) {
      return output.expand();
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Visit the transform node.
   *
   * <p>Provides an ordered visit of the input values, the primitive
   * transform (or child nodes for composite transforms), then the
   * output values.
   */
  public void visit(PipelineVisitor visitor,
                    Set<PValue> visitedValues) {
    if (!finishedSpecifying) {
      finishSpecifying();
    }

    // Visit inputs.
    for (Map.Entry<PValue, TransformTreeNode> entry : inputs.entrySet()) {
      if (visitedValues.add(entry.getKey())) {
        visitor.visitValue(entry.getKey(), entry.getValue());
      }
    }

    if (isCompositeNode()) {
      PipelineVisitor.CompositeBehavior recurse = visitor.enterCompositeTransform(this);

      if (recurse.equals(CompositeBehavior.ENTER_TRANSFORM)) {
        for (TransformTreeNode child : parts) {
          child.visit(visitor, visitedValues);
        }
      }
      visitor.leaveCompositeTransform(this);
    } else {
      visitor.visitPrimitiveTransform(this);
    }

    // Visit outputs.
    for (PValue pValue : getExpandedOutputs()) {
      if (visitedValues.add(pValue)) {
        visitor.visitValue(pValue, this);
      }
    }
  }

  /**
   * Finish specifying a transform.
   *
   * <p>All inputs are finished first, then the transform, then
   * all outputs.
   */
  public void finishSpecifying() {
    if (finishedSpecifying) {
      return;
    }
    finishedSpecifying = true;

    for (TransformTreeNode input : inputs.values()) {
      if (input != null) {
        input.finishSpecifying();
      }
    }

    if (output != null) {
      output.finishSpecifyingOutput();
    }
  }
}
