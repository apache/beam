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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

/**
 * Provides internal tracking of transform relationships with helper methods
 * for initialization and ordered visitation.
 */
public class TransformTreeNode {
  private final TransformHierarchy hierarchy;
  private final TransformTreeNode enclosingNode;

  // The PTransform for this node, which may be a composite PTransform.
  // The root of a TransformHierarchy is represented as a TransformTreeNode
  // with a null transform field.
  private final PTransform<?, ?> transform;

  private final String fullName;

  // Nodes for sub-transforms of a composite transform.
  private final Collection<TransformTreeNode> parts = new ArrayList<>();

  // Input to the transform, in unexpanded form.
  private final PInput input;

  // TODO: track which outputs need to be exported to parent.
  // Output of the transform, in unexpanded form.
  private POutput output;

  @VisibleForTesting
  boolean finishedSpecifying = false;

  public static TransformTreeNode root(TransformHierarchy hierarchy) {
    return new TransformTreeNode(hierarchy, null, null, "", null);
  }

  public static TransformTreeNode subtransform(
      TransformTreeNode enclosing, PTransform<?, ?> transform, String fullName, PInput input) {
    checkNotNull(enclosing);
    checkNotNull(transform);
    checkNotNull(fullName);
    checkNotNull(input);
    return new TransformTreeNode(enclosing.hierarchy, enclosing, transform, fullName, input);
  }

  /**
   * Creates a new TransformTreeNode with the given parent and transform.
   *
   * <p>EnclosingNode and transform may both be null for a root-level node, which holds all other
   * nodes.
   *
   * @param enclosingNode the composite node containing this node
   * @param transform the PTransform tracked by this node
   * @param fullName the fully qualified name of the transform
   * @param input the unexpanded input to the transform
   */
  private TransformTreeNode(
      TransformHierarchy hierarchy,
      @Nullable TransformTreeNode enclosingNode,
      @Nullable PTransform<?, ?> transform,
      String fullName,
      @Nullable PInput input) {
    this.hierarchy = hierarchy;
    this.enclosingNode = enclosingNode;
    this.transform = transform;
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
   * Returns true if this node represents a composite transform that does not perform processing of
   * its own, but merely encapsulates a sub-pipeline (which may be empty).
   *
   * <p>Note that a node may be composite with no sub-transforms if it returns its input directly
   * extracts a component of a tuple, or other operations that occur at pipeline assembly time.
   */
  public boolean isCompositeNode() {
    return !parts.isEmpty() || isRootNode() || returnsOthersOutput();
  }

  private boolean returnsOthersOutput() {
    PTransform<?, ?> transform = getTransform();
    if (output != null) {
      for (PValue outputValue : output.expand()) {
        if (!hierarchy.getProducer(outputValue).getTransform().equals(transform)) {
          return true;
        }
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
   * Returns the transform input, in unexpanded form.
   */
  public PInput getInput() {
    return input;
  }

  /**
   * Adds an output to the transform node.
   */
  public void setOutput(POutput output) {
    checkState(!finishedSpecifying);
    checkState(this.output == null, "Tried to specify more than one output for %s", getFullName());
    checkNotNull(output, "Tried to set the output of %s to null", getFullName());
    this.output = output;

    // Validate that a primitive transform produces only primitive output, and a composite transform
    // does not produce primitive output.
    Set<TransformTreeNode> outputProducers = new HashSet<>();
    for (PValue outputValue : output.expand()) {
      outputProducers.add(hierarchy.getProducer(outputValue));
    }
    if (outputProducers.contains(this) && outputProducers.size() != 1) {
      Set<String> otherProducerNames = new HashSet<>();
      for (TransformTreeNode outputProducer : outputProducers) {
        if (outputProducer != this) {
          otherProducerNames.add(outputProducer.getFullName());
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Output of transform [%s] contains a %s produced by it as well as other Transforms. "
                  + "A primitive transform must produce all of its outputs, and outputs of a "
                  + "composite transform must be produced by a component transform or be part of"
                  + "the input."
                  + "%n    Other Outputs: %s"
                  + "%n    Other Producers: %s",
              getFullName(), POutput.class.getSimpleName(), output.expand(), otherProducerNames));
    }
  }

  /**
   * Returns the transform output, in unexpanded form.
   */
  public POutput getOutput() {
    return output;
  }

  AppliedPTransform<?, ?, ?> toAppliedPTransform() {
    return AppliedPTransform.of(
        getFullName(), getInput(), getOutput(), (PTransform) getTransform());
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

    if (!isRootNode()) {
      // Visit inputs.
      for (PValue inputValue : input.expand()) {
        if (visitedValues.add(inputValue)) {
          visitor.visitValue(inputValue, hierarchy.getProducer(inputValue));
        }
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

    if (!isRootNode()) {
      // Visit outputs.
      for (PValue pValue : output.expand()) {
        if (visitedValues.add(pValue)) {
          visitor.visitValue(pValue, this);
        }
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
  }
}
