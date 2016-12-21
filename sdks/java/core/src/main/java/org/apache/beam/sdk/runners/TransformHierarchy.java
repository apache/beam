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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private final Node root;
  private final Map<POutput, Node> producers;
  // Maintain a stack based on the enclosing nodes
  private Node current;

  public TransformHierarchy() {
    root = new Node(null, null, "", null);
    current = root;
    producers = new HashMap<>();
  }

  /**
   * Adds the named {@link PTransform} consuming the provided {@link PInput} as a node in this
   * {@link TransformHierarchy} as a child of the current node, and sets it to be the current node.
   *
   * <p>This call should be finished by expanding and recursively calling {@link #pushNode(String,
   * PInput, PTransform)}, calling {@link #finishSpecifyingInput()}, setting the output with {@link
   * #setOutput(POutput)}, and ending with a call to {@link #popNode()}.
   *
   * @return the added node
   */
  public Node pushNode(String name, PInput input, PTransform<?, ?> transform) {
    checkNotNull(
        transform, "A %s must be provided for all Nodes", PTransform.class.getSimpleName());
    checkNotNull(
        name, "A name must be provided for all %s Nodes", PTransform.class.getSimpleName());
    checkNotNull(
        input, "An input must be provided for all %s Nodes", PTransform.class.getSimpleName());
    Node node = new Node(current, transform, name, input);
    current.addComposite(node);
    current = node;
    return current;
  }

  /**
   * Finish specifying all of the input {@link PValue PValues} of the current {@link
   * Node}. Ensures that all of the inputs to the current node have been fully
   * specified, and have been produced by a node in this graph.
   */
  public void finishSpecifyingInput() {
    // Inputs must be completely specified before they are consumed by a transform.
    for (TaggedPValue inputValue : current.getInputs()) {
      inputValue.getValue().finishSpecifying();
      checkState(
          producers.get(inputValue.getValue()) != null,
          "Producer unknown for input %s",
          inputValue.getValue());
    }
  }

  /**
   * Set the output of the current {@link Node}. If the output is new (setOutput has
   * not previously been called with it as the parameter), the current node is set as the producer
   * of that {@link POutput}.
   *
   * <p>Also validates the output - specifically, a Primitive {@link PTransform} produces all of
   * its outputs, and a Composite {@link PTransform} produces none of its outputs. Verifies that the
   * expanded output does not contain {@link PValue PValues} produced by both this node and other
   * nodes.
   */
  public void setOutput(POutput output) {
    output.finishSpecifyingOutput();
    for (TaggedPValue value : output.expand()) {
      if (!producers.containsKey(value.getValue())) {
        producers.put(value.getValue(), current);
      }
    }
    current.setOutput(output);
    // TODO: Replace with a "generateDefaultNames" method.
    output.recordAsOutput(current.toAppliedPTransform());
  }

  /**
   * Pops the current node off the top of the stack, finishing it. Outputs of the node are finished
   * once they are consumed as input.
   */
  public void popNode() {
    current.finishSpecifying();
    current = current.getEnclosingNode();
    checkState(current != null, "Can't pop the root node of a TransformHierarchy");
  }

  Node getProducer(PValue produced) {
    return producers.get(produced);
  }

  /**
   * Returns all producing transforms for the {@link PValue PValues} contained
   * in {@code output}.
   */
  List<Node> getProducingTransforms(POutput output) {
    List<Node> producingTransforms = new ArrayList<>();
    for (TaggedPValue value : output.expand()) {
      Node producer = getProducer(value.getValue());
      if (producer != null) {
        producingTransforms.add(producer);
      }
    }
    return producingTransforms;
  }

  public Set<PValue> visit(PipelineVisitor visitor) {
    Set<PValue> visitedValues = new HashSet<>();
    root.visit(visitor, visitedValues);
    return visitedValues;
  }

  public Node getCurrent() {
    return current;
  }

  /**
   * Provides internal tracking of transform relationships with helper methods
   * for initialization and ordered visitation.
   */
  public class Node {
    private final Node enclosingNode;
    // The PTransform for this node, which may be a composite PTransform.
    // The root of a TransformHierarchy is represented as a Node
    // with a null transform field.
    private final PTransform<?, ?> transform;

    private final String fullName;

    // Nodes for sub-transforms of a composite transform.
    private final Collection<Node> parts = new ArrayList<>();

    // Input to the transform, in unexpanded form.
    private final PInput input;

    // TODO: track which outputs need to be exported to parent.
    // Output of the transform, in unexpanded form.
    private POutput output;

    @VisibleForTesting
    boolean finishedSpecifying = false;

    /**
     * Creates a new Node with the given parent and transform.
     *
     * <p>EnclosingNode and transform may both be null for a root-level node, which holds all other
     * nodes.
     *
     * @param enclosingNode the composite node containing this node
     * @param transform the PTransform tracked by this node
     * @param fullName the fully qualified name of the transform
     * @param input the unexpanded input to the transform
     */
    private Node(
        @Nullable Node enclosingNode,
        @Nullable PTransform<?, ?> transform,
        String fullName,
        @Nullable PInput input) {
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
    public Node getEnclosingNode() {
      return enclosingNode;
    }

    /**
     * Adds a composite operation to the transform node.
     *
     * <p>As soon as a node is added, the transform node is considered a
     * composite operation instead of a primitive transform.
     */
    public void addComposite(Node node) {
      parts.add(node);
    }

    /**
     * Returns true if this node represents a composite transform that does not perform processing
     * of its own, but merely encapsulates a sub-pipeline (which may be empty).
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
        for (TaggedPValue outputValue : output.expand()) {
          if (!getProducer(outputValue.getValue()).getTransform().equals(transform)) {
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

    /** Returns the transform input, in unexpanded form. */
    public List<TaggedPValue> getInputs() {
      return input == null ? Collections.<TaggedPValue>emptyList() : input.expand();
    }

    /**
     * Adds an output to the transform node.
     */
    private void setOutput(POutput output) {
      checkState(!finishedSpecifying);
      checkState(
          this.output == null, "Tried to specify more than one output for %s", getFullName());
      checkNotNull(output, "Tried to set the output of %s to null", getFullName());
      this.output = output;

      // Validate that a primitive transform produces only primitive output, and a composite
      // transform does not produce primitive output.
      Set<Node> outputProducers = new HashSet<>();
      for (TaggedPValue outputValue : output.expand()) {
        outputProducers.add(getProducer(outputValue.getValue()));
      }
      if (outputProducers.contains(this) && outputProducers.size() != 1) {
        Set<String> otherProducerNames = new HashSet<>();
        for (Node outputProducer : outputProducers) {
          if (outputProducer != this) {
            otherProducerNames.add(outputProducer.getFullName());
          }
        }
        throw new IllegalArgumentException(
            String.format(
                "Output of transform [%s] contains a %s produced by it as well as other "
                    + "Transforms. A primitive transform must produce all of its outputs, and "
                    + "outputs of a composite transform must be produced by a component transform "
                    + "or be part of the input."
                    + "%n    Other Outputs: %s"
                    + "%n    Other Producers: %s",
                getFullName(), POutput.class.getSimpleName(), output.expand(), otherProducerNames));
      }
    }

    /** Returns the transform output, in unexpanded form. */
    public List<TaggedPValue> getOutputs() {
      return output == null ? Collections.<TaggedPValue>emptyList() : output.expand();
    }

    /**
     * Returns the {@link AppliedPTransform} representing this {@link Node}.
     */
    public AppliedPTransform<?, ?, ?> toAppliedPTransform() {
      return AppliedPTransform.of(getFullName(), input, output, (PTransform) getTransform());
    }

    /**
     * Visit the transform node.
     *
     * <p>Provides an ordered visit of the input values, the primitive transform (or child nodes for
     * composite transforms), then the output values.
     */
    private void visit(PipelineVisitor visitor, Set<PValue> visitedValues) {
      if (!finishedSpecifying) {
        finishSpecifying();
      }

      if (!isRootNode()) {
        // Visit inputs.
        for (TaggedPValue inputValue : input.expand()) {
          if (visitedValues.add(inputValue.getValue())) {
            visitor.visitValue(inputValue.getValue(), getProducer(inputValue.getValue()));
          }
        }
      }

      if (isCompositeNode()) {
        PipelineVisitor.CompositeBehavior recurse = visitor.enterCompositeTransform(this);

        if (recurse.equals(CompositeBehavior.ENTER_TRANSFORM)) {
          for (Node child : parts) {
            child.visit(visitor, visitedValues);
          }
        }
        visitor.leaveCompositeTransform(this);
      } else {
        visitor.visitPrimitiveTransform(this);
      }

      if (!isRootNode()) {
        // Visit outputs.
        for (TaggedPValue pValue : output.expand()) {
          if (visitedValues.add(pValue.getValue())) {
            visitor.visitValue(pValue.getValue(), this);
          }
        }
      }
    }

    /**
     * Finish specifying a transform.
     *
     * <p>All inputs are finished first, then the transform, then all outputs.
     */
    private void finishSpecifying() {
      if (finishedSpecifying) {
        return;
      }
      finishedSpecifying = true;
    }
  }
}
