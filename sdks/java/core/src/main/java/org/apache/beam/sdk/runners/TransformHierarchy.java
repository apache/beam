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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Captures information about a collection of transformations and their
 * associated {@link PValue}s.
 */
public class TransformHierarchy {
  private static final Logger LOG = LoggerFactory.getLogger(TransformHierarchy.class);

  private final Pipeline pipeline;
  private final Node root;
  private final Map<Node, PInput> unexpandedInputs;
  private final Map<POutput, Node> producers;
  // A map of PValue to the PInput the producing PTransform is applied to
  private final Map<PValue, PInput> producerInput;
  // Maintain a stack based on the enclosing nodes
  private Node current;

  public TransformHierarchy(Pipeline pipeline) {
    this.pipeline = pipeline;
    producers = new HashMap<>();
    producerInput = new HashMap<>();
    unexpandedInputs = new HashMap<>();
    root = new Node(null, null, "", null);
    current = root;
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
    unexpandedInputs.put(node, input);
    current.addComposite(node);
    current = node;
    return current;
  }

  public Node replaceNode(Node existing, PInput input, PTransform<?, ?> transform) {
    checkNotNull(existing);
    checkNotNull(input);
    checkNotNull(transform);
    checkState(
        unexpandedInputs.isEmpty(),
        "Replacing a node when the graph has an unexpanded input. This is an SDK bug.");
    Node replacement =
        new Node(existing.getEnclosingNode(), transform, existing.getFullName(), input);
    for (PValue output : existing.getOutputs().values()) {
      Node producer = producers.get(output);
      boolean producedInExisting = false;
      do {
        if (producer.equals(existing)) {
          producedInExisting = true;
        } else {
          producer = producer.getEnclosingNode();
        }
      } while (!producedInExisting && !producer.isRootNode());
      if (producedInExisting) {
        producers.remove(output);
        LOG.debug("Removed producer for value {} as it is part of a replaced composite {}",
            output,
            existing.getFullName());
      } else {
        LOG.debug(
            "Value {} not produced in existing node {}", output, existing.getFullName());
      }
    }
    existing.getEnclosingNode().replaceChild(existing, replacement);
    unexpandedInputs.remove(existing);
    unexpandedInputs.put(replacement, input);
    current = replacement;
    return replacement;
  }

  /**
   * Finish specifying all of the input {@link PValue PValues} of the current {@link
   * Node}. Ensures that all of the inputs to the current node have been fully
   * specified, and have been produced by a node in this graph.
   */
  public void finishSpecifyingInput() {
    // Inputs must be completely specified before they are consumed by a transform.
    for (PValue inputValue : current.getInputs().values()) {
      Node producerNode = getProducer(inputValue);
      PInput input = producerInput.remove(inputValue);
      inputValue.finishSpecifying(input, producerNode.getTransform());
      checkState(
          producers.get(inputValue) != null,
          "Producer unknown for input %s",
          inputValue);
      checkState(
          producers.get(inputValue) != null,
          "Producer unknown for input %s",
          inputValue);
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
    for (PValue value : output.expand().values()) {
      if (!producers.containsKey(value)) {
        producers.put(value, current);
      }
      value.finishSpecifyingOutput(unexpandedInputs.get(current), current.transform);
      producerInput.put(value, unexpandedInputs.get(current));
    }
    output.finishSpecifyingOutput(unexpandedInputs.get(current), current.transform);
    current.setOutput(output);
    // TODO: Replace with a "generateDefaultNames" method.
    output.recordAsOutput(current.toAppliedPTransform());
  }

  /**
   * Recursively replace the outputs of the current {@link Node} with the original outputs of the
   * node it is replacing. No value that is a key in {@code originalToReplacement} may be present
   * within the {@link TransformHierarchy} after this method completes.
   */
  public void replaceOutputs(Map<PValue, ReplacementOutput> originalToReplacement) {
    current.replaceOutputs(originalToReplacement);
  }

  /**
   * Pops the current node off the top of the stack, finishing it. Outputs of the node are finished
   * once they are consumed as input.
   */
  public void popNode() {
    current.finishSpecifying();
    unexpandedInputs.remove(current);
    current = current.getEnclosingNode();
    checkState(current != null, "Can't pop the root node of a TransformHierarchy");
  }

  Node getProducer(PValue produced) {
    return producers.get(produced);
  }

  public Set<PValue> visit(PipelineVisitor visitor) {
    finishSpecifying();
    Set<PValue> visitedValues = new HashSet<>();
    root.visit(visitor, visitedValues);
    return visitedValues;
  }

  /**
   * Finish specifying any remaining nodes within the {@link TransformHierarchy}. These are {@link
   * PValue PValues} that are produced as output of some {@link PTransform} but are never consumed
   * as input. These values must still be finished specifying.
   */
  private void finishSpecifying() {
    for (Entry<PValue, PInput> producerInputEntry : producerInput.entrySet()) {
      PValue value = producerInputEntry.getKey();
      value.finishSpecifying(producerInputEntry.getValue(), getProducer(value).getTransform());
    }
    producerInput.clear();
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
    private final List<Node> parts = new ArrayList<>();

    // Input to the transform, in expanded form.
    private final Map<TupleTag<?>, PValue> inputs;

    // TODO: track which outputs need to be exported to parent.
    // Output of the transform, in expanded form.
    private Map<TupleTag<?>, PValue> outputs;

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
      this.inputs = input == null ? Collections.<TupleTag<?>, PValue>emptyMap() : input.expand();
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
     * Replace a sub-node of this transform node.
     *
     * <p>The existing {@link Node} must be a direct child of this {@link Node}. The replacement
     * node need not be fully specified.
     */
    public void replaceChild(Node existing, Node replacement) {
      checkNotNull(existing);
      checkNotNull(replacement);
      int existingIndex = parts.indexOf(existing);
      checkArgument(
          existingIndex >= 0,
          "Tried to replace a node %s that doesn't exist as a component of node %s",
          existing.getFullName(),
          getFullName());
      LOG.debug(
          "Replaced original node {} with replacement {} at index {}",
          existing,
          replacement,
          existingIndex);
      parts.set(existingIndex, replacement);
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
      if (outputs != null) {
        for (PValue outputValue : outputs.values()) {
          if (!getProducer(outputValue).getTransform().equals(transform)) {
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
    public Map<TupleTag<?>, PValue> getInputs() {
      return inputs == null ? Collections.<TupleTag<?>, PValue>emptyMap() : inputs;
    }

    /**
     * Adds an output to the transform node.
     */
    private void setOutput(POutput output) {
      checkState(!finishedSpecifying);
      checkState(
          this.outputs == null, "Tried to specify more than one output for %s", getFullName());
      checkNotNull(output, "Tried to set the output of %s to null", getFullName());
      this.outputs = output.expand();

      // Validate that a primitive transform produces only primitive output, and a composite
      // transform does not produce primitive output.
      Set<Node> outputProducers = new HashSet<>();
      for (PValue outputValue : output.expand().values()) {
        outputProducers.add(getProducer(outputValue));
      }
      if (outputProducers.contains(this)) {
        if (!parts.isEmpty() || outputProducers.size() > 1) {
          Set<String> otherProducerNames = new HashSet<>();
          for (Node outputProducer : outputProducers) {
            if (outputProducer != this) {
              otherProducerNames.add(outputProducer.getFullName());
            }
          }
          throw new IllegalArgumentException(
              String.format(
                  "Output of composite transform [%s] contains a primitive %s produced by it. "
                      + "Only primitive transforms are permitted to produce primitive outputs."
                      + "%n    Outputs: %s"
                      + "%n    Other Producers: %s"
                      + "%n    Components: %s",
                  getFullName(),
                  POutput.class.getSimpleName(),
                  output.expand(),
                  otherProducerNames,
                  parts));
        }
      }
    }

    /**
     * Replaces each value in {@code originalToReplacement} present in this {@link Node Node's}
     * outputs with the key that maps to that value.
     *
     * <p>Outputs produced by this node that do not have an associated key in this map remain
     * as-is. Generally, these are nodes which are internal to a composite replacement; they should
     * not appear as outputs of the replacement composite.
     *
     * @param originalToReplacement A map from the outputs of the replacement {@link Node} to the
     *                              original output.
     */
    void replaceOutputs(Map<PValue, ReplacementOutput> originalToReplacement) {
      checkNotNull(this.outputs, "Outputs haven't been specified for node %s yet", getFullName());
      for (Node component : this.parts) {
        // Replace the outputs of the component nodes
        component.replaceOutputs(originalToReplacement);
      }
      ImmutableMap.Builder<TupleTag<?>, PValue> newOutputsBuilder = ImmutableMap.builder();
      for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
        ReplacementOutput mapping = originalToReplacement.get(output.getValue());
        if (mapping != null) {
          if (this.equals(producers.get(mapping.getReplacement().getValue()))) {
            // This Node produced the replacement PCollection. The structure of this if statement
            // requires the replacement transform to produce only new outputs; otherwise the
            // producers map will not be appropriately updated. TODO: investigate alternatives
            producerInput.remove(mapping.getReplacement().getValue());
            // original and replacement might be identical
            producers.remove(mapping.getReplacement().getValue());
            producers.put(mapping.getOriginal().getValue(), this);
          }
          LOG.debug(
              "Replacing output {} with original {}",
              mapping.getReplacement(),
              mapping.getOriginal());
          newOutputsBuilder.put(output.getKey(), mapping.getOriginal().getValue());
        } else {
          newOutputsBuilder.put(output);
        }
      }
      ImmutableMap<TupleTag<?>, PValue> newOutputs = newOutputsBuilder.build();
      checkState(
          outputs.size() == newOutputs.size(),
          "Number of outputs must be stable across replacement");
      this.outputs = newOutputs;
    }

    /** Returns the transform output, in expanded form. */
    public Map<TupleTag<?>, PValue> getOutputs() {
      return outputs == null ? Collections.<TupleTag<?>, PValue>emptyMap() : outputs;
    }

    /**
     * Returns the {@link AppliedPTransform} representing this {@link Node}.
     */
    public AppliedPTransform<?, ?, ?> toAppliedPTransform() {
      return AppliedPTransform.of(
          getFullName(), inputs, outputs, (PTransform) getTransform(), pipeline);
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
        for (PValue inputValue : inputs.values()) {
          if (visitedValues.add(inputValue)) {
            visitor.visitValue(inputValue, getProducer(inputValue));
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
        checkNotNull(outputs, "Outputs for non-root node %s are null", getFullName());
        // Visit outputs.
        for (PValue pValue : outputs.values()) {
          if (visitedValues.add(pValue)) {
            visitor.visitValue(pValue, this);
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

    @Override
    public String toString() {
      if (isRootNode()) {
        return "RootNode";
      }
      return MoreObjects.toStringHelper(getClass()).add("fullName", fullName).toString();
    }
  }
}
