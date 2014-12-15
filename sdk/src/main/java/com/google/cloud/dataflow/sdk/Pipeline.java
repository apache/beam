/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk;

import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.TransformHierarchy;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.PValue;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Pipeline manages a DAG of PTransforms, and the PCollections
 * that the PTransforms consume and produce.
 *
 * <p> After a {@code Pipeline} has been constructed, it can be executed,
 * using a default or an explicit {@link PipelineRunner}.
 *
 * <p> Multiple {@code Pipeline}s can be constructed and executed independently
 * and concurrently.
 *
 * <p> Each {@code Pipeline} is self-contained and isolated from any other
 * {@code Pipeline}.  The {@link PValues} that are inputs and outputs of each of a
 * {@code Pipeline}'s {@link PTransform}s are also owned by that {@code Pipeline}.
 * A {@code PValue} owned by one {@code Pipeline} can be read only by {@code PTransform}s
 * also owned by that {@code Pipeline}.
 *
 * <p> Here's a typical example of use:
 * <pre> {@code
 * // Start by defining the options for the pipeline.
 * PipelineOptions options = PipelineOptionsFactory.create();
 * // Then create the pipeline.
 * Pipeline p = Pipeline.create(options);
 *
 * // A root PTransform, like TextIO.Read or Create, gets added
 * // to the Pipeline by being applied:
 * PCollection<String> lines =
 *     p.apply(TextIO.Read.from("gs://bucket/dir/file*.txt"));
 *
 * // A Pipeline can have multiple root transforms:
 * PCollection<String> moreLines =
 *     p.apply(TextIO.Read.from("gs://bucket/other/dir/file*.txt"));
 * PCollection<String> yetMoreLines =
 *     p.apply(Create.of("yet", "more", "lines")).setCoder(StringUtf8Coder.of());
 *
 * // Further PTransforms can be applied, in an arbitrary (acyclic) graph.
 * // Subsequent PTransforms (and intermediate PCollections etc.) are
 * // implicitly part of the same Pipeline.
 * PCollection<String> allLines =
 *     PCollectionList.of(lines).and(moreLines).and(yetMoreLines)
 *     .apply(new Flatten<String>());
 * PCollection<KV<String, Integer>> wordCounts =
 *     allLines
 *     .apply(ParDo.of(new ExtractWords()))
 *     .apply(new Count<String>());
 * PCollection<String> formattedWordCounts =
 *     wordCounts.apply(ParDo.of(new FormatCounts()));
 * formattedWordCounts.apply(TextIO.Write.to("gs://bucket/dir/counts.txt"));
 *
 * // PTransforms aren't executed when they're applied, rather they're
 * // just added to the Pipeline.  Once the whole Pipeline of PTransforms
 * // is constructed, the Pipeline's PTransforms can be run using a
 * // PipelineRunner.  The default PipelineRunner executes the Pipeline
 * // directly, sequentially, in this one process, which is useful for
 * // unit tests and simple experiments:
 * p.run();
 *
 * } </pre>
 */
public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

  /////////////////////////////////////////////////////////////////////////////
  // Public operations.

  /**
   * Constructs a pipeline from the provided options.
   *
   * @return The newly created pipeline.
   */
  public static Pipeline create(PipelineOptions options) {
    Pipeline pipeline = new Pipeline(PipelineRunner.fromOptions(options), options);
    LOG.debug("Creating {}", pipeline);
    return pipeline;
  }

  /**
   * Returns a {@link PBegin} owned by this Pipeline.  This is useful
   * as the input of a root PTransform such as {@code TextIO.Read} or
   * {@link com.google.cloud.dataflow.sdk.transforms.Create}.
   */
  public PBegin begin() {
    return PBegin.in(this);
  }

  /**
   * Starts using this pipeline with a root PTransform such as
   * {@code TextIO.Read} or
   * {@link com.google.cloud.dataflow.sdk.transforms.Create}.
   *
   * <P>
   * Alias for {@code begin().apply(root)}.
   */
  public <Output extends POutput> Output apply(
      PTransform<? super PBegin, Output> root) {
    return begin().apply(root);
  }

  /**
   * Runs the Pipeline.
   */
  public PipelineResult run() {
    LOG.debug("Running {} via {}", this, runner);
    try {
      return runner.run(this);
    } catch (UserCodeException e) {
      // This serves to replace the stack with one that ends here and
      // is caused by the caught UserCodeException, thereby splicing
      // out all the stack frames in between the PipelineRunner itself
      // and where the worker calls into the user's code.
      throw new RuntimeException(e.getCause());
    }
  }


  /////////////////////////////////////////////////////////////////////////////
  // Below here are operations that aren't normally called by users.

  /**
   * Returns the {@link CoderRegistry} that this Pipeline uses.
   */
  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = new CoderRegistry();
      coderRegistry.registerStandardCoders();
    }
    return coderRegistry;
  }

  /**
   * Sets the {@link CoderRegistry} that this Pipeline uses.
   */
  public void setCoderRegistry(CoderRegistry coderRegistry) {
    this.coderRegistry = coderRegistry;
  }

  /**
   * A PipelineVisitor can be passed into
   * {@link Pipeline#traverseTopologically} to be called for each of the
   * transforms and values in the Pipeline.
   */
  public interface PipelineVisitor {
    public void enterCompositeTransform(TransformTreeNode node);
    public void leaveCompositeTransform(TransformTreeNode node);
    public void visitTransform(TransformTreeNode node);
    public void visitValue(PValue value, TransformTreeNode producer);
  }

  /**
   * Invokes the PipelineVisitor's
   * {@link PipelineVisitor#visitTransform} and
   * {@link PipelineVisitor#visitValue} operations on each of this
   * Pipeline's PTransforms and PValues, in forward
   * topological order.
   *
   * <p> Traversal of the pipeline causes PTransform and PValue instances to
   * be marked as finished, at which point they may no longer be modified.
   *
   * <p> Typically invoked by {@link PipelineRunner} subclasses.
   */
  public void traverseTopologically(PipelineVisitor visitor) {
    Set<PValue> visitedValues = new HashSet<>();
    // Visit all the transforms, which should implicitly visit all the values.
    transforms.visit(visitor, visitedValues);
    if (!visitedValues.containsAll(values)) {
      throw new RuntimeException(
          "internal error: should have visited all the values "
          + "after visiting all the transforms");
    }
  }

  /**
   * Applies the given PTransform to the given Input,
   * and returns its Output.
   *
   * <p> Called by PInput subclasses in their {@code apply} methods.
   */
  public static <Input extends PInput, Output extends POutput>
  Output applyTransform(Input input,
                        PTransform<? super Input, Output> transform) {
    return input.getPipeline().applyInternal(input, transform);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Below here are internal operations, never called by users.

  private final PipelineRunner<?> runner;
  private final PipelineOptions options;
  private final TransformHierarchy transforms = new TransformHierarchy();
  private Collection<PValue> values = new ArrayList<>();
  private Set<String> usedFullNames = new HashSet<>();
  private CoderRegistry coderRegistry;

  @Deprecated
  protected Pipeline(PipelineRunner<?> runner) {
    this(runner, PipelineOptionsFactory.create());
  }

  protected Pipeline(PipelineRunner<?> runner, PipelineOptions options) {
    this.runner = runner;
    this.options = options;
  }

  @Override
  public String toString() { return "Pipeline#" + hashCode(); }

  /**
   * Applies a transformation to the given input.
   *
   * @see Pipeline#apply
   */
  private <Input extends PInput, Output extends POutput>
  Output applyInternal(Input input,
      PTransform<? super Input, Output> transform) {
    input.finishSpecifying();

    TransformTreeNode parent = transforms.getCurrent();
    String namePrefix = parent.getFullName();
    String fullName = uniquifyInternal(namePrefix, transform.getName());
    TransformTreeNode child = new TransformTreeNode(parent, transform, fullName, input);
    parent.addComposite(child);

    transforms.addInput(child, input);

    transform.setPipeline(this);
    LOG.debug("Adding {} to {}", transform, this);
    try {
      transforms.pushNode(child);
      Output output = runner.apply(transform, input);
      transforms.setOutput(child, output);

      // recordAsOutput is a NOOP if already called;
      output.recordAsOutput(this, child.getTransform());
      verifyOutputState(output, child);
      return output;
    } finally {
      transforms.popNode();
    }
  }

  /**
   * Returns all producing transforms for the {@link PValue}s contained
   * in {@code output}.
   */
  private List<PTransform<?, ?>> getProducingTransforms(POutput output) {
    List<PTransform<?, ?>> producingTransforms = new ArrayList<>();
    for (PValue value : output.expand()) {
      PTransform<?, ?> transform = value.getProducingTransformInternal();
      if (transform != null) {
        producingTransforms.add(transform);
      }
    }
    return producingTransforms;
  }

  /**
   * Verifies that the output of a PTransform is correctly defined.
   *
   * <p> A non-composite transform must have all
   * of its outputs registered as produced by the transform.
   */
  private void verifyOutputState(POutput output, TransformTreeNode node) {
    if (!node.isCompositeNode()) {
      PTransform<?, ?> thisTransform = node.getTransform();
      List<PTransform<?, ?>> producingTransforms = getProducingTransforms(output);
      for (PTransform<?, ?> producingTransform : producingTransforms) {
        if (thisTransform != producingTransform) {
          throw new IllegalArgumentException("Output of non-composite transform "
              + thisTransform + " is registered as being produced by"
              + " a different transform: " + producingTransform);
        }
      }
    }
  }

  /**
   * Returns the configured pipeline runner.
   */
  public PipelineRunner<?> getRunner() {
    return runner;
  }

  /**
   * Returns the configured pipeline options.
   */
  public PipelineOptions getOptions() {
    return options;
  }

  /**
   * Returns the output associated with a transform.
   *
   * @throws IllegalStateException if the transform has not been applied to the pipeline.
   */
  public POutput getOutput(PTransform<?, ?> transform) {
    TransformTreeNode node = transforms.getNode(transform);
    Preconditions.checkState(node != null,
                             "Unknown transform: " + transform);
    return node.getOutput();
  }

  /**
   * Returns the input associated with a transform.
   *
   * @throws IllegalStateException if the transform has not been applied to the pipeline.
   */
  public PInput getInput(PTransform<?, ?> transform) {
    TransformTreeNode node = transforms.getNode(transform);
    Preconditions.checkState(node != null,
                             "Unknown transform: " + transform);
    return node.getInput();
  }

  /**
   * Returns the fully qualified name of a transform.
   *
   * @throws IllegalStateException if the transform has not been applied to the pipeline.
   */
  public String getFullName(PTransform<?, ?> transform) {
    TransformTreeNode node = transforms.getNode(transform);
    Preconditions.checkState(node != null,
                             "Unknown transform: " + transform);
    return node.getFullName();
  }

  /**
   * Returns a unique name for a transform with the given prefix (from
   * enclosing transforms) and initial name.
   *
   * <p> For internal use only.
   */
  private String uniquifyInternal(String namePrefix, String origName) {
    String name = origName;
    int suffixNum = 2;
    while (true) {
      String candidate = namePrefix.isEmpty() ? name : namePrefix + "/" + name;
      if (usedFullNames.add(candidate)) {
        return candidate;
      }
      // A duplicate!  Retry.
      name = origName + suffixNum++;
    }
  }

  /**
   * Adds the given PValue to this Pipeline.
   *
   * <p> For internal use only.
   */
  public void addValueInternal(PValue value) {
    this.values.add(value);
    value.setPipelineInternal(this);
    LOG.debug("Adding {} to {}", value, this);
  }
}
