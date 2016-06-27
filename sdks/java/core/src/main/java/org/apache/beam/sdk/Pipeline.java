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
package org.apache.beam.sdk;

import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformTreeNode;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link Pipeline} manages a directed acyclic graph of {@link PTransform PTransforms}, and the
 * {@link PCollection PCollections} that the {@link PTransform}s consume and produce.
 *
 * <p>A {@link Pipeline} is initialized with a {@link PipelineRunner} that will later
 * execute the {@link Pipeline}.
 *
 * <p>{@link Pipeline Pipelines} are independent, so they can be constructed and executed
 * concurrently.
 *
 * <p>Each {@link Pipeline} is self-contained and isolated from any other
 * {@link Pipeline}. The {@link PValue PValues} that are inputs and outputs of each of a
 * {@link Pipeline Pipeline's} {@link PTransform PTransforms} are also owned by that
 * {@link Pipeline}. A {@link PValue} owned by one {@link Pipeline} can be read only by
 * {@link PTransform PTransforms} also owned by that {@link Pipeline}.
 *
 * <p>Here is a typical example of use:
 * <pre> {@code
 * // Start by defining the options for the pipeline.
 * PipelineOptions options = PipelineOptionsFactory.create();
 * // Then create the pipeline. The runner is determined by the options.
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
 *     p.apply(Create.of("yet", "more", "lines").withCoder(StringUtf8Coder.of()));
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

  /**
   * Thrown during execution of a {@link Pipeline}, whenever user code within that
   * {@link Pipeline} throws an exception.
   *
   * <p>The original exception thrown by user code may be retrieved via {@link #getCause}.
   */
  public static class PipelineExecutionException extends RuntimeException {
    /**
     * Wraps {@code cause} into a {@link PipelineExecutionException}.
     */
    public PipelineExecutionException(Throwable cause) {
      super(cause);
    }
  }

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
   * as the input of a root PTransform such as {@link Read} or
   * {@link Create}.
   */
  public PBegin begin() {
    return PBegin.in(this);
  }

  /**
   * Like {@link #apply(String, PTransform)} but the transform node in the {@link Pipeline}
   * graph will be named according to {@link PTransform#getName}.
   *
   * @see #apply(String, PTransform)
   */
  public <OutputT extends POutput> OutputT apply(
      PTransform<? super PBegin, OutputT> root) {
    return begin().apply(root);
  }

  /**
   * Adds a root {@link PTransform}, such as {@link Read} or {@link Create},
   * to this {@link Pipeline}.
   *
   * <p>The node in the {@link Pipeline} graph will use the provided {@code name}.
   * This name is used in various places, including the monitoring UI, logging,
   * and to stably identify this node in the {@link Pipeline} graph upon update.
   *
   * <p>Alias for {@code begin().apply(name, root)}.
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PBegin, OutputT> root) {
    return begin().apply(name, root);
  }

  /**
   * Runs the {@link Pipeline} using its {@link PipelineRunner}.
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
      throw new PipelineExecutionException(e.getCause());
    }
  }


  /////////////////////////////////////////////////////////////////////////////
  // Below here are operations that aren't normally called by users.

  /**
   * Returns the {@link CoderRegistry} that this {@link Pipeline} uses.
   */
  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = new CoderRegistry();
      coderRegistry.registerStandardCoders();
    }
    return coderRegistry;
  }

  /**
   * Sets the {@link CoderRegistry} that this {@link Pipeline} uses.
   */
  public void setCoderRegistry(CoderRegistry coderRegistry) {
    this.coderRegistry = coderRegistry;
  }

  /**
   * A {@link PipelineVisitor} can be passed into
   * {@link Pipeline#traverseTopologically} to be called for each of the
   * transforms and values in the {@link Pipeline}.
   */
  public interface PipelineVisitor {
    /**
     * Called for each composite transform after all topological predecessors have been visited
     * but before any of its component transforms.
     *
     * <p>The return value controls whether or not child transforms are visited.
     */
    public CompositeBehavior enterCompositeTransform(TransformTreeNode node);

    /**
     * Called for each composite transform after all of its component transforms and their outputs
     * have been visited.
     */
    public void leaveCompositeTransform(TransformTreeNode node);

    /**
     * Called for each primitive transform after all of its topological predecessors
     * and inputs have been visited.
     */
    public void visitPrimitiveTransform(TransformTreeNode node);

    /**
     * Called for each value after the transform that produced the value has been
     * visited.
     */
    public void visitValue(PValue value, TransformTreeNode producer);

    /**
     * Control enum for indicating whether or not a traversal should process the contents of
     * a composite transform or not.
     */
    public enum CompositeBehavior {
      ENTER_TRANSFORM,
      DO_NOT_ENTER_TRANSFORM;
    }

    /**
     * Default no-op {@link PipelineVisitor} that enters all composite transforms.
     * User implementations can override just those methods they are interested in.
     */
    public class Defaults implements PipelineVisitor {
      @Override
      public CompositeBehavior enterCompositeTransform(TransformTreeNode node) {
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void leaveCompositeTransform(TransformTreeNode node) { }

      @Override
      public void visitPrimitiveTransform(TransformTreeNode node) { }

      @Override
      public void visitValue(PValue value, TransformTreeNode producer) { }
    }
  }

  /**
   * Invokes the {@link PipelineVisitor PipelineVisitor's}
   * {@link PipelineVisitor#visitPrimitiveTransform} and
   * {@link PipelineVisitor#visitValue} operations on each of this
   * {@link Pipeline Pipeline's} transform and value nodes, in forward
   * topological order.
   *
   * <p>Traversal of the {@link Pipeline} causes {@link PTransform PTransforms} and
   * {@link PValue PValues} owned by the {@link Pipeline} to be marked as finished,
   * at which point they may no longer be modified.
   *
   * <p>Typically invoked by {@link PipelineRunner} subclasses.
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
   * Like {@link #applyTransform(String, PInput, PTransform)} but defaulting to the name
   * provided by the {@link PTransform}.
   */
  public static <InputT extends PInput, OutputT extends POutput>
  OutputT applyTransform(InputT input,
      PTransform<? super InputT, OutputT> transform) {
    return input.getPipeline().applyInternal(transform.getName(), input, transform);
  }

  /**
   * Applies the given {@code PTransform} to this input {@code InputT} and returns
   * its {@code OutputT}. This uses {@code name} to identify this specific application
   * of the transform. This name is used in various places, including the monitoring UI,
   * logging, and to stably identify this application node in the {@link Pipeline} graph during
   * update.
   *
   * <p>Each {@link PInput} subclass that provides an {@code apply} method should delegate to
   * this method to ensure proper registration with the {@link PipelineRunner}.
   */
  public static <InputT extends PInput, OutputT extends POutput>
  OutputT applyTransform(String name, InputT input,
      PTransform<? super InputT, OutputT> transform) {
    return input.getPipeline().applyInternal(name, input, transform);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Below here are internal operations, never called by users.

  private final PipelineRunner<?> runner;
  private final PipelineOptions options;
  private final TransformHierarchy transforms = new TransformHierarchy();
  private Collection<PValue> values = new ArrayList<>();
  private Set<String> usedFullNames = new HashSet<>();
  private CoderRegistry coderRegistry;
  private Multimap<PTransform<?, ?>, AppliedPTransform<?, ?, ?>> transformApplicationsForTesting =
      HashMultimap.create();

  /**
   * @deprecated replaced by {@link #Pipeline(PipelineRunner, PipelineOptions)}
   */
  @Deprecated
  protected Pipeline(PipelineRunner<?> runner) {
    this(runner, PipelineOptionsFactory.create());
  }

  protected Pipeline(PipelineRunner<?> runner, PipelineOptions options) {
    this.runner = runner;
    this.options = options;
  }

  @Override
  public String toString() {
    return "Pipeline#" + hashCode();
  }

  /**
   * Applies a {@link PTransform} to the given {@link PInput}.
   *
   * @see Pipeline#apply
   */
  private <InputT extends PInput, OutputT extends POutput>
  OutputT applyInternal(String name, InputT input,
      PTransform<? super InputT, OutputT> transform) {
    input.finishSpecifying();

    TransformTreeNode parent = transforms.getCurrent();
    String namePrefix = parent.getFullName();
    String fullName = uniquifyInternal(namePrefix, name);

    boolean nameIsUnique = fullName.equals(buildName(namePrefix, name));

    if (!nameIsUnique) {
      switch (getOptions().getStableUniqueNames()) {
        case OFF:
          break;
        case WARNING:
          LOG.warn("Transform {} does not have a stable unique name. "
              + "This will prevent updating of pipelines.", fullName);
          break;
        case ERROR:
          throw new IllegalStateException(
              "Transform " + fullName + " does not have a stable unique name. "
              + "This will prevent updating of pipelines.");
        default:
          throw new IllegalArgumentException(
              "Unrecognized value for stable unique names: " + getOptions().getStableUniqueNames());
      }
    }

    TransformTreeNode child =
        new TransformTreeNode(parent, transform, fullName, input);
    parent.addComposite(child);

    transforms.addInput(child, input);

    LOG.debug("Adding {} to {}", transform, this);
    try {
      transforms.pushNode(child);
      transform.validate(input);
      OutputT output = runner.apply(transform, input);
      transforms.setOutput(child, output);

      AppliedPTransform<?, ?, ?> applied = AppliedPTransform.of(
          child.getFullName(), input, output, transform);
      transformApplicationsForTesting.put(transform, applied);
      // recordAsOutput is a NOOP if already called;
      output.recordAsOutput(applied);
      verifyOutputState(output, child);
      return output;
    } finally {
      transforms.popNode();
    }
  }

  /**
   * Returns all producing transforms for the {@link PValue PValues} contained
   * in {@code output}.
   */
  private List<AppliedPTransform<?, ?, ?>> getProducingTransforms(POutput output) {
    List<AppliedPTransform<?, ?, ?>> producingTransforms = new ArrayList<>();
    for (PValue value : output.expand()) {
      AppliedPTransform<?, ?, ?> transform = value.getProducingTransformInternal();
      if (transform != null) {
        producingTransforms.add(transform);
      }
    }
    return producingTransforms;
  }

  /**
   * Verifies that the output of a {@link PTransform} is correctly configured in its
   * {@link TransformTreeNode} in the {@link Pipeline} graph.
   *
   * <p>A non-composite {@link PTransform} must have all
   * of its outputs registered as produced by that {@link PTransform}.
   *
   * <p>A composite {@link PTransform} must have all of its outputs
   * registered as produced by the contained primitive {@link PTransform PTransforms}.
   * They have each had the above check performed already, when
   * they were applied, so the only possible failure state is
   * that the composite {@link PTransform} has returned a primitive output.
   */
  private void verifyOutputState(POutput output, TransformTreeNode node) {
    if (!node.isCompositeNode()) {
      PTransform<?, ?> thisTransform = node.getTransform();
      List<AppliedPTransform<?, ?, ?>> producingTransforms = getProducingTransforms(output);
      for (AppliedPTransform<?, ?, ?> producingTransform : producingTransforms) {
        // Using != because object identity indicates that the transforms
        // are the same node in the pipeline
        if (thisTransform != producingTransform.getTransform()) {
          throw new IllegalArgumentException("Output of non-composite transform "
              + thisTransform + " is registered as being produced by"
              + " a different transform: " + producingTransform);
        }
      }
    } else {
      PTransform<?, ?> thisTransform = node.getTransform();
      List<AppliedPTransform<?, ?, ?>> producingTransforms = getProducingTransforms(output);
      for (AppliedPTransform<?, ?, ?> producingTransform : producingTransforms) {
        // Using == because object identity indicates that the transforms
        // are the same node in the pipeline
        if (thisTransform == producingTransform.getTransform()) {
          throw new IllegalStateException("Output of composite transform "
              + thisTransform + " is registered as being produced by it,"
              + " but the output of every composite transform should be"
              + " produced by a primitive transform contained therein.");
        }
      }
    }
  }

  /**
   * Returns the configured {@link PipelineRunner}.
   */
  public PipelineRunner<?> getRunner() {
    return runner;
  }

  /**
   * Returns the configured {@link PipelineOptions}.
   */
  public PipelineOptions getOptions() {
    return options;
  }

  /**
   * @deprecated this method is no longer compatible with the design of {@link Pipeline},
   * as {@link PTransform PTransforms} can be applied multiple times, with different names
   * each time.
   */
  @Deprecated
  public String getFullNameForTesting(PTransform<?, ?> transform) {
    Collection<AppliedPTransform<?, ?, ?>> uses =
        transformApplicationsForTesting.get(transform);
    checkState(uses.size() > 0, "Unknown transform: " + transform);
    checkState(uses.size() <= 1, "Transform used multiple times: " + transform);
    return Iterables.getOnlyElement(uses).getFullName();
  }

  /**
   * Returns a unique name for a transform with the given prefix (from
   * enclosing transforms) and initial name.
   *
   * <p>For internal use only.
   */
  private String uniquifyInternal(String namePrefix, String origName) {
    String name = origName;
    int suffixNum = 2;
    while (true) {
      String candidate = buildName(namePrefix, name);
      if (usedFullNames.add(candidate)) {
        return candidate;
      }
      // A duplicate!  Retry.
      name = origName + suffixNum++;
    }
  }

  /**
   * Builds a name from a "/"-delimited prefix and a name.
   */
  private String buildName(String namePrefix, String name) {
    return namePrefix.isEmpty() ? name : namePrefix + "/" + name;
  }

  /**
   * Adds the given {@link PValue} to this {@link Pipeline}.
   *
   * <p>For internal use only.
   */
  public void addValueInternal(PValue value) {
    this.values.add(value);
    LOG.debug("Adding {} to {}", value, this);
  }
}
