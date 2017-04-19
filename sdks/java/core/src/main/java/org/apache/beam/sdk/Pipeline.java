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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Replaces all nodes that match a {@link PTransformOverride} in this pipeline. Overrides are
   * applied in the order they are present within the list.
   *
   * <p>After all nodes are replaced, ensures that no nodes in the updated graph match any of the
   * overrides.
   */
  public void replaceAll(List<PTransformOverride> overrides) {
    for (PTransformOverride override : overrides) {
      replace(override);
    }
    checkNoMoreMatches(overrides);
  }

  private void checkNoMoreMatches(final List<PTransformOverride> overrides) {
    traverseTopologically(
        new PipelineVisitor.Defaults() {
          SetMultimap<Node, PTransformOverride> matched = HashMultimap.create();

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              for (PTransformOverride override : overrides) {
                if (override.getMatcher().matches(node.toAppliedPTransform())) {
                  matched.put(node, override);
                }
              }
            }
            if (!matched.containsKey(node)) {
              return CompositeBehavior.ENTER_TRANSFORM;
            }
            return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode()) {
              checkState(
                  matched.isEmpty(), "Found nodes that matched overrides. Matches: %s", matched);
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            for (PTransformOverride override : overrides) {
              if (override.getMatcher().matches(node.toAppliedPTransform())) {
                matched.put(node, override);
              }
            }
          }
        });
  }

  private void replace(final PTransformOverride override) {
    final Set<Node> matches = new HashSet<>();
    final Set<Node> freedNodes = new HashSet<>();
    transforms.visit(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode() && freedNodes.contains(node.getEnclosingNode())) {
              // This node will be freed because its parent will be freed.
              freedNodes.add(node);
              return CompositeBehavior.ENTER_TRANSFORM;
            }
            if (!node.isRootNode() && override.getMatcher().matches(node.toAppliedPTransform())) {
              matches.add(node);
              // This node will be freed. When we visit any of its children, they will also be freed
              freedNodes.add(node);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            if (freedNodes.contains(node.getEnclosingNode())) {
              freedNodes.add(node);
            } else if (override.getMatcher().matches(node.toAppliedPTransform())) {
              matches.add(node);
              freedNodes.add(node);
            }
          }
        });
    for (Node freedNode : freedNodes) {
      usedFullNames.remove(freedNode.getFullName());
    }
    for (Node match : matches) {
      applyReplacement(match, override.getOverrideFactory());
    }
  }

  /**
   * Runs the {@link Pipeline} using its {@link PipelineRunner}.
   */
  public PipelineResult run() {
    // Ensure all of the nodes are fully specified before a PipelineRunner gets access to the
    // pipeline.
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
    CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node);

    /**
     * Called for each composite transform after all of its component transforms and their outputs
     * have been visited.
     */
    void leaveCompositeTransform(TransformHierarchy.Node node);

    /**
     * Called for each primitive transform after all of its topological predecessors
     * and inputs have been visited.
     */
    void visitPrimitiveTransform(TransformHierarchy.Node node);

    /**
     * Called for each value after the transform that produced the value has been
     * visited.
     */
    void visitValue(PValue value, TransformHierarchy.Node producer);

    /**
     * Control enum for indicating whether or not a traversal should process the contents of
     * a composite transform or not.
     */
    enum CompositeBehavior {
      ENTER_TRANSFORM,
      DO_NOT_ENTER_TRANSFORM
    }

    /**
     * Default no-op {@link PipelineVisitor} that enters all composite transforms.
     * User implementations can override just those methods they are interested in.
     */
    class Defaults implements PipelineVisitor {
      @Override
      public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void leaveCompositeTransform(TransformHierarchy.Node node) { }

      @Override
      public void visitPrimitiveTransform(TransformHierarchy.Node node) { }

      @Override
      public void visitValue(PValue value, TransformHierarchy.Node producer) { }
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
    transforms.visit(visitor);
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
  private final TransformHierarchy transforms = new TransformHierarchy(this);
  private Set<String> usedFullNames = new HashSet<>();
  private CoderRegistry coderRegistry;

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
  private <InputT extends PInput, OutputT extends POutput> OutputT applyInternal(
      String name, InputT input, PTransform<? super InputT, OutputT> transform) {
    String namePrefix = transforms.getCurrent().getFullName();
    String uniqueName = uniquifyInternal(namePrefix, name);

    boolean nameIsUnique = uniqueName.equals(buildName(namePrefix, name));

    if (!nameIsUnique) {
      switch (getOptions().getStableUniqueNames()) {
        case OFF:
          break;
        case WARNING:
          LOG.warn(
              "Transform {} does not have a stable unique name. "
                  + "This will prevent updating of pipelines.",
              uniqueName);
          break;
        case ERROR:
          throw new IllegalStateException(
              "Transform "
                  + uniqueName
                  + " does not have a stable unique name. "
                  + "This will prevent updating of pipelines.");
        default:
          throw new IllegalArgumentException(
              "Unrecognized value for stable unique names: " + getOptions().getStableUniqueNames());
      }
    }

    LOG.debug("Adding {} to {}", transform, this);
    transforms.pushNode(uniqueName, input, transform);
    try {
      transforms.finishSpecifyingInput();
      transform.validate(input);
      OutputT output = transform.expand(input);
      transforms.setOutput(output);

      return output;
    } finally {
      transforms.popNode();
    }
  }

  private <InputT extends PInput, OutputT extends POutput,
          TransformT extends PTransform<? super InputT, OutputT>>
      void applyReplacement(
          Node original,
          PTransformOverrideFactory<InputT, OutputT, TransformT> replacementFactory) {
    PTransformReplacement<InputT, OutputT> replacement =
        replacementFactory.getReplacementTransform(
            (AppliedPTransform<InputT, OutputT, TransformT>) original.toAppliedPTransform());
    if (replacement.getTransform() == original.getTransform()) {
      return;
    }
    InputT originalInput = replacement.getInput();

    LOG.debug("Replacing {} with {}", original, replacement);
    transforms.replaceNode(original, originalInput, replacement.getTransform());
    try {
      OutputT newOutput = replacement.getTransform().expand(originalInput);
      Map<PValue, ReplacementOutput> originalToReplacement =
          replacementFactory.mapOutputs(original.getOutputs(), newOutput);
      // Ensure the internal TransformHierarchy data structures are consistent.
      transforms.setOutput(newOutput);
      transforms.replaceOutputs(originalToReplacement);
    } finally {
      transforms.popNode();
    }
  }

  /**
   * Returns the configured {@link PipelineOptions}.
   *
   * @deprecated see BEAM-818 Remove Pipeline.getPipelineOptions. Configuration should be explicitly
   *     provided to a transform if it is required.
   */
  @Deprecated
  public PipelineOptions getOptions() {
    return options;
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
   * Returns a {@link Map} from each {@link Aggregator} in the {@link Pipeline} to the {@link
   * PTransform PTransforms} in which it is used.
   */
  public Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> getAggregatorSteps() {
    return new AggregatorPipelineExtractor(this).getAggregatorSteps();
  }

  /**
   * Builds a name from a "/"-delimited prefix and a name.
   */
  private String buildName(String namePrefix, String name) {
    return namePrefix.isEmpty() ? name : namePrefix + "/" + name;
  }
}
