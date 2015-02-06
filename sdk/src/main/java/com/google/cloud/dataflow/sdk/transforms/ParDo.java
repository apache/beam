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

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.util.DirectModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.DoFnRunner;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@code ParDo} is the core element-wise transform in Google Cloud
 * Dataflow, invoking a user-specified function (from {@code I} to
 * {@code O}) on each of the elements of the input
 * {@code PCollection<I>} to produce zero or more output elements, all
 * of which are collected into the output {@code PCollection<O>}.
 *
 * <p> Elements are processed independently, and possibly in parallel across
 * distributed cloud resources.
 *
 * <p> The {@code ParDo} processing style is similar to what happens inside
 * the "Mapper" or "Reducer" class of a MapReduce-style algorithm.
 *
 * <h2>{@code DoFn}s</h2>
 *
 * <p> The function to use to process each element is specified by a
 * {@link DoFn DoFn<I, O>}.
 *
 * <p> Conceptually, when a {@code ParDo} transform is executed, the
 * elements of the input {@code PCollection<I>} are first divided up
 * into some number of "batches".  These are farmed off to distributed
 * worker machines (or run locally, if using the
 * {@link com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner}).
 * For each batch of input elements, a fresh instance of the argument
 * {@code DoFn<I, O>} is created on a worker, then the {@code DoFn}'s
 * optional {@link DoFn#startBundle} method is called to initialize it,
 * then the {@code DoFn}'s required {@link DoFn#processElement} method
 * is called on each of the input elements in the batch, then the
 * {@code DoFn}'s optional {@link DoFn#finishBundle} method is called
 * to complete its work, and finally the {@code DoFn} instance is
 * thrown away.  Each of the calls to any of the {@code DoFn}'s
 * methods can produce zero or more output elements, which are
 * collected together into a batch of output elements.  All of the
 * batches of output elements from all of the {@code DoFn} instances
 * are "flattened" together into the output {@code PCollection<O>}.
 *
 * <p> For example:
 *
 * <pre> {@code
 * PCollection<String> lines = ...;
 * PCollection<String> words =
 *     lines.apply(ParDo.of(new DoFn<String, String>() {
 *         public void processElement(ProcessContext c) {
 *           String line = c.element();
 *           for (String word : line.split("[^a-zA-Z']+")) {
 *             c.output(word);
 *           }
 *         }}));
 * PCollection<Integer> wordLengths =
 *     words.apply(ParDo.of(new DoFn<String, Integer>() {
 *         public void processElement(ProcessContext c) {
 *           String word = c.element();
 *           Integer length = word.length();
 *           c.output(length);
 *         }}));
 * } </pre>
 *
 * <p> Each output element has the same timestamp and is in the same windows
 * as its corresponding input element, and the output {@code PCollection}
 * has the same
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
 * associated with it as the input.
 *
 * <h2>Naming {@code ParDo}s</h2>
 *
 * <p> A {@code ParDo} transform can be given a name using
 * {@link #named}.  While the system will automatically provide a name
 * if none is specified explicitly, it is still a good practice to
 * provide an explicit name, since that will probably make monitoring
 * output more readable.  For example:
 *
 * <pre> {@code
 * PCollection<String> words =
 *     lines.apply(ParDo.named("ExtractWords")
 *                      .of(new DoFn<String, String>() { ... }));
 * PCollection<Integer> wordLengths =
 *     words.apply(ParDo.named("ComputeWordLengths")
 *                      .of(new DoFn<String, Integer>() { ... }));
 * } </pre>
 *
 * <h2>Side Inputs</h2>
 *
 * <p> While a {@code ParDo} iterates over a single "main input"
 * {@code PCollection}, it can take additional "side input"
 * {@code PCollectionView}s. These side input
 * {@code PCollectionView}s express styles of accessing
 * {@code PCollection}s computed by earlier pipeline operations,
 * passed in to the {@code ParDo} transform using
 * {@link #withSideInputs}, and their contents accessible to each of
 * the {@code DoFn} operations via {@link DoFn.Context#sideInput}.
 * For example:
 *
 * <pre> {@code
 * PCollection<String> words = ...;
 * PCollection<Integer> maxWordLengthCutOff = ...; // Singleton PCollection
 * final PCollectionView<Integer> maxWordLengthCutOffView =
 *     SingletonPCollectionView.of(maxWordLengthCutOff);
 * PCollection<String> wordsBelowCutOff =
 *     words.apply(ParDo.withSideInput(maxWordLengthCutOffView)
 *                      .of(new DoFn<String, String>() {
 *         public void processElement(ProcessContext c) {
 *           String word = c.element();
 *           int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
 *           if (word.length() <= lengthCutOff) {
 *             c.output(word);
 *           }
 *         }}));
 * } </pre>
 *
 * <h2>Side Outputs</h2>
 *
 * <p> Optionally, a {@code ParDo} transform can produce multiple
 * output {@code PCollection}s, both a "main output"
 * {@code PCollection<O>} plus any number of "side output"
 * {@code PCollection}s, each keyed by a distinct {@link TupleTag},
 * and bundled in a {@link PCollectionTuple}.  The {@code TupleTag}s
 * to be used for the output {@code PCollectionTuple} is specified by
 * invoking {@link #withOutputTags}.  Unconsumed side outputs does not
 * necessarily need to be explicity specified, even if the {@code DoFn}
 * generates them. Within the {@code DoFn}, an element is added to the
 * main output {@code PCollection} as normal, using
 * {@link DoFn.Context#output}, while an element is added to a side output
 * {@code PCollection} using {@link DoFn.Context#sideOutput}.  For example:
 *
 * <pre> {@code
 * PCollection<String> words = ...;
 * // Select words whose length is below a cut off,
 * // plus the lengths of words that are above the cut off.
 * // Also select words starting with "MARKER".
 * final int wordLengthCutOff = 10;
 * // Create tags to use for the main and side outputs.
 * final TupleTag<String> wordsBelowCutOffTag =
 *     new TupleTag<String>(){};
 * final TupleTag<Integer> wordLengthsAboveCutOffTag =
 *     new TupleTag<Integer>(){};
 * final TupleTag<String> markedWordsTag =
 *     new TupleTag<String>(){};
 * PCollectionTuple results =
 *     words.apply(
 *         ParDo
 *         // Specify the main and consumed side output tags of the
 *         // PCollectionTuple result:
 *         .withOutputTags(wordsBelowCutOffTag,
 *                         TupleTagList.of(wordLengthsAboveCutOffTag)
 *                                     .and(markedWordsTag))
 *         .of(new DoFn<String, String>() {
 *             // Create a tag for the unconsumed side output.
 *             final TupleTag<String> specialWordsTag =
 *                 new TupleTag<String>(){};
 *             public void processElement(ProcessContext c) {
 *               String word = c.element();
 *               if (word.length() <= wordLengthCutOff) {
 *                 // Emit this short word to the main output.
 *                 c.output(word);
 *               } else {
 *                 // Emit this long word's length to a side output.
 *                 c.sideOutput(wordLengthsAboveCutOffTag, word.length());
 *               }
 *               if (word.startsWith("MARKER")) {
 *                 // Emit this word to a different side output.
 *                 c.sideOutput(markedWordsTag, word);
 *               }
 *               if (word.startsWith("SPECIAL")) {
 *                 // Emit this word to the unconsumed side output.
 *                 c.sideOutput(specialWordsTag, word);
 *               }
 *             }}));
 * // Extract the PCollection results, by tag.
 * PCollection<String> wordsBelowCutOff =
 *     results.get(wordsBelowCutOffTag);
 * PCollection<Integer> wordLengthsAboveCutOff =
 *     results.get(wordLengthsAboveCutOffTag);
 * PCollection<String> markedWords =
 *     results.get(markedWordsTag);
 * } </pre>
 *
 * <h2>Properties May Be Specified In Any Order</h2>
 *
 * Several properties can be specified for a {@code ParDo}
 * {@code PTransform}, including name, side inputs, side output tags,
 * and {@code DoFn} to invoke.  Only the {@code DoFn} is required; the
 * name is encouraged but not required, and side inputs and side
 * output tags are only specified when they're needed.  These
 * properties can be specified in any order, as long as they're
 * specified before the {@code ParDo} {@code PTransform} is applied.
 *
 * <p> The approach used to allow these properties to be specified in
 * any order, with some properties omitted, is to have each of the
 * property "setter" methods defined as static factory methods on
 * {@code ParDo} itself, which return an instance of either
 * {@link ParDo.Unbound ParDo.Unbound} or
 * {@link ParDo.Bound ParDo.Bound} nested classes, each of which offer
 * property setter instance methods to enable setting additional
 * properties.  {@code ParDo.Bound} is used for {@code ParDo}
 * transforms whose {@code DoFn} is specified and whose input and
 * output static types have been bound.  {@code ParDo.Unbound} is used
 * for {@code ParDo} transforms that have not yet had their
 * {@code DoFn} specified.  Only {@code ParDo.Bound} instances can be
 * applied.
 *
 * <p> Another benefit of this approach is that it reduces the number
 * of type parameters that need to be specified manually.  In
 * particular, the input and output types of the {@code ParDo}
 * {@code PTransform} are inferred automatically from the type
 * parameters of the {@code DoFn} argument passed to {@link ParDo#of}.
 *
 * <h2>Output Coders</h2>
 *
 * <p> By default, the {@code Coder} of the
 * elements of the main output {@code PCollection<O>} is inferred from the
 * concrete type of the {@code DoFn<I, O>}'s output type {@code O}.
 *
 * <p> By default, the {@code Coder} of the elements of a side output
 * {@code PCollection<X>} is inferred from the concrete type of the
 * corresponding {@code TupleTag<X>}'s type {@code X}.  To be
 * successful, the {@code TupleTag} should be created as an instance
 * of a trivial anonymous subclass, with {@code {}} suffixed to the
 * constructor call.  Such uses block Java's generic type parameter
 * inference, so the {@code <X>} argument must be provided explicitly.
 * For example:
 * <pre> {@code
 * // A TupleTag to use for a side input can be written concisely:
 * final TupleTag<Integer> sideInputTag = new TupleTag<>();
 * // A TupleTag to use for a side output should be written with "{}",
 * // and explicit generic parameter type:
 * final TupleTag<String> sideOutputTag = new TupleTag<String>(){};
 * } </pre>
 * This style of {@code TupleTag} instantiation is used in the example of
 * multiple side outputs, above.
 *
 * <h2>Ordered Input and/or Output PCollections</h2>
 *
 * <p> If the input {@code PCollection} is ordered (see
 * {@link PCollection#setOrdered}), then each batch of the input
 * processed by a {@code DoFn} instance will correspond to a
 * consecutive subsequence of elements of the input, and the
 * {@link DoFn#processElement} operation will be invoked on each
 * element of the batch in order; otherwise, batches will correspond
 * to arbitrary subsets of elements of the input, processed in
 * arbitrary order.
 *
 * <p> Independently, if a main or side output {@code PCollection} is
 * ordered, then the order in which elements are output to it will be
 * preserved in the output {@code PCollection}; otherwise, the order
 * in which elements are output to the {@code PCollection} doesn't
 * matter.  If the input {@code PCollection} is also ordered, then the
 * sequences of elements output from the batches will be concatenated
 * together in the same order as the batches appear in the input,
 * supporting order-preserving transforms on {@code PCollection}s.
 *
 * <h2>Serializability of {@code DoFn}s</h2>
 *
 * <p> A {@code DoFn} passed to a {@code ParDo} transform must be
 * {@code Serializable}.  This allows the {@code DoFn} instance
 * created in this "main program" to be sent (in serialized form) to
 * remote worker machines and reconstituted for each batch of elements
 * of the input {@code PCollection} being processed.  A {@code DoFn}
 * can have instance variable state, and non-transient instance
 * variable state will be serialized in the main program and then
 * deserialized on remote worker machines for each batch of elements
 * to process.
 *
 * <p> To aid in ensuring that {@code DoFn}s are properly
 * {@code Serializable}, even local execution using the
 * {@link DirectPipelineRunner} will serialize and then deserialize
 * {@code DoFn}s before executing them on a batch.
 *
 * <p> {@code DoFn}s expressed as anonymous inner classes can be
 * convenient, but due to a quirk in Java's rules for serializability,
 * non-static inner or nested classes (including anonymous inner
 * classes) automatically capture their enclosing class's instance in
 * their serialized state.  This can lead to including much more than
 * intended in the serialized state of a {@code DoFn}, or even things
 * that aren't {@code Serializable}.
 *
 * <p> There are two ways to avoid unintended serialized state in a
 * {@code DoFn}:
 *
 * <ul>
 *
 * <li> Define the {@code DoFn} as a named, static class.
 *
 * <li> Define the {@code DoFn} as an anonymous inner class inside of
 * a static method.
 *
 * </ul>
 *
 * Both these approaches ensure that there is no implicit enclosing
 * class instance serialized along with the {@code DoFn} instance.
 *
 * <p> Prior to Java 8, any local variables of the enclosing
 * method referenced from within an anonymous inner class need to be
 * marked as {@code final}.  If defining the {@code DoFn} as a named
 * static class, such variables would be passed as explicit
 * constructor arguments and stored in explicit instance variables.
 *
 * <p> There are three main ways to initialize the state of a
 * {@code DoFn} instance processing a batch:
 *
 * <ul>
 *
 * <li> Define instance variable state (including implicit instance
 * variables holding final variables captured by an anonymous inner
 * class), initialized by the {@code DoFn}'s constructor (which is
 * implicit for an anonymous inner class).  This state will be
 * automatically serialized and then deserialized in the {@code DoFn}
 * instance created for each batch.  This method is good for state
 * known when the original {@code DoFn} is created in the main
 * program, if it's not overly large.
 *
 * <li> Compute the state as a singleton {@code PCollection} and pass it
 * in as a side input to the {@code DoFn}.  This is good if the state
 * needs to be computed by the pipeline, or if the state is very large
 * and so is best read from file(s) rather than sent as part of the
 * {@code DoFn}'s serialized state.
 *
 * <li> Initialize the state in each {@code DoFn} instance, in
 * {@link DoFn#startBundle}.  This is good if the initialization
 * doesn't depend on any information known only by the main program or
 * computed by earlier pipeline operations, but is the same for all
 * instances of this {@code DoFn} for all program executions, say
 * setting up empty caches or initializing constant data.
 *
 * </ul>
 *
 * <h2>No Global Shared State</h2>
 *
 * <p> {@code ParDo} operations are intended to be able to run in
 * parallel across multiple worker machines.  This precludes easy
 * sharing and updating mutable state across those machines.  There is
 * no support in the Google Cloud Dataflow system for communicating
 * and synchronizing updates to shared state across worker machines,
 * so programs should not access any mutable static variable state in
 * their {@code DoFn}, without understanding that the Java processes
 * for the main program and workers will each have its own independent
 * copy of such state, and there won't be any automatic copying of
 * that state across Java processes.  All information should be
 * communicated to {@code DoFn} instances via main and side inputs and
 * serialized state, and all output should be communicated from a
 * {@code DoFn} instance via main and side outputs, in the absence of
 * external communication mechanisms written by user code.
 *
 * <h2>Fault Tolerance</h2>
 *
 * <p> In a distributed system, things can fail: machines can crash,
 * machines can be unable to communicate across the network, etc.
 * While individual failures are rare, the larger the job, the greater
 * the chance that something, somewhere, will fail.  The Google Cloud
 * Dataflow service strives to mask such failures automatically,
 * principally by retrying failed {@code DoFn} batches.  This means
 * that a {@code DoFn} instance might process a batch partially, then
 * crash for some reason, then be rerun (often on a different worker
 * machine) on that same batch and on the same elements as before.
 * Sometimes two or more {@code DoFn} instances will be running on the
 * same batch simultaneously, with the system taking the results of
 * the first instance to complete successfully.  Consequently, the
 * code in a {@code DoFn} needs to be written such that these
 * duplicate (sequential or concurrent) executions do not cause
 * problems.  If the outputs of a {@code DoFn} are a pure function of
 * its inputs, then this requirement is satisfied.  However, if a
 * {@code DoFn}'s execution has external side-effects, say performing
 * updates to external HTTP services, then the {@code DoFn}'s code
 * needs to take care to ensure that those updates are idempotent and
 * that concurrent updates are acceptable.  This property can be
 * difficult to achieve, so it is advisable to strive to keep
 * {@code DoFn}s as pure functions as much as possible.
 *
 * <h2>Optimization</h2>
 *
 * <p> The Google Cloud Dataflow service automatically optimizes a
 * pipeline before it is executed.  A key optimization, <i>fusion</i>,
 * relates to ParDo operations.  If one ParDo operation produces a
 * PCollection that is then consumed as the main input of another
 * ParDo operation, the two ParDo operations will be <i>fused</i>
 * together into a single ParDo operation and run in a single pass;
 * this is "producer-consumer fusion".  Similarly, if
 * two or more ParDo operations have the same PCollection main input,
 * they will be fused into a single ParDo which makes just one pass
 * over the input PCollection; this is "sibling fusion".
 *
 * <p> If after fusion there are no more unfused references to a
 * PCollection (e.g., one between a producer ParDo and a consumer
 * ParDo), the PCollection itself is "fused away" and won't ever be
 * written to disk, saving all the I/O and space expense of
 * constructing it.
 *
 * <p> The Google Cloud Dataflow service applies fusion as much as
 * possible, greatly reducing the cost of executing pipelines.  As a
 * result, it is essentially "free" to write ParDo operations in a
 * very modular, composable style, each ParDo operation doing one
 * clear task, and stringing together sequences of ParDo operations to
 * get the desired overall effect.  Such programs can be easier to
 * understand, easier to unit-test, easier to extend and evolve, and
 * easier to reuse in new programs.  The predefined library of
 * PTransforms that come with Google Cloud Dataflow makes heavy use of
 * this modular, composable style, trusting to the Google Cloud
 * Dataflow service's optimizer to "flatten out" all the compositions
 * into highly optimized stages.
 *
 * @see <a href="https://cloud.google.com/dataflow/java-sdk/par-do">Parallel
 * Processing with ParDo</a>
 */
public class ParDo {

  /**
   * Creates a {@code ParDo} {@code PTransform} with the given name.
   *
   * <p> See the discussion of Naming above for more explanation.
   *
   * <p> The resulting {@code PTransform} is incomplete, and its
   * input/output types are not yet bound.  Use
   * {@link ParDo.Unbound#of} to specify the {@link DoFn} to
   * invoke, which will also bind the input/output types of this
   * {@code PTransform}.
   */
  public static Unbound named(String name) {
    return new Unbound().named(name);
  }

  /**
   * Creates a {@code ParDo} {@code PTransform} with the given
   * side inputs.
   *
   * <p> Side inputs are {@link PCollectionView}s, whose contents are
   * computed during pipeline execution and then made accessible to
   * {@code DoFn} code via {@link DoFn.Context#sideInput}. Each
   * invocation of the {@code DoFn} receives the same values for these
   * side inputs.
   *
   * <p> See the discussion of Side Inputs above for more explanation.
   *
   * <p> The resulting {@code PTransform} is incomplete, and its
   * input/output types are not yet bound.  Use
   * {@link ParDo.Unbound#of} to specify the {@link DoFn} to
   * invoke, which will also bind the input/output types of this
   * {@code PTransform}.
   */
  public static Unbound withSideInputs(PCollectionView<?, ?>... sideInputs) {
    return new Unbound().withSideInputs(sideInputs);
  }

  /**
    * Creates a {@code ParDo} with the given side inputs.
    *
   * <p> Side inputs are {@link PCollectionView}s, whose contents are
   * computed during pipeline execution and then made accessible to
   * {@code DoFn} code via {@link DoFn.Context#sideInput}.
   *
   * <p> See the discussion of Side Inputs above for more explanation.
   *
   * <p> The resulting {@code PTransform} is incomplete, and its
   * input/output types are not yet bound.  Use
   * {@link ParDo.Unbound#of} to specify the {@link DoFn} to
   * invoke, which will also bind the input/output types of this
   * {@code PTransform}.
   */
  public static Unbound withSideInputs(
      Iterable<? extends PCollectionView<?, ?>> sideInputs) {
    return new Unbound().withSideInputs(sideInputs);
  }

  /**
   * Creates a multi-output {@code ParDo} {@code PTransform} whose
   * output {@link PCollection}s will be referenced using the given main
   * output and side output tags.
   *
   * <p> {@link TupleTag}s are used to name (with its static element
   * type {@code T}) each main and side output {@code PCollection<T>}.
   * This {@code PTransform}'s {@link DoFn} emits elements to the main
   * output {@code PCollection} as normal, using
   * {@link DoFn.Context#output}.  The {@code DoFn} emits elements to
   * a side output {@code PCollection} using
   * {@link DoFn.Context#sideOutput}, passing that side output's tag
   * as an argument.  The result of invoking this {@code PTransform}
   * will be a {@link PCollectionTuple}, and any of the the main and
   * side output {@code PCollection}s can be retrieved from it via
   * {@link PCollectionTuple#get}, passing the output's tag as an
   * argument.
   *
   * <p> See the discussion of Side Outputs above for more explanation.
   *
   * <p> The resulting {@code PTransform} is incomplete, and its input
   * type is not yet bound.  Use {@link ParDo.UnboundMulti#of}
   * to specify the {@link DoFn} to invoke, which will also bind the
   * input type of this {@code PTransform}.
   */
  public static <O> UnboundMulti<O> withOutputTags(
      TupleTag<O> mainOutputTag,
      TupleTagList sideOutputTags) {
    return new Unbound().withOutputTags(mainOutputTag, sideOutputTags);
  }

  /**
   * Creates a {@code ParDo} {@code PTransform} that will invoke the
   * given {@link DoFn} function.
   *
   * <p> The resulting {@code PTransform}'s types have been bound, with the
   * input being a {@code PCollection<I>} and the output a
   * {@code PCollection<O>}, inferred from the types of the argument
   * {@code DoFn<I, O>}.  It is ready to be applied, or further
   * properties can be set on it first.
   */
  public static <I, O> Bound<I, O> of(DoFn<I, O> fn) {
    return new Unbound().of(fn);
  }

  /**
   * An incomplete {@code ParDo} transform, with unbound input/output types.
   *
   * <p> Before being applied, {@link ParDo.Unbound#of} must be
   * invoked to specify the {@link DoFn} to invoke, which will also
   * bind the input/output types of this {@code PTransform}.
   */
  public static class Unbound {
    String name;
    List<PCollectionView<?, ?>> sideInputs = Collections.emptyList();

    Unbound() {}

    Unbound(String name,
            List<PCollectionView<?, ?>> sideInputs) {
      this.name = name;
      this.sideInputs = sideInputs;
    }

    /**
     * Returns a new {@code ParDo} transform that's like this
     * transform but with the specified name.  Does not modify this
     * transform.  The resulting transform is still incomplete.
     *
     * <p> See the discussion of Naming above for more explanation.
     */
    public Unbound named(String name) {
      return new Unbound(name, sideInputs);
    }

    /**
     * Returns a new {@code ParDo} transform that's like this
     * transform but with the specified side inputs.
     * Does not modify this transform. The resulting transform is
     * still incomplete.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Unbound withSideInputs(PCollectionView<?, ?>... sideInputs) {
      return new Unbound(name, ImmutableList.copyOf(sideInputs));
    }

    /**
     * Returns a new {@code ParDo} transform that's like this
     * transform but with the specified side inputs.  Does not modify
     * this transform.  The resulting transform is still incomplete.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Unbound withSideInputs(
        Iterable<? extends PCollectionView<?, ?>> sideInputs) {
      return new Unbound(name, ImmutableList.copyOf(sideInputs));
    }

    /**
     * Returns a new multi-output {@code ParDo} transform that's like
     * this transform but with the specified main and side output
     * tags.  Does not modify this transform.  The resulting transform
     * is still incomplete.
     *
     * <p> See the discussion of Side Outputs above and on
     * {@link ParDo#withOutputTags} for more explanation.
     */
    public <O> UnboundMulti<O> withOutputTags(TupleTag<O> mainOutputTag,
                                              TupleTagList sideOutputTags) {
      return new UnboundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new {@code ParDo} {@code PTransform} that's like this
     * transform but which will invoke the given {@link DoFn}
     * function, and which has its input and output types bound.  Does
     * not modify this transform.  The resulting {@code PTransform} is
     * sufficiently specified to be applied, but more properties can
     * still be specified.
     */
    public <I, O> Bound<I, O> of(DoFn<I, O> fn) {
      return new Bound<>(name, sideInputs, fn);
    }
  }

  /**
   * A {@code PTransform} that, when applied to a {@code PCollection<I>},
   * invokes a user-specified {@code DoFn<I, O>} on all its elements,
   * with all its outputs collected into an output
   * {@code PCollection<O>}.
   *
   * <p> A multi-output form of this transform can be created with
   * {@link ParDo.Bound#withOutputTags}.
   *
   * @param <I> the type of the (main) input {@code PCollection} elements
   * @param <O> the type of the (main) output {@code PCollection} elements
   */
  @SuppressWarnings("serial")
  public static class Bound<I, O>
      extends PTransform<PCollection<? extends I>, PCollection<O>> {
    // Inherits name.
    List<PCollectionView<?, ?>> sideInputs;
    DoFn<I, O> fn;

    Bound(String name,
          List<PCollectionView<?, ?>> sideInputs,
          DoFn<I, O> fn) {
      super(name);
      this.sideInputs = sideInputs;
      this.fn = fn;
    }

    /**
     * Returns a new {@code ParDo} {@code PTransform} that's like this
     * {@code PTransform} but with the specified name.  Does not
     * modify this {@code PTransform}.
     *
     * <p> See the discussion of Naming above for more explanation.
     */
    public Bound<I, O> named(String name) {
      return new Bound<>(name, sideInputs, fn);
    }

    /**
     * Returns a new {@code ParDo} {@code PTransform} that's like this
     * {@code PTransform} but with the specified side inputs.  Does not
     * modify this {@code PTransform}.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Bound<I, O> withSideInputs(PCollectionView<?, ?>... sideInputs) {
      return new Bound<>(name, ImmutableList.copyOf(sideInputs), fn);
    }

    /**
     * Returns a new {@code ParDo} {@code PTransform} that's like this
     * {@code PTransform} but with the specified side inputs.  Does not
     * modify this {@code PTransform}.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Bound<I, O> withSideInputs(
        Iterable<? extends PCollectionView<?, ?>> sideInputs) {
      return new Bound<>(name, ImmutableList.copyOf(sideInputs), fn);
    }

    /**
     * Returns a new multi-output {@code ParDo} {@code PTransform}
     * that's like this {@code PTransform} but with the specified main
     * and side output tags.  Does not modify this {@code PTransform}.
     *
     * <p> See the discussion of Side Outputs above and on
     * {@link ParDo#withOutputTags} for more explanation.
     */
    public BoundMulti<I, O> withOutputTags(TupleTag<O> mainOutputTag,
                                           TupleTagList sideOutputTags) {
      return new BoundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags, fn);
    }

    @Override
    public PCollection<O> apply(PCollection<? extends I> input) {
      if (sideInputs == null) {
        sideInputs = Collections.emptyList();
      }
      return PCollection.<O>createPrimitiveOutputInternal(getInput().getWindowFn())
          .setTypeTokenInternal(fn.getOutputTypeToken());
    }

    @Override
    protected Coder<O> getDefaultOutputCoder() {
      return getPipeline().getCoderRegistry().getDefaultCoder(
          fn.getOutputTypeToken(),
          fn.getInputTypeToken(),
          ((PCollection<I>) getInput()).getCoder());
    }

    @Override
    protected String getDefaultName() {
      return StringUtils.approximateSimpleName(fn.getClass());
    }

    @Override
    protected String getKindString() { return "ParDo"; }

    public DoFn<I, O> getFn() {
      return fn;
    }

    public List<PCollectionView<?, ?>> getSideInputs() {
      return sideInputs;
    }
  }

  /**
   * An incomplete multi-output {@code ParDo} transform, with unbound
   * input type.
   *
   * <p> Before being applied, {@link ParDo.UnboundMulti#of} must be
   * invoked to specify the {@link DoFn} to invoke, which will also
   * bind the input type of this {@code PTransform}.
   *
   * @param <O> the type of the main output {@code PCollection} elements
   */
  public static class UnboundMulti<O> {
    String name;
    List<PCollectionView<?, ?>> sideInputs;
    TupleTag<O> mainOutputTag;
    TupleTagList sideOutputTags;

    UnboundMulti(String name,
                 List<PCollectionView<?, ?>> sideInputs,
                 TupleTag<O> mainOutputTag,
                 TupleTagList sideOutputTags) {
      this.name = name;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.sideOutputTags = sideOutputTags;
    }

    /**
     * Returns a new multi-output {@code ParDo} transform that's like
     * this transform but with the specified name.  Does not modify
     * this transform.  The resulting transform is still incomplete.
     *
     * <p> See the discussion of Naming above for more explanation.
     */
    public UnboundMulti<O> named(String name) {
      return new UnboundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new multi-output {@code ParDo} transform that's like
     * this transform but with the specified side inputs.  Does not
     * modify this transform.  The resulting transform is still
     * incomplete.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public UnboundMulti<O> withSideInputs(
        PCollectionView<?, ?>... sideInputs) {
      return new UnboundMulti<>(
          name, ImmutableList.copyOf(sideInputs),
          mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new multi-output {@code ParDo} transform that's like
     * this transform but with the specified side inputs.  Does not
     * modify this transform.  The resulting transform is still
     * incomplete.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public UnboundMulti<O> withSideInputs(
        Iterable<? extends PCollectionView<?, ?>> sideInputs) {
      return new UnboundMulti<>(
          name, ImmutableList.copyOf(sideInputs),
          mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new multi-output {@code ParDo} {@code PTransform}
     * that's like this transform but which will invoke the given
     * {@link DoFn} function, and which has its input type bound.
     * Does not modify this transform.  The resulting
     * {@code PTransform} is sufficiently specified to be applied, but
     * more properties can still be specified.
     */
    public <I> BoundMulti<I, O> of(DoFn<I, O> fn) {
      return new BoundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags, fn);
    }
  }

  /**
   * A {@code PTransform} that, when applied to a
   * {@code PCollection<I>}, invokes a user-specified
   * {@code DoFn<I, O>} on all its elements, which can emit elements
   * to any of the {@code PTransform}'s main and side output
   * {@code PCollection}s, which are bundled into a result
   * {@code PCollectionTuple}.
   *
   * @param <I> the type of the (main) input {@code PCollection} elements
   * @param <O> the type of the main output {@code PCollection} elements
   */
  @SuppressWarnings("serial")
  public static class BoundMulti<I, O>
      extends PTransform<PCollection<? extends I>, PCollectionTuple> {
    // Inherits name.
    List<PCollectionView<?, ?>> sideInputs;
    TupleTag<O> mainOutputTag;
    TupleTagList sideOutputTags;
    DoFn<I, O> fn;

    BoundMulti(String name,
               List<PCollectionView<?, ?>> sideInputs,
               TupleTag<O> mainOutputTag,
               TupleTagList sideOutputTags,
               DoFn<I, O> fn) {
      super(name);
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.sideOutputTags = sideOutputTags;
      this.fn = fn;
    }

    /**
     * Returns a new multi-output {@code ParDo} {@code PTransform}
     * that's like this {@code PTransform} but with the specified
     * name.  Does not modify this {@code PTransform}.
     *
     * <p> See the discussion of Naming above for more explanation.
     */
    public BoundMulti<I, O> named(String name) {
      return new BoundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags, fn);
    }

    /**
     * Returns a new multi-output {@code ParDo} {@code PTransform}
     * that's like this {@code PTransform} but with the specified side
     * inputs.  Does not modify this {@code PTransform}.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public BoundMulti<I, O> withSideInputs(
        PCollectionView<?, ?>... sideInputs) {
      return new BoundMulti<>(
          name, ImmutableList.copyOf(sideInputs),
          mainOutputTag, sideOutputTags, fn);
    }

    /**
     * Returns a new multi-output {@code ParDo} {@code PTransform}
     * that's like this {@code PTransform} but with the specified side
     * inputs.  Does not modify this {@code PTransform}.
     *
     * <p> See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public BoundMulti<I, O> withSideInputs(
        Iterable<? extends PCollectionView<?, ?>> sideInputs) {
      return new BoundMulti<>(
          name, ImmutableList.copyOf(sideInputs),
          mainOutputTag, sideOutputTags, fn);
    }


    @Override
    public PCollectionTuple apply(PCollection<? extends I> input) {
      PCollectionTuple outputs = PCollectionTuple.ofPrimitiveOutputsInternal(
          TupleTagList.of(mainOutputTag).and(sideOutputTags.getAll()),
          getInput().getWindowFn());

      // The fn will likely be an instance of an anonymous subclass
      // such as DoFn<Integer, String> { }, thus will have a high-fidelity
      // TypeToken for the output type.
      outputs.get(mainOutputTag).setTypeTokenInternal(fn.getOutputTypeToken());

      return outputs;
    }

    @Override
    protected Coder<O> getDefaultOutputCoder() {
      throw new RuntimeException(
          "internal error: shouldn't be calling this on a multi-output ParDo");
    }

    @Override
    protected String getDefaultName() {
      return StringUtils.approximateSimpleName(fn.getClass());
    }

    @Override
    protected String getKindString() { return "ParMultiDo"; }

    public DoFn<I, O> getFn() {
      return fn;
    }

    public TupleTag<O> getMainOutputTag() {
      return mainOutputTag;
    }

    public List<PCollectionView<?, ?>> getSideInputs() {
      return sideInputs;
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  static {
    DirectPipelineRunner.registerDefaultTransformEvaluator(
        Bound.class,
        new DirectPipelineRunner.TransformEvaluator<Bound>() {
          @Override
          public void evaluate(
              Bound transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateSingleHelper(transform, context);
          }
        });
  }

  private static <I, O> void evaluateSingleHelper(
      Bound<I, O> transform,
      DirectPipelineRunner.EvaluationContext context) {
    TupleTag<O> mainOutputTag = new TupleTag<>("out");

    DirectModeExecutionContext executionContext = new DirectModeExecutionContext();

    DoFnRunner<I, O, List> fnRunner =
        evaluateHelper(transform.fn, context.getStepName(transform),
            transform.getInput(), transform.sideInputs,
            mainOutputTag, new ArrayList<TupleTag<?>>(),
            context, executionContext);

    context.setPCollectionValuesWithMetadata(
        transform.getOutput(),
        executionContext.getOutput(mainOutputTag));
  }

  /////////////////////////////////////////////////////////////////////////////

  static {
    DirectPipelineRunner.registerDefaultTransformEvaluator(
        BoundMulti.class,
        new DirectPipelineRunner.TransformEvaluator<BoundMulti>() {
          @Override
          public void evaluate(
              BoundMulti transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateMultiHelper(transform, context);
          }
        });
  }

  private static <I, O> void evaluateMultiHelper(
      BoundMulti<I, O> transform,
      DirectPipelineRunner.EvaluationContext context) {

    DirectModeExecutionContext executionContext = new DirectModeExecutionContext();

    DoFnRunner<I, O, List> fnRunner =
        evaluateHelper(transform.fn, context.getStepName(transform),
                       transform.getInput(), transform.sideInputs,
                       transform.mainOutputTag, transform.sideOutputTags.getAll(),
                       context, executionContext);

    for (Map.Entry<TupleTag<?>, PCollection<?>> entry
        : transform.getOutput().getAll().entrySet()) {
      TupleTag<Object> tag = (TupleTag<Object>) entry.getKey();
      @SuppressWarnings("unchecked")
      PCollection<Object> pc = (PCollection<Object>) entry.getValue();

      context.setPCollectionValuesWithMetadata(
          pc,
          (tag == transform.mainOutputTag
              ? executionContext.getOutput(tag)
              : executionContext.getSideOutput(tag)));
    }
  }

  private static <I, O> DoFnRunner<I, O, List> evaluateHelper(
      DoFn<I, O> doFn,
      String name,
      PCollection<? extends I> input,
      List<PCollectionView<?, ?>> sideInputs,
      TupleTag<O> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      DirectPipelineRunner.EvaluationContext context,
      DirectModeExecutionContext executionContext) {
    // TODO: Run multiple shards?
    DoFn<I, O> fn = context.ensureSerializable(doFn);

    PTuple sideInputValues = PTuple.empty();
    for (PCollectionView<?, ?> view : sideInputs) {
      sideInputValues = sideInputValues.and(
          view.getTagInternal(),
          context.getPCollectionView(view));
    }

    DoFnRunner<I, O, List> fnRunner =
        DoFnRunner.createWithListOutputs(
            context.getPipelineOptions(),
            fn,
            sideInputValues,
            mainOutputTag,
            sideOutputTags,
            executionContext.getStepContext(name),
            context.getAddCounterMutator(),
            input.getWindowFn());

    fnRunner.startBundle();

    for (DirectPipelineRunner.ValueWithMetadata<? extends I> elem
             : context.getPCollectionValuesWithMetadata(input)) {
      if (doFn instanceof DoFn.RequiresKeyedState) {
        // If the DoFn needs keyed state, set the implicit keys to the keys in the input elements.
        if (!(elem.getValue() instanceof KV)) {
          throw new IllegalStateException(
              name + " marked as 'RequiresKeyedState' but input elements were not of type KV.");
        }
        executionContext.setKey(((KV) elem.getValue()).getKey());
      } else {
        executionContext.setKey(elem.getKey());
      }
      fnRunner.processElement((WindowedValue<I>) elem.getWindowedValue());
    }

    fnRunner.finishBundle();

    return fnRunner;
  }
}
