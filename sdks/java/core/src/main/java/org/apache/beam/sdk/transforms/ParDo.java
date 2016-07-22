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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypedPValue;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * {@link ParDo} is the core element-wise transform in Google Cloud
 * Dataflow, invoking a user-specified function on each of the elements of the input
 * {@link PCollection} to produce zero or more output elements, all
 * of which are collected into the output {@link PCollection}.
 *
 * <p>Elements are processed independently, and possibly in parallel across
 * distributed cloud resources.
 *
 * <p>The {@link ParDo} processing style is similar to what happens inside
 * the "Mapper" or "Reducer" class of a MapReduce-style algorithm.
 *
 * <h2>{@link OldDoFn DoFns}</h2>
 *
 * <p>The function to use to process each element is specified by a
 * {@link OldDoFn OldDoFn&lt;InputT, OutputT&gt;}, primarily via its
 * {@link OldDoFn#processElement processElement} method. The {@link OldDoFn} may also
 * override the default implementations of {@link OldDoFn#startBundle startBundle}
 * and {@link OldDoFn#finishBundle finishBundle}.
 *
 * <p>Conceptually, when a {@link ParDo} transform is executed, the
 * elements of the input {@link PCollection} are first divided up
 * into some number of "bundles". These are farmed off to distributed
 * worker machines (or run locally, if using the {@code DirectRunner}).
 * For each bundle of input elements processing proceeds as follows:
 *
 * <ol>
 *   <li>If required, a fresh instance of the argument {@link OldDoFn} is created
 *     on a worker. This may be through deserialization or other means. A
 *     {@link PipelineRunner} may reuse {@link OldDoFn} instances for multiple bundles.
 *     A {@link OldDoFn} that has terminated abnormally (by throwing an {@link Exception}
 *     will never be reused.</li>
 *   <li>The {@link OldDoFn OldDoFn's} {@link OldDoFn#startBundle} method is called to
 *     initialize it. If this method is not overridden, the call may be optimized
 *     away.</li>
 *   <li>The {@link OldDoFn OldDoFn's} {@link OldDoFn#processElement} method
 *     is called on each of the input elements in the bundle.</li>
 *   <li>The {@link OldDoFn OldDoFn's} {@link OldDoFn#finishBundle} method is called
 *     to complete its work. After {@link OldDoFn#finishBundle} is called, the
 *     framework will not again invoke {@link OldDoFn#processElement} or
 *     {@link OldDoFn#finishBundle}
 *     until a new call to {@link OldDoFn#startBundle} has occurred.
 *     If this method is not overridden, this call may be optimized away.</li>
 * </ol>
 *
 * Each of the calls to any of the {@link OldDoFn OldDoFn's} processing
 * methods can produce zero or more output elements. All of the
 * of output elements from all of the {@link OldDoFn} instances
 * are included in the output {@link PCollection}.
 *
 * <p>For example:
 *
 * <pre> {@code
 * PCollection<String> lines = ...;
 * PCollection<String> words =
 *     lines.apply(ParDo.of(new OldDoFn<String, String>() {
 *         public void processElement(ProcessContext c) {
 *           String line = c.element();
 *           for (String word : line.split("[^a-zA-Z']+")) {
 *             c.output(word);
 *           }
 *         }}));
 * PCollection<Integer> wordLengths =
 *     words.apply(ParDo.of(new OldDoFn<String, Integer>() {
 *         public void processElement(ProcessContext c) {
 *           String word = c.element();
 *           Integer length = word.length();
 *           c.output(length);
 *         }}));
 * } </pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows
 * as its corresponding input element, and the output {@code PCollection}
 * has the same {@link WindowFn} associated with it as the input.
 *
 * <h2>Naming {@link ParDo ParDo} transforms</h2>
 *
 * <p>The name of a transform is used to provide a name for any node in the
 * {@link Pipeline} graph resulting from application of the transform.
 * It is best practice to provide a name at the time of application,
 * via {@link PCollection#apply(String, PTransform)}. Otherwise,
 * a unique name - which may not be stable across pipeline revision -
 * will be generated, based on the transform name.
 *
 * <p>For example:
 *
 * <pre> {@code
 * PCollection<String> words =
 *     lines.apply("ExtractWords", ParDo.of(new OldDoFn<String, String>() { ... }));
 * PCollection<Integer> wordLengths =
 *     words.apply("ComputeWordLengths", ParDo.of(new OldDoFn<String, Integer>() { ... }));
 * } </pre>
 *
 * <h2>Side Inputs</h2>
 *
 * <p>While a {@link ParDo} processes elements from a single "main input"
 * {@link PCollection}, it can take additional "side input"
 * {@link PCollectionView PCollectionViews}. These side input
 * {@link PCollectionView PCollectionViews} express styles of accessing
 * {@link PCollection PCollections} computed by earlier pipeline operations,
 * passed in to the {@link ParDo} transform using
 * {@link #withSideInputs}, and their contents accessible to each of
 * the {@link OldDoFn} operations via {@link OldDoFn.ProcessContext#sideInput sideInput}.
 * For example:
 *
 * <pre> {@code
 * PCollection<String> words = ...;
 * PCollection<Integer> maxWordLengthCutOff = ...; // Singleton PCollection
 * final PCollectionView<Integer> maxWordLengthCutOffView =
 *     maxWordLengthCutOff.apply(View.<Integer>asSingleton());
 * PCollection<String> wordsBelowCutOff =
 *     words.apply(ParDo.withSideInputs(maxWordLengthCutOffView)
 *                      .of(new OldDoFn<String, String>() {
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
 * <p>Optionally, a {@link ParDo} transform can produce multiple
 * output {@link PCollection PCollections}, both a "main output"
 * {@code PCollection<OutputT>} plus any number of "side output"
 * {@link PCollection PCollections}, each keyed by a distinct {@link TupleTag},
 * and bundled in a {@link PCollectionTuple}. The {@link TupleTag TupleTags}
 * to be used for the output {@link PCollectionTuple} are specified by
 * invoking {@link #withOutputTags}. Unconsumed side outputs do not
 * necessarily need to be explicitly specified, even if the {@link OldDoFn}
 * generates them. Within the {@link OldDoFn}, an element is added to the
 * main output {@link PCollection} as normal, using
 * {@link OldDoFn.Context#output}, while an element is added to a side output
 * {@link PCollection} using {@link OldDoFn.Context#sideOutput}. For example:
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
 *         .of(new OldDoFn<String, String>() {
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
 * <p>Several properties can be specified for a {@link ParDo}
 * {@link PTransform}, including name, side inputs, side output tags,
 * and {@link OldDoFn} to invoke. Only the {@link OldDoFn} is required; the
 * name is encouraged but not required, and side inputs and side
 * output tags are only specified when they're needed. These
 * properties can be specified in any order, as long as they're
 * specified before the {@link ParDo} {@link PTransform} is applied.
 *
 * <p>The approach used to allow these properties to be specified in
 * any order, with some properties omitted, is to have each of the
 * property "setter" methods defined as static factory methods on
 * {@link ParDo} itself, which return an instance of either
 * {@link ParDo.Unbound} or
 * {@link ParDo.Bound} nested classes, each of which offer
 * property setter instance methods to enable setting additional
 * properties. {@link ParDo.Bound} is used for {@link ParDo}
 * transforms whose {@link OldDoFn} is specified and whose input and
 * output static types have been bound. {@link ParDo.Unbound ParDo.Unbound} is used
 * for {@link ParDo} transforms that have not yet had their
 * {@link OldDoFn} specified. Only {@link ParDo.Bound} instances can be
 * applied.
 *
 * <p>Another benefit of this approach is that it reduces the number
 * of type parameters that need to be specified manually. In
 * particular, the input and output types of the {@link ParDo}
 * {@link PTransform} are inferred automatically from the type
 * parameters of the {@link OldDoFn} argument passed to {@link ParDo#of}.
 *
 * <h2>Output Coders</h2>
 *
 * <p>By default, the {@link Coder Coder&lt;OutputT&gt;} for the
 * elements of the main output {@link PCollection PCollection&lt;OutputT&gt;} is
 * inferred from the concrete type of the {@link OldDoFn OldDoFn&lt;InputT, OutputT&gt;}.
 *
 * <p>By default, the {@link Coder Coder&lt;SideOutputT&gt;} for the elements of
 * a side output {@link PCollection PCollection&lt;SideOutputT&gt;} is inferred
 * from the concrete type of the corresponding {@link TupleTag TupleTag&lt;SideOutputT&gt;}.
 * To be successful, the {@link TupleTag} should be created as an instance
 * of a trivial anonymous subclass, with {@code {}} suffixed to the
 * constructor call. Such uses block Java's generic type parameter
 * inference, so the {@code <X>} argument must be provided explicitly.
 * For example:
 * <pre> {@code
 * // A TupleTag to use for a side input can be written concisely:
 * final TupleTag<Integer> sideInputag = new TupleTag<>();
 * // A TupleTag to use for a side output should be written with "{}",
 * // and explicit generic parameter type:
 * final TupleTag<String> sideOutputTag = new TupleTag<String>(){};
 * } </pre>
 * This style of {@code TupleTag} instantiation is used in the example of
 * multiple side outputs, above.
 *
 * <h2>Serializability of {@link OldDoFn DoFns}</h2>
 *
 * <p>A {@link OldDoFn} passed to a {@link ParDo} transform must be
 * {@link Serializable}. This allows the {@link OldDoFn} instance
 * created in this "main program" to be sent (in serialized form) to
 * remote worker machines and reconstituted for bundles of elements
 * of the input {@link PCollection} being processed. A {@link OldDoFn}
 * can have instance variable state, and non-transient instance
 * variable state will be serialized in the main program and then
 * deserialized on remote worker machines for some number of bundles
 * of elements to process.
 *
 * <p>{@link OldDoFn DoFns} expressed as anonymous inner classes can be
 * convenient, but due to a quirk in Java's rules for serializability,
 * non-static inner or nested classes (including anonymous inner
 * classes) automatically capture their enclosing class's instance in
 * their serialized state. This can lead to including much more than
 * intended in the serialized state of a {@link OldDoFn}, or even things
 * that aren't {@link Serializable}.
 *
 * <p>There are two ways to avoid unintended serialized state in a
 * {@link OldDoFn}:
 *
 * <ul>
 *
 * <li>Define the {@link OldDoFn} as a named, static class.
 *
 * <li>Define the {@link OldDoFn} as an anonymous inner class inside of
 * a static method.
 *
 * </ul>
 *
 * <p>Both of these approaches ensure that there is no implicit enclosing
 * instance serialized along with the {@link OldDoFn} instance.
 *
 * <p>Prior to Java 8, any local variables of the enclosing
 * method referenced from within an anonymous inner class need to be
 * marked as {@code final}. If defining the {@link OldDoFn} as a named
 * static class, such variables would be passed as explicit
 * constructor arguments and stored in explicit instance variables.
 *
 * <p>There are three main ways to initialize the state of a
 * {@link OldDoFn} instance processing a bundle:
 *
 * <ul>
 *
 * <li>Define instance variable state (including implicit instance
 * variables holding final variables captured by an anonymous inner
 * class), initialized by the {@link OldDoFn}'s constructor (which is
 * implicit for an anonymous inner class). This state will be
 * automatically serialized and then deserialized in the {@code OldDoFn}
 * instances created for bundles. This method is good for state
 * known when the original {@code OldDoFn} is created in the main
 * program, if it's not overly large. This is not suitable for any
 * state which must only be used for a single bundle, as {@link OldDoFn OldDoFn's}
 * may be used to process multiple bundles.
 *
 * <li>Compute the state as a singleton {@link PCollection} and pass it
 * in as a side input to the {@link OldDoFn}. This is good if the state
 * needs to be computed by the pipeline, or if the state is very large
 * and so is best read from file(s) rather than sent as part of the
 * {@code OldDoFn}'s serialized state.
 *
 * <li>Initialize the state in each {@link OldDoFn} instance, in
 * {@link OldDoFn#startBundle}. This is good if the initialization
 * doesn't depend on any information known only by the main program or
 * computed by earlier pipeline operations, but is the same for all
 * instances of this {@link OldDoFn} for all program executions, say
 * setting up empty caches or initializing constant data.
 *
 * </ul>
 *
 * <h2>No Global Shared State</h2>
 *
 * <p>{@link ParDo} operations are intended to be able to run in
 * parallel across multiple worker machines. This precludes easy
 * sharing and updating mutable state across those machines. There is
 * no support in the Google Cloud Dataflow system for communicating
 * and synchronizing updates to shared state across worker machines,
 * so programs should not access any mutable static variable state in
 * their {@link OldDoFn}, without understanding that the Java processes
 * for the main program and workers will each have its own independent
 * copy of such state, and there won't be any automatic copying of
 * that state across Java processes. All information should be
 * communicated to {@link OldDoFn} instances via main and side inputs and
 * serialized state, and all output should be communicated from a
 * {@link OldDoFn} instance via main and side outputs, in the absence of
 * external communication mechanisms written by user code.
 *
 * <h2>Fault Tolerance</h2>
 *
 * <p>In a distributed system, things can fail: machines can crash,
 * machines can be unable to communicate across the network, etc.
 * While individual failures are rare, the larger the job, the greater
 * the chance that something, somewhere, will fail. The Google Cloud
 * Dataflow service strives to mask such failures automatically,
 * principally by retrying failed {@link OldDoFn} bundle. This means
 * that a {@code OldDoFn} instance might process a bundle partially, then
 * crash for some reason, then be rerun (often on a different worker
 * machine) on that same bundle and on the same elements as before.
 * Sometimes two or more {@link OldDoFn} instances will be running on the
 * same bundle simultaneously, with the system taking the results of
 * the first instance to complete successfully. Consequently, the
 * code in a {@link OldDoFn} needs to be written such that these
 * duplicate (sequential or concurrent) executions do not cause
 * problems. If the outputs of a {@link OldDoFn} are a pure function of
 * its inputs, then this requirement is satisfied. However, if a
 * {@link OldDoFn OldDoFn's} execution has external side-effects, such as performing
 * updates to external HTTP services, then the {@link OldDoFn OldDoFn's} code
 * needs to take care to ensure that those updates are idempotent and
 * that concurrent updates are acceptable. This property can be
 * difficult to achieve, so it is advisable to strive to keep
 * {@link OldDoFn DoFns} as pure functions as much as possible.
 *
 * <h2>Optimization</h2>
 *
 * <p>The Google Cloud Dataflow service automatically optimizes a
 * pipeline before it is executed. A key optimization, <i>fusion</i>,
 * relates to {@link ParDo} operations. If one {@link ParDo} operation produces a
 * {@link PCollection} that is then consumed as the main input of another
 * {@link ParDo} operation, the two {@link ParDo} operations will be <i>fused</i>
 * together into a single ParDo operation and run in a single pass;
 * this is "producer-consumer fusion". Similarly, if
 * two or more ParDo operations have the same {@link PCollection} main input,
 * they will be fused into a single {@link ParDo} that makes just one pass
 * over the input {@link PCollection}; this is "sibling fusion".
 *
 * <p>If after fusion there are no more unfused references to a
 * {@link PCollection} (e.g., one between a producer ParDo and a consumer
 * {@link ParDo}), the {@link PCollection} itself is "fused away" and won't ever be
 * written to disk, saving all the I/O and space expense of
 * constructing it.
 *
 * <p>The Google Cloud Dataflow service applies fusion as much as
 * possible, greatly reducing the cost of executing pipelines. As a
 * result, it is essentially "free" to write {@link ParDo} operations in a
 * very modular, composable style, each {@link ParDo} operation doing one
 * clear task, and stringing together sequences of {@link ParDo} operations to
 * get the desired overall effect. Such programs can be easier to
 * understand, easier to unit-test, easier to extend and evolve, and
 * easier to reuse in new programs. The predefined library of
 * PTransforms that come with Google Cloud Dataflow makes heavy use of
 * this modular, composable style, trusting to the Google Cloud
 * Dataflow service's optimizer to "flatten out" all the compositions
 * into highly optimized stages.
 *
 * @see <a href="https://cloud.google.com/dataflow/model/par-do">the web
 * documentation for ParDo</a>
 */
public class ParDo {

  /**
   * Creates a {@link ParDo} {@link PTransform} with the given
   * side inputs.
   *
   * <p>Side inputs are {@link PCollectionView PCollectionViews}, whose contents are
   * computed during pipeline execution and then made accessible to
   * {@link OldDoFn} code via {@link OldDoFn.ProcessContext#sideInput sideInput}. Each
   * invocation of the {@link OldDoFn} receives the same values for these
   * side inputs.
   *
   * <p>See the discussion of Side Inputs above for more explanation.
   *
   * <p>The resulting {@link PTransform} is incomplete, and its
   * input/output types are not yet bound. Use
   * {@link ParDo.Unbound#of} to specify the {@link OldDoFn} to
   * invoke, which will also bind the input/output types of this
   * {@link PTransform}.
   */
  public static Unbound withSideInputs(PCollectionView<?>... sideInputs) {
    return new Unbound().withSideInputs(sideInputs);
  }

  /**
    * Creates a {@link ParDo} with the given side inputs.
    *
   * <p>Side inputs are {@link PCollectionView}s, whose contents are
   * computed during pipeline execution and then made accessible to
   * {@code OldDoFn} code via {@link OldDoFn.ProcessContext#sideInput sideInput}.
   *
   * <p>See the discussion of Side Inputs above for more explanation.
   *
   * <p>The resulting {@link PTransform} is incomplete, and its
   * input/output types are not yet bound. Use
   * {@link ParDo.Unbound#of} to specify the {@link OldDoFn} to
   * invoke, which will also bind the input/output types of this
   * {@link PTransform}.
   */
  public static Unbound withSideInputs(
      Iterable<? extends PCollectionView<?>> sideInputs) {
    return new Unbound().withSideInputs(sideInputs);
  }

  /**
   * Creates a multi-output {@link ParDo} {@link PTransform} whose
   * output {@link PCollection}s will be referenced using the given main
   * output and side output tags.
   *
   * <p>{@link TupleTag TupleTags} are used to name (with its static element
   * type {@code T}) each main and side output {@code PCollection<T>}.
   * This {@link PTransform PTransform's} {@link OldDoFn} emits elements to the main
   * output {@link PCollection} as normal, using
   * {@link OldDoFn.Context#output}. The {@link OldDoFn} emits elements to
   * a side output {@code PCollection} using
   * {@link OldDoFn.Context#sideOutput}, passing that side output's tag
   * as an argument. The result of invoking this {@link PTransform}
   * will be a {@link PCollectionTuple}, and any of the the main and
   * side output {@code PCollection}s can be retrieved from it via
   * {@link PCollectionTuple#get}, passing the output's tag as an
   * argument.
   *
   * <p>See the discussion of Side Outputs above for more explanation.
   *
   * <p>The resulting {@link PTransform} is incomplete, and its input
   * type is not yet bound. Use {@link ParDo.UnboundMulti#of}
   * to specify the {@link OldDoFn} to invoke, which will also bind the
   * input type of this {@link PTransform}.
   */
  public static <OutputT> UnboundMulti<OutputT> withOutputTags(
      TupleTag<OutputT> mainOutputTag,
      TupleTagList sideOutputTags) {
    return new Unbound().withOutputTags(mainOutputTag, sideOutputTags);
  }

  /**
   * Creates a {@link ParDo} {@link PTransform} that will invoke the
   * given {@link OldDoFn} function.
   *
   * <p>The resulting {@link PTransform PTransform's} types have been bound, with the
   * input being a {@code PCollection<InputT>} and the output a
   * {@code PCollection<OutputT>}, inferred from the types of the argument
   * {@code OldDoFn<InputT, OutputT>}. It is ready to be applied, or further
   * properties can be set on it first.
   */
  public static <InputT, OutputT> Bound<InputT, OutputT> of(OldDoFn<InputT, OutputT> fn) {
    return of(fn, fn.getClass());
  }

  private static <InputT, OutputT> Bound<InputT, OutputT> of(
          OldDoFn<InputT, OutputT> fn, Class<?> fnClass) {
    return new Unbound().of(fn, fnClass);
  }

  private static <InputT, OutputT> OldDoFn<InputT, OutputT>
      adapt(DoFnWithContext<InputT, OutputT> fn) {
    return DoFnReflector.of(fn.getClass()).toDoFn(fn);
  }

  /**
   * Creates a {@link ParDo} {@link PTransform} that will invoke the
   * given {@link DoFnWithContext} function.
   *
   * <p>The resulting {@link PTransform PTransform's} types have been bound, with the
   * input being a {@code PCollection<InputT>} and the output a
   * {@code PCollection<OutputT>}, inferred from the types of the argument
   * {@code OldDoFn<InputT, OutputT>}. It is ready to be applied, or further
   * properties can be set on it first.
   *
   * <p>{@link DoFnWithContext} is an experimental alternative to
   * {@link OldDoFn} which simplifies accessing the window of the element.
   */
  @Experimental
  public static <InputT, OutputT> Bound<InputT, OutputT> of(DoFnWithContext<InputT, OutputT> fn) {
    return of(adapt(fn), fn.getClass());
  }

  /**
   * An incomplete {@link ParDo} transform, with unbound input/output types.
   *
   * <p>Before being applied, {@link ParDo.Unbound#of} must be
   * invoked to specify the {@link OldDoFn} to invoke, which will also
   * bind the input/output types of this {@link PTransform}.
   */
  public static class Unbound {
    private final String name;
    private final List<PCollectionView<?>> sideInputs;

    Unbound() {
      this(null, ImmutableList.<PCollectionView<?>>of());
    }

    Unbound(String name, List<PCollectionView<?>> sideInputs) {
      this.name = name;
      this.sideInputs = sideInputs;
    }

    /**
     * Returns a new {@link ParDo} transform that's like this
     * transform but with the specified additional side inputs.
     * Does not modify this transform. The resulting transform is
     * still incomplete.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Unbound withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a new {@link ParDo} transform that is like this
     * transform but with the specified additional side inputs. Does not modify
     * this transform. The resulting transform is still incomplete.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Unbound withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      ImmutableList.Builder<PCollectionView<?>> builder = ImmutableList.builder();
      builder.addAll(this.sideInputs);
      builder.addAll(sideInputs);
      return new Unbound(name, builder.build());
    }

    /**
     * Returns a new multi-output {@link ParDo} transform that's like
     * this transform but with the specified main and side output
     * tags. Does not modify this transform. The resulting transform
     * is still incomplete.
     *
     * <p>See the discussion of Side Outputs above and on
     * {@link ParDo#withOutputTags} for more explanation.
     */
    public <OutputT> UnboundMulti<OutputT> withOutputTags(TupleTag<OutputT> mainOutputTag,
                                              TupleTagList sideOutputTags) {
      return new UnboundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this
     * transform but that will invoke the given {@link OldDoFn}
     * function, and that has its input and output types bound. Does
     * not modify this transform. The resulting {@link PTransform} is
     * sufficiently specified to be applied, but more properties can
     * still be specified.
     */
    public <InputT, OutputT> Bound<InputT, OutputT> of(OldDoFn<InputT, OutputT> fn) {
      return of(fn, fn.getClass());
    }

    private <InputT, OutputT> Bound<InputT, OutputT> of(
        OldDoFn<InputT, OutputT> fn, Class<?> fnClass) {
      return new Bound<>(name, sideInputs, fn, fnClass);
    }


    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this
     * transform but which will invoke the given {@link DoFnWithContext}
     * function, and which has its input and output types bound. Does
     * not modify this transform. The resulting {@link PTransform} is
     * sufficiently specified to be applied, but more properties can
     * still be specified.
     */
    public <InputT, OutputT> Bound<InputT, OutputT> of(DoFnWithContext<InputT, OutputT> fn) {
      return of(adapt(fn), fn.getClass());
    }
  }

  /**
   * A {@link PTransform} that, when applied to a {@code PCollection<InputT>},
   * invokes a user-specified {@code OldDoFn<InputT, OutputT>} on all its elements,
   * with all its outputs collected into an output
   * {@code PCollection<OutputT>}.
   *
   * <p>A multi-output form of this transform can be created with
   * {@link ParDo.Bound#withOutputTags}.
   *
   * @param <InputT> the type of the (main) input {@link PCollection} elements
   * @param <OutputT> the type of the (main) output {@link PCollection} elements
   */
  public static class Bound<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    // Inherits name.
    private final List<PCollectionView<?>> sideInputs;
    private final OldDoFn<InputT, OutputT> fn;
    private final Class<?> fnClass;

    Bound(String name,
          List<PCollectionView<?>> sideInputs,
          OldDoFn<InputT, OutputT> fn,
          Class<?> fnClass) {
      super(name);
      this.sideInputs = sideInputs;
      this.fn = SerializableUtils.clone(fn);
      this.fnClass = fnClass;
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this
     * {@link PTransform} but with the specified additional side inputs. Does not
     * modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Bound<InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this
     * {@link PTransform} but with the specified additional side inputs. Does not
     * modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public Bound<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      ImmutableList.Builder<PCollectionView<?>> builder = ImmutableList.builder();
      builder.addAll(this.sideInputs);
      builder.addAll(sideInputs);
      return new Bound<>(name, builder.build(), fn, fnClass);
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform}
     * that's like this {@link PTransform} but with the specified main
     * and side output tags. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Outputs above and on
     * {@link ParDo#withOutputTags} for more explanation.
     */
    public BoundMulti<InputT, OutputT> withOutputTags(TupleTag<OutputT> mainOutputTag,
                                           TupleTagList sideOutputTags) {
      return new BoundMulti<>(
          name, sideInputs, mainOutputTag, sideOutputTags, fn, fnClass);
    }

    @Override
    public PCollection<OutputT> apply(PCollection<? extends InputT> input) {
      return PCollection.<OutputT>createPrimitiveOutputInternal(
              input.getPipeline(),
              input.getWindowingStrategy(),
              input.isBounded())
          .setTypeDescriptorInternal(fn.getOutputTypeDescriptor());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Coder<OutputT> getDefaultOutputCoder(PCollection<? extends InputT> input)
        throws CannotProvideCoderException {
      return input.getPipeline().getCoderRegistry().getDefaultCoder(
          fn.getOutputTypeDescriptor(),
          fn.getInputTypeDescriptor(),
          ((PCollection<InputT>) input).getCoder());
    }

    @Override
    protected String getKindString() {
      Class<?> clazz = DoFnReflector.getDoFnClass(fn);
      if (clazz.isAnonymousClass()) {
        return "AnonymousParDo";
      } else {
        return String.format("ParDo(%s)", StringUtils.approximateSimpleName(clazz));
      }
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link ParDo} registers its internal {@link OldDoFn} as a subcomponent for display data.
     * {@link OldDoFn} implementations can register display data by overriding
     * {@link OldDoFn#populateDisplayData}.
     */
    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      ParDo.populateDisplayData(builder, fn, fnClass);
    }

    public OldDoFn<InputT, OutputT> getFn() {
      return fn;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }
  }

  /**
   * An incomplete multi-output {@link ParDo} transform, with unbound
   * input type.
   *
   * <p>Before being applied, {@link ParDo.UnboundMulti#of} must be
   * invoked to specify the {@link OldDoFn} to invoke, which will also
   * bind the input type of this {@link PTransform}.
   *
   * @param <OutputT> the type of the main output {@code PCollection} elements
   */
  public static class UnboundMulti<OutputT> {
    private final String name;
    private final List<PCollectionView<?>> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final TupleTagList sideOutputTags;

    UnboundMulti(String name,
                 List<PCollectionView<?>> sideInputs,
                 TupleTag<OutputT> mainOutputTag,
                 TupleTagList sideOutputTags) {
      this.name = name;
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.sideOutputTags = sideOutputTags;
    }

    /**
     * Returns a new multi-output {@link ParDo} transform that's like
     * this transform but with the specified side inputs. Does not
     * modify this transform. The resulting transform is still
     * incomplete.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public UnboundMulti<OutputT> withSideInputs(
        PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a new multi-output {@link ParDo} transform that's like
     * this transform but with the specified additional side inputs. Does not
     * modify this transform. The resulting transform is still
     * incomplete.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public UnboundMulti<OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      ImmutableList.Builder<PCollectionView<?>> builder = ImmutableList.builder();
      builder.addAll(this.sideInputs);
      builder.addAll(sideInputs);
      return new UnboundMulti<>(
          name, builder.build(),
          mainOutputTag, sideOutputTags);
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform}
     * that's like this transform but that will invoke the given
     * {@link OldDoFn} function, and that has its input type bound.
     * Does not modify this transform. The resulting
     * {@link PTransform} is sufficiently specified to be applied, but
     * more properties can still be specified.
     */
    public <InputT> BoundMulti<InputT, OutputT> of(OldDoFn<InputT, OutputT> fn) {
      return of(fn, fn.getClass());
    }

    public <InputT> BoundMulti<InputT, OutputT> of(OldDoFn<InputT, OutputT> fn, Class<?> fnClass) {
      return new BoundMulti<>(
              name, sideInputs, mainOutputTag, sideOutputTags, fn, fnClass);
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform}
     * that's like this transform but which will invoke the given
     * {@link DoFnWithContext} function, and which has its input type bound.
     * Does not modify this transform. The resulting
     * {@link PTransform} is sufficiently specified to be applied, but
     * more properties can still be specified.
     */
    public <InputT> BoundMulti<InputT, OutputT> of(DoFnWithContext<InputT, OutputT> fn) {
      return of(adapt(fn), fn.getClass());
    }
  }

  /**
   * A {@link PTransform} that, when applied to a
   * {@code PCollection<InputT>}, invokes a user-specified
   * {@code OldDoFn<InputT, OutputT>} on all its elements, which can emit elements
   * to any of the {@link PTransform}'s main and side output
   * {@code PCollection}s, which are bundled into a result
   * {@code PCollectionTuple}.
   *
   * @param <InputT> the type of the (main) input {@code PCollection} elements
   * @param <OutputT> the type of the main output {@code PCollection} elements
   */
  public static class BoundMulti<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollectionTuple> {
    // Inherits name.
    private final List<PCollectionView<?>> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final TupleTagList sideOutputTags;
    private final OldDoFn<InputT, OutputT> fn;
    private final Class<?> fnClass;

    BoundMulti(String name,
               List<PCollectionView<?>> sideInputs,
               TupleTag<OutputT> mainOutputTag,
               TupleTagList sideOutputTags,
               OldDoFn<InputT, OutputT> fn,
               Class<?> fnClass) {
      super(name);
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.sideOutputTags = sideOutputTags;
      this.fn = SerializableUtils.clone(fn);
      this.fnClass = fnClass;
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform}
     * that's like this {@link PTransform} but with the specified additional side
     * inputs. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public BoundMulti<InputT, OutputT> withSideInputs(
        PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform}
     * that's like this {@link PTransform} but with the specified additional side
     * inputs. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above and on
     * {@link ParDo#withSideInputs} for more explanation.
     */
    public BoundMulti<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      ImmutableList.Builder<PCollectionView<?>> builder = ImmutableList.builder();
      builder.addAll(this.sideInputs);
      builder.addAll(sideInputs);
      return new BoundMulti<>(
          name, builder.build(),
          mainOutputTag, sideOutputTags, fn, fnClass);
    }


    @Override
    public PCollectionTuple apply(PCollection<? extends InputT> input) {
      PCollectionTuple outputs = PCollectionTuple.ofPrimitiveOutputsInternal(
          input.getPipeline(),
          TupleTagList.of(mainOutputTag).and(sideOutputTags.getAll()),
          input.getWindowingStrategy(),
          input.isBounded());

      // The fn will likely be an instance of an anonymous subclass
      // such as OldDoFn<Integer, String> { }, thus will have a high-fidelity
      // TypeDescriptor for the output type.
      outputs.get(mainOutputTag).setTypeDescriptorInternal(fn.getOutputTypeDescriptor());

      return outputs;
    }

    @Override
    protected Coder<OutputT> getDefaultOutputCoder() {
      throw new RuntimeException(
          "internal error: shouldn't be calling this on a multi-output ParDo");
    }

    @Override
    public <T> Coder<T> getDefaultOutputCoder(
        PCollection<? extends InputT> input, TypedPValue<T> output)
        throws CannotProvideCoderException {
      @SuppressWarnings("unchecked")
      Coder<InputT> inputCoder = ((PCollection<InputT>) input).getCoder();
      return input.getPipeline().getCoderRegistry().getDefaultCoder(
          output.getTypeDescriptor(),
          fn.getInputTypeDescriptor(),
          inputCoder);
      }

    @Override
    protected String getKindString() {
      Class<?> clazz = DoFnReflector.getDoFnClass(fn);
      if (fn.getClass().isAnonymousClass()) {
        return "AnonymousParMultiDo";
      } else {
        return String.format("ParMultiDo(%s)", StringUtils.approximateSimpleName(clazz));
      }
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      ParDo.populateDisplayData(builder, fn, fnClass);
    }

    public OldDoFn<InputT, OutputT> getFn() {
      return fn;
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return mainOutputTag;
    }

    public TupleTagList getSideOutputTags() {
      return sideOutputTags;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }
  }

  private static void populateDisplayData(
      DisplayData.Builder builder, OldDoFn<?, ?> fn, Class<?> fnClass) {
    builder
        .include(fn)
        .add(DisplayData.item("fn", fnClass)
            .withLabel("Transform Function"));
  }
}
