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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.ConvertHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.transforms.DoFn.WindowedContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.DisplayData.ItemSpec;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.FieldAccessDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.MethodWithExtraParameters;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.OnTimerMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SchemaElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SideInputParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link ParDo} is the core element-wise transform in Apache Beam, invoking a user-specified
 * function on each of the elements of the input {@link PCollection} to produce zero or more output
 * elements, all of which are collected into the output {@link PCollection}.
 *
 * <p>Elements are processed independently, and possibly in parallel across distributed cloud
 * resources.
 *
 * <p>The {@link ParDo} processing style is similar to what happens inside the "Mapper" or "Reducer"
 * class of a MapReduce-style algorithm.
 *
 * <h2>{@link DoFn DoFns}</h2>
 *
 * <p>The function to use to process each element is specified by a {@link DoFn DoFn&lt;InputT,
 * OutputT&gt;}, primarily via its {@link DoFn.ProcessElement ProcessElement} method. The {@link
 * DoFn} may also provide a {@link DoFn.StartBundle StartBundle} and {@link DoFn.FinishBundle
 * finishBundle} method.
 *
 * <p>Conceptually, when a {@link ParDo} transform is executed, the elements of the input {@link
 * PCollection} are first divided up into some number of "bundles". These are farmed off to
 * distributed worker machines (or run locally, if using the {@code DirectRunner}). For each bundle
 * of input elements processing proceeds as follows:
 *
 * <ol>
 *   <li>If required, a fresh instance of the argument {@link DoFn} is created on a worker, and the
 *       {@link DoFn.Setup} method is called on this instance. This may be through deserialization
 *       or other means. A {@link PipelineRunner} may reuse {@link DoFn} instances for multiple
 *       bundles. A {@link DoFn} that has terminated abnormally (by throwing an {@link Exception})
 *       will never be reused.
 *   <li>The {@link DoFn DoFn's} {@link DoFn.StartBundle} method, if provided, is called to
 *       initialize it.
 *   <li>The {@link DoFn DoFn's} {@link DoFn.ProcessElement} method is called on each of the input
 *       elements in the bundle.
 *   <li>The {@link DoFn DoFn's} {@link DoFn.FinishBundle} method, if provided, is called to
 *       complete its work. After {@link DoFn.FinishBundle} is called, the framework will not again
 *       invoke {@link DoFn.ProcessElement} or {@link DoFn.FinishBundle} until a new call to {@link
 *       DoFn.StartBundle} has occurred.
 *   <li>If any of {@link DoFn.Setup}, {@link DoFn.StartBundle}, {@link DoFn.ProcessElement} or
 *       {@link DoFn.FinishBundle} methods throw an exception, the {@link DoFn.Teardown} method, if
 *       provided, will be called on the {@link DoFn} instance.
 *   <li>If a runner will no longer use a {@link DoFn}, the {@link DoFn.Teardown} method, if
 *       provided, will be called on the discarded instance.
 *   <li>If a bundle requested bundle finalization by registering a {@link
 *       DoFn.BundleFinalizer.Callback bundle finalization callback}, the callback will be invoked
 *       after the runner has successfully committed the output of a successful bundle.
 * </ol>
 *
 * <p>Note also that calls to {@link DoFn.Teardown} are best effort, and may not be called before a
 * {@link DoFn} is discarded in the general case. As a result, use of the {@link DoFn.Teardown}
 * method to perform side effects is not appropriate, because the elements that produced the side
 * effect will not be replayed in case of failure, and those side effects are permanently lost.
 *
 * <p>Each of the calls to any of the {@link DoFn DoFn's} processing methods can produce zero or
 * more output elements. All of the of output elements from all of the {@link DoFn} instances are
 * included in an output {@link PCollection}.
 *
 * <p>For example:
 *
 * <pre>{@code PCollection<String> lines = ...;
 * PCollection<String> words =
 *     lines.apply(ParDo.of(new DoFn<String, String>() }{
 *        {@code @ProcessElement
 *         public void processElement(@Element String line,
 *           OutputReceiver<String> r) {
 *           for (String word : line.split("[^a-zA-Z']+")) {
 *             r.output(word);
 *           }
 *         }}}));
 * {@code PCollection<Integer> wordLengths =
 *     words.apply(ParDo.of(new DoFn<String, Integer>() }{
 *        {@code @ProcessElement
 *         public void processElement(@Element String word,
 *           OutputReceiver<Integer> r) {
 *           Integer length = word.length();
 *           r.output(length);
 *         }}}));
 * </pre>
 *
 * <p>Each output element has the same timestamp and is in the same windows as its corresponding
 * input element, and the output {@code PCollection} has the same {@link WindowFn} associated with
 * it as the input.
 *
 * <h2>Naming {@link ParDo ParDo} transforms</h2>
 *
 * <p>The name of a transform is used to provide a name for any node in the {@link Pipeline} graph
 * resulting from application of the transform. It is best practice to provide a name at the time of
 * application, via {@link PCollection#apply(String, PTransform)}. Otherwise, a unique name - which
 * may not be stable across pipeline revision - will be generated, based on the transform name.
 *
 * <p>For example:
 *
 * <pre>{@code PCollection<String> words =
 *     lines.apply("ExtractWords", ParDo.of(new DoFn<String, String>() { ... }));
 * PCollection<Integer> wordLengths =
 *     words.apply("ComputeWordLengths", ParDo.of(new DoFn<String, Integer>() { ... }));
 * }</pre>
 *
 * <h2>Side Inputs</h2>
 *
 * <p>While a {@link ParDo} processes elements from a single "main input" {@link PCollection}, it
 * can take additional "side input" {@link PCollectionView PCollectionViews}. These side input
 * {@link PCollectionView PCollectionViews} express styles of accessing {@link PCollection
 * PCollections} computed by earlier pipeline operations, passed in to the {@link ParDo} transform
 * using {@link SingleOutput#withSideInputs}, and their contents accessible to each of the {@link
 * DoFn} operations via {@link DoFn.ProcessContext#sideInput sideInput}. For example:
 *
 * <pre>{@code PCollection<String> words = ...;
 * PCollection<Integer> maxWordLengthCutOff = ...; // Singleton PCollection
 * final PCollectionView<Integer> maxWordLengthCutOffView =
 *     maxWordLengthCutOff.apply(View.<Integer>asSingleton());
 * PCollection<String> wordsBelowCutOff =
 *     words.apply(ParDo.of(new DoFn<String, String>() }{
 *        {@code @ProcessElement
 *         public void processElement(ProcessContext c) {
 *             String word = c.element();
 *             int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
 *             if (word.length() <= lengthCutOff) {
 *                 c.output(word);
 *             }
 *         }}}).withSideInputs(maxWordLengthCutOffView));
 * </pre>
 *
 * <h2>Additional Outputs</h2>
 *
 * <p>Optionally, a {@link ParDo} transform can produce multiple output {@link PCollection
 * PCollections}, both a "main output" {@code PCollection<OutputT>} plus any number of additional
 * output {@link PCollection PCollections}, each keyed by a distinct {@link TupleTag}, and bundled
 * in a {@link PCollectionTuple}. The {@link TupleTag TupleTags} to be used for the output {@link
 * PCollectionTuple} are specified by invoking {@link SingleOutput#withOutputTags}. Unconsumed
 * outputs do not necessarily need to be explicitly specified, even if the {@link DoFn} generates
 * them. Within the {@link DoFn}, an element is added to the main output {@link PCollection} as
 * normal, using {@link WindowedContext#output(Object)}, while an element is added to any additional
 * output {@link PCollection} using {@link WindowedContext#output(TupleTag, Object)}. For example:
 *
 * <pre>{@code PCollection<String> words = ...;
 * // Select words whose length is below a cut off,
 * // plus the lengths of words that are above the cut off.
 * // Also select words starting with "MARKER".
 * final int wordLengthCutOff = 10;
 * // Create tags to use for the main and additional outputs.
 * final TupleTag<String> wordsBelowCutOffTag =
 *     new TupleTag<String>(){};
 * final TupleTag<Integer> wordLengthsAboveCutOffTag =
 *     new TupleTag<Integer>(){};
 * final TupleTag<String> markedWordsTag =
 *     new TupleTag<String>(){};
 * PCollectionTuple results =
 *     words.apply(
 *         ParDo
 *         .of(new DoFn<String, String>() {
 *             // Create a tag for the unconsumed output.
 *             final TupleTag<String> specialWordsTag =
 *                 new TupleTag<String>(){};}}
 *            {@code @ProcessElement
 *             public void processElement(@Element String word, MultiOutputReceiver r) {
 *               if (word.length() <= wordLengthCutOff) {
 *                 // Emit this short word to the main output.
 *                 r.get(wordsBelowCutOffTag).output(word);
 *               } else {
 *                 // Emit this long word's length to a specified output.
 *                 r.get(wordLengthsAboveCutOffTag).output(word.length());
 *               }
 *               if (word.startsWith("MARKER")) {
 *                 // Emit this word to a different specified output.
 *                 r.get(markedWordsTag).output(word);
 *               }
 *               if (word.startsWith("SPECIAL")) {
 *                 // Emit this word to the unconsumed output.
 *                 r.get(specialWordsTag).output(word);
 *               }
 *             }}})
 *             // Specify the main and consumed output tags of the
 *             // PCollectionTuple result:
 *         .withOutputTags(wordsBelowCutOffTag,
 *             TupleTagList.of(wordLengthsAboveCutOffTag)
 *                         .and(markedWordsTag)));
 * // Extract the PCollection results, by tag.{@code
 * PCollection<String> wordsBelowCutOff =
 *     results.get(wordsBelowCutOffTag);
 * PCollection<Integer> wordLengthsAboveCutOff =
 *     results.get(wordLengthsAboveCutOffTag);
 * PCollection<String> markedWords =
 *     results.get(markedWordsTag);
 * }</pre>
 *
 * <h2>Output Coders</h2>
 *
 * <p>By default, the {@link Coder Coder&lt;OutputT&gt;} for the elements of the main output {@link
 * PCollection PCollection&lt;OutputT&gt;} is inferred from the concrete type of the {@link DoFn
 * DoFn&lt;InputT, OutputT&gt;}.
 *
 * <p>By default, the {@link Coder Coder&lt;AdditionalOutputT&gt;} for the elements of an output
 * {@link PCollection PCollection&lt;AdditionalOutputT&gt;} is inferred from the concrete type of
 * the corresponding {@link TupleTag TupleTag&lt;AdditionalOutputT&gt;}. To be successful, the
 * {@link TupleTag} should be created as an instance of a trivial anonymous subclass, with {@code
 * {}} suffixed to the constructor call. Such uses block Java's generic type parameter inference, so
 * the {@code <X>} argument must be provided explicitly. For example:
 *
 * <pre>{@code
 * // A TupleTag to use for a side input can be written concisely:
 * final TupleTag<Integer> sideInputag = new TupleTag<>();
 * // A TupleTag to use for an output should be written with "{}",
 * // and explicit generic parameter type:
 * final TupleTag<String> additionalOutputTag = new TupleTag<String>(){};
 * }</pre>
 *
 * This style of {@code TupleTag} instantiation is used in the example of {@link ParDo ParDos} that
 * produce multiple outputs, above.
 *
 * <h2>Serializability of {@link DoFn DoFns}</h2>
 *
 * <p>A {@link DoFn} passed to a {@link ParDo} transform must be {@link Serializable}. This allows
 * the {@link DoFn} instance created in this "main program" to be sent (in serialized form) to
 * remote worker machines and reconstituted for bundles of elements of the input {@link PCollection}
 * being processed. A {@link DoFn} can have instance variable state, and non-transient instance
 * variable state will be serialized in the main program and then deserialized on remote worker
 * machines for some number of bundles of elements to process.
 *
 * <p>{@link DoFn DoFns} expressed as anonymous inner classes can be convenient, but due to a quirk
 * in Java's rules for serializability, non-static inner or nested classes (including anonymous
 * inner classes) automatically capture their enclosing class's instance in their serialized state.
 * This can lead to including much more than intended in the serialized state of a {@link DoFn}, or
 * even things that aren't {@link Serializable}.
 *
 * <p>There are two ways to avoid unintended serialized state in a {@link DoFn}:
 *
 * <ul>
 *   <li>Define the {@link DoFn} as a named, static class.
 *   <li>Define the {@link DoFn} as an anonymous inner class inside of a static method.
 * </ul>
 *
 * <p>Both of these approaches ensure that there is no implicit enclosing instance serialized along
 * with the {@link DoFn} instance.
 *
 * <p>Prior to Java 8, any local variables of the enclosing method referenced from within an
 * anonymous inner class need to be marked as {@code final}. If defining the {@link DoFn} as a named
 * static class, such variables would be passed as explicit constructor arguments and stored in
 * explicit instance variables.
 *
 * <p>There are three main ways to initialize the state of a {@link DoFn} instance processing a
 * bundle:
 *
 * <ul>
 *   <li>Define instance variable state (including implicit instance variables holding final
 *       variables captured by an anonymous inner class), initialized by the {@link DoFn}'s
 *       constructor (which is implicit for an anonymous inner class). This state will be
 *       automatically serialized and then deserialized in the {@link DoFn} instances created for
 *       bundles. This method is good for state known when the original {@link DoFn} is created in
 *       the main program, if it's not overly large. This is not suitable for any state which must
 *       only be used for a single bundle, as {@link DoFn DoFn's} may be used to process multiple
 *       bundles.
 *   <li>Compute the state as a singleton {@link PCollection} and pass it in as a side input to the
 *       {@link DoFn}. This is good if the state needs to be computed by the pipeline, or if the
 *       state is very large and so is best read from file(s) rather than sent as part of the {@link
 *       DoFn DoFn's} serialized state.
 *   <li>Initialize the state in each {@link DoFn} instance, in a {@link DoFn.Setup} method. This is
 *       good if the initialization doesn't depend on any information known only by the main program
 *       or computed by earlier pipeline operations, but is the same for all instances of this
 *       {@link DoFn} for all program executions, say setting up empty caches or initializing
 *       constant data.
 * </ul>
 *
 * <h2>No Global Shared State</h2>
 *
 * <p>{@link ParDo} operations are intended to be able to run in parallel across multiple worker
 * machines. This precludes easy sharing and updating mutable state across those machines. There is
 * no support in the Beam model for communicating and synchronizing updates to shared state across
 * worker machines, so programs should not access any mutable static variable state in their {@link
 * DoFn}, without understanding that the Java processes for the main program and workers will each
 * have its own independent copy of such state, and there won't be any automatic copying of that
 * state across Java processes. All information should be communicated to {@link DoFn} instances via
 * main and side inputs and serialized state, and all output should be communicated from a {@link
 * DoFn} instance via output {@link PCollection PCollections}, in the absence of external
 * communication mechanisms written by user code.
 *
 * <h2>Fault Tolerance</h2>
 *
 * <p>In a distributed system, things can fail: machines can crash, machines can be unable to
 * communicate across the network, etc. While individual failures are rare, the larger the job, the
 * greater the chance that something, somewhere, will fail. Beam runners may strive to mask such
 * failures by retrying failed {@link DoFn} bundle. This means that a {@link DoFn} instance might
 * process a bundle partially, then crash for some reason, then be rerun (often in a new JVM) on
 * that same bundle and on the same elements as before. Sometimes two or more {@link DoFn} instances
 * will be running on the same bundle simultaneously, with the system taking the results of the
 * first instance to complete successfully. Consequently, the code in a {@link DoFn} needs to be
 * written such that these duplicate (sequential or concurrent) executions do not cause problems. If
 * the outputs of a {@link DoFn} are a pure function of its inputs, then this requirement is
 * satisfied. However, if a {@link DoFn DoFn's} execution has external side-effects, such as
 * performing updates to external HTTP services, then the {@link DoFn DoFn's} code needs to take
 * care to ensure that those updates are idempotent and that concurrent updates are acceptable. This
 * property can be difficult to achieve, so it is advisable to strive to keep {@link DoFn DoFns} as
 * pure functions as much as possible.
 *
 * <h2>Optimization</h2>
 *
 * <p>Beam runners may choose to apply optimizations to a pipeline before it is executed. A key
 * optimization, <i>fusion</i>, relates to {@link ParDo} operations. If one {@link ParDo} operation
 * produces a {@link PCollection} that is then consumed as the main input of another {@link ParDo}
 * operation, the two {@link ParDo} operations will be <i>fused</i> together into a single ParDo
 * operation and run in a single pass; this is "producer-consumer fusion". Similarly, if two or more
 * ParDo operations have the same {@link PCollection} main input, they will be fused into a single
 * {@link ParDo} that makes just one pass over the input {@link PCollection}; this is "sibling
 * fusion".
 *
 * <p>If after fusion there are no more unfused references to a {@link PCollection} (e.g., one
 * between a producer ParDo and a consumer {@link ParDo}), the {@link PCollection} itself is "fused
 * away" and won't ever be written to disk, saving all the I/O and space expense of constructing it.
 *
 * <p>When Beam runners apply fusion optimization, it is essentially "free" to write {@link ParDo}
 * operations in a very modular, composable style, each {@link ParDo} operation doing one clear
 * task, and stringing together sequences of {@link ParDo} operations to get the desired overall
 * effect. Such programs can be easier to understand, easier to unit-test, easier to extend and
 * evolve, and easier to reuse in new programs. The predefined library of PTransforms that come with
 * Beam makes heavy use of this modular, composable style, trusting to the runner to "flatten out"
 * all the compositions into highly optimized stages.
 *
 * @see <a href= "https://beam.apache.org/documentation/programming-guide/#pardo"> the web
 *     documentation for ParDo</a>
 */
public class ParDo {

  /**
   * Creates a {@link ParDo} {@link PTransform} that will invoke the given {@link DoFn} function.
   *
   * <p>The resulting {@link PTransform PTransform} is ready to be applied, or further properties
   * can be set on it first.
   */
  public static <InputT, OutputT> SingleOutput<InputT, OutputT> of(DoFn<InputT, OutputT> fn) {
    validate(fn);
    return new SingleOutput<>(fn, Collections.emptyMap(), displayDataForFn(fn));
  }

  private static <T> DisplayData.ItemSpec<? extends Class<?>> displayDataForFn(T fn) {
    return DisplayData.item("fn", fn.getClass()).withLabel("Transform Function");
  }

  private static void finishSpecifyingStateSpecs(
      DoFn<?, ?> fn,
      CoderRegistry coderRegistry,
      SchemaRegistry schemaRegistry,
      Coder<?> inputCoder) {
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
    Map<String, DoFnSignature.StateDeclaration> stateDeclarations = signature.stateDeclarations();
    for (DoFnSignature.StateDeclaration stateDeclaration : stateDeclarations.values()) {
      try {
        StateSpec<?> stateSpec = (StateSpec<?>) stateDeclaration.field().get(fn);
        Coder[] coders;
        try {
          coders = schemasForStateSpecTypes(stateDeclaration, schemaRegistry);
        } catch (NoSuchSchemaException e) {
          coders = codersForStateSpecTypes(stateDeclaration, coderRegistry, inputCoder);
        }
        stateSpec.offerCoders(coders);
        stateSpec.finishSpecifying();
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static void validateStateApplicableForInput(DoFn<?, ?> fn, PCollection<?> input) {
    Coder<?> inputCoder = input.getCoder();
    checkArgument(
        inputCoder instanceof KvCoder,
        "%s requires its input to use %s in order to use state and timers.",
        ParDo.class.getSimpleName(),
        KvCoder.class.getSimpleName());

    KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) inputCoder;
    try {
      kvCoder.getKeyCoder().verifyDeterministic();
    } catch (Coder.NonDeterministicException exc) {
      throw new IllegalArgumentException(
          String.format(
              "%s requires a deterministic key coder in order to use state and timers",
              ParDo.class.getSimpleName()));
    }
  }

  private static void validateSideInputTypes(
      Map<String, PCollectionView<?>> sideInputs, DoFn<?, ?> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
    DoFnSignature.ProcessElementMethod processElementMethod = signature.processElement();
    for (SideInputParameter sideInput : processElementMethod.getSideInputParameters()) {
      PCollectionView<?> view = sideInputs.get(sideInput.sideInputId());
      checkArgument(
          view != null,
          "the ProcessElement method expects a side input identified with the tag %s, but no such side input was"
              + " supplied. Use withSideInput(String, PCollectionView) to supply this side input.",
          sideInput.sideInputId());
      TypeDescriptor<?> viewType = view.getViewFn().getTypeDescriptor();

      // Currently check that the types exactly match, even if the types are convertible.
      checkArgument(
          viewType.equals(sideInput.elementT()),
          "Side Input with tag %s and type %s cannot be bound to ProcessElement parameter with type %s",
          sideInput.sideInputId(),
          viewType,
          sideInput.elementT());
    }
  }

  private static FieldAccessDescriptor getFieldAccessDescriptorFromParameter(
      @Nullable String fieldAccessString,
      Schema inputSchema,
      Map<String, FieldAccessDeclaration> fieldAccessDeclarations,
      DoFn<?, ?> fn) {

    // Resolve the FieldAccessDescriptor against the Schema.
    // This will be resolved anyway by the runner, however we want any resolution errors
    // (i.e. caused by a FieldAccessDescriptor that references fields not in the schema) to
    // be caught and presented to the user at graph-construction time. Therefore we resolve
    // here as well to catch these errors.
    FieldAccessDescriptor fieldAccessDescriptor = null;
    if (fieldAccessString == null) {
      // This is the case where no FieldId is defined. Default to all fields accessed.
      fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();
    } else {
      // If there is a FieldAccessDescriptor in the class with this id, use that.
      FieldAccessDeclaration fieldAccessDeclaration =
          fieldAccessDeclarations.get(fieldAccessString);
      if (fieldAccessDeclaration != null) {
        checkArgument(fieldAccessDeclaration.field().getType().equals(FieldAccessDescriptor.class));
        try {
          fieldAccessDescriptor = (FieldAccessDescriptor) fieldAccessDeclaration.field().get(fn);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      } else {
        // Otherwise, interpret the string as a field-name expression.
        fieldAccessDescriptor = FieldAccessDescriptor.withFieldNames(fieldAccessString);
      }
    }
    return fieldAccessDescriptor.resolve(inputSchema);
  }

  private static SchemaCoder[] schemasForStateSpecTypes(
      DoFnSignature.StateDeclaration stateDeclaration, SchemaRegistry schemaRegistry)
      throws NoSuchSchemaException {
    Type stateType = stateDeclaration.stateType().getType();

    if (!(stateType instanceof ParameterizedType)) {
      // No type arguments means no coders to infer.
      return new SchemaCoder[0];
    }

    Type[] typeArguments = ((ParameterizedType) stateType).getActualTypeArguments();
    SchemaCoder[] coders = new SchemaCoder[typeArguments.length];

    for (int i = 0; i < typeArguments.length; i++) {
      Type typeArgument = typeArguments[i];
      TypeDescriptor typeDescriptor = TypeDescriptor.of(typeArgument);
      coders[i] =
          SchemaCoder.of(
              schemaRegistry.getSchema(typeDescriptor),
              typeDescriptor,
              schemaRegistry.getToRowFunction(typeDescriptor),
              schemaRegistry.getFromRowFunction(typeDescriptor));
    }
    return coders;
  }

  /**
   * Try to provide coders for as many of the type arguments of given {@link
   * DoFnSignature.StateDeclaration} as possible.
   */
  private static <InputT> Coder[] codersForStateSpecTypes(
      DoFnSignature.StateDeclaration stateDeclaration,
      CoderRegistry coderRegistry,
      Coder<InputT> inputCoder) {
    Type stateType = stateDeclaration.stateType().getType();

    if (!(stateType instanceof ParameterizedType)) {
      // No type arguments means no coders to infer.
      return new Coder[0];
    }

    Type[] typeArguments = ((ParameterizedType) stateType).getActualTypeArguments();
    Coder[] coders = new Coder[typeArguments.length];

    for (int i = 0; i < typeArguments.length; i++) {
      Type typeArgument = typeArguments[i];
      TypeDescriptor<?> typeDescriptor = TypeDescriptor.of(typeArgument);
      try {
        coders[i] = coderRegistry.getCoder(typeDescriptor);
      } catch (CannotProvideCoderException e) {
        try {
          coders[i] =
              coderRegistry.getCoder(
                  typeDescriptor, inputCoder.getEncodedTypeDescriptor(), inputCoder);
        } catch (CannotProvideCoderException ignored) {
          // Since not all type arguments will have a registered coder we ignore this exception.
        }
      }
    }

    return coders;
  }

  /**
   * Perform common validations of the {@link DoFn} against the input {@link PCollection}, for
   * example ensuring that the window type expected by the {@link DoFn} matches the window type of
   * the {@link PCollection}.
   */
  private static <InputT, OutputT> void validateWindowType(
      PCollection<? extends InputT> input, DoFn<InputT, OutputT> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature((Class) fn.getClass());

    TypeDescriptor<? extends BoundedWindow> actualWindowT =
        input.getWindowingStrategy().getWindowFn().getWindowTypeDescriptor();

    validateWindowTypeForMethod(actualWindowT, signature.processElement());
    for (OnTimerMethod method : signature.onTimerMethods().values()) {
      validateWindowTypeForMethod(actualWindowT, method);
    }
    for (DoFnSignature.OnTimerFamilyMethod method : signature.onTimerFamilyMethods().values()) {
      validateWindowTypeForMethod(actualWindowT, method);
    }
  }

  private static void validateWindowTypeForMethod(
      TypeDescriptor<? extends BoundedWindow> actualWindowT,
      MethodWithExtraParameters methodSignature) {
    if (methodSignature.windowT() != null) {
      checkArgument(
          methodSignature.windowT().isSupertypeOf(actualWindowT),
          "%s unable to provide window -- expected window type from parameter (%s) is not a "
              + "supertype of actual window type assigned by windowing (%s)",
          methodSignature.targetMethod(),
          methodSignature.windowT(),
          actualWindowT);
    }
  }

  /**
   * Perform common validations of the {@link DoFn}, for example ensuring that state is used
   * correctly and that its features can be supported.
   */
  private static <InputT, OutputT> void validate(DoFn<InputT, OutputT> fn) {
    DoFnSignature signature = DoFnSignatures.getSignature((Class) fn.getClass());

    // State is semantically incompatible with splitting
    if (!signature.stateDeclarations().isEmpty() && signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is splittable and uses state, but these are not compatible",
              fn.getClass().getName()));
    }

    // Timers are semantically incompatible with splitting
    if ((!signature.timerDeclarations().isEmpty() || !signature.timerFamilyDeclarations().isEmpty())
        && signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is splittable and uses timers, but these are not compatible",
              fn.getClass().getName()));
    }

    // TimerFamily is semantically incompatible with splitting
    if (!signature.timerFamilyDeclarations().isEmpty()
        && signature.processElement().isSplittable()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s is splittable and uses timer family, but these are not compatible",
              fn.getClass().getName()));
    }
  }
  /**
   * Extract information on how the DoFn uses schemas. In particular, if the schema of an element
   * parameter does not match the input PCollection's schema, convert.
   */
  @Internal
  public static DoFnSchemaInformation getDoFnSchemaInformation(
      DoFn<?, ?> fn, PCollection<?> input) {
    DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
    DoFnSignature.ProcessElementMethod processElementMethod = signature.processElement();
    if (!processElementMethod.getSchemaElementParameters().isEmpty()) {
      if (!input.hasSchema()) {
        throw new IllegalArgumentException("Type of @Element must match the DoFn type" + input);
      }
    }

    SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
    DoFnSchemaInformation doFnSchemaInformation = DoFnSchemaInformation.create();
    for (SchemaElementParameter parameter : processElementMethod.getSchemaElementParameters()) {
      TypeDescriptor<?> elementT = parameter.elementT();
      FieldAccessDescriptor accessDescriptor =
          getFieldAccessDescriptorFromParameter(
              parameter.fieldAccessString(),
              input.getSchema(),
              signature.fieldAccessDeclarations(),
              fn);
      Schema selectedSchema = SelectHelpers.getOutputSchema(input.getSchema(), accessDescriptor);
      ConvertHelpers.ConvertedSchemaInformation converted =
          ConvertHelpers.getConvertedSchemaInformation(selectedSchema, elementT, schemaRegistry);
      if (converted.outputSchemaCoder != null) {
        doFnSchemaInformation =
            doFnSchemaInformation.withSelectFromSchemaParameter(
                (SchemaCoder<?>) input.getCoder(),
                accessDescriptor,
                selectedSchema,
                converted.outputSchemaCoder,
                converted.unboxedType != null);
      } else {
        // If the selected schema is a Row containing a single primitive type (which is the output
        // of Select when selecting a primitive), attempt to unbox it and match against the
        // parameter.
        checkArgument(converted.unboxedType != null);
        doFnSchemaInformation =
            doFnSchemaInformation.withUnboxPrimitiveParameter(
                (SchemaCoder<?>) input.getCoder(), accessDescriptor, selectedSchema, elementT);
      }
    }

    return doFnSchemaInformation;
  }

  /**
   * A {@link PTransform} that, when applied to a {@code PCollection<InputT>}, invokes a
   * user-specified {@code DoFn<InputT, OutputT>} on all its elements, with all its outputs
   * collected into an output {@code PCollection<OutputT>}.
   *
   * <p>A multi-output form of this transform can be created with {@link
   * SingleOutput#withOutputTags}.
   *
   * @param <InputT> the type of the (main) input {@link PCollection} elements
   * @param <OutputT> the type of the (main) output {@link PCollection} elements
   */
  public static class SingleOutput<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {

    private static final String MAIN_OUTPUT_TAG = "output";

    private final Map<String, PCollectionView<?>> sideInputs;
    private final DoFn<InputT, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;

    SingleOutput(
        DoFn<InputT, OutputT> fn,
        Map<String, PCollectionView<?>> sideInputs,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.sideInputs = sideInputs;
    }
    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this {@link PTransform} but with
     * the specified additional side inputs. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above for more explanation.
     */
    public SingleOutput<InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this {@link PTransform} but with
     * the specified additional side inputs. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above for more explanation.
     */
    public SingleOutput<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      Map<String, PCollectionView<?>> mappedInputs =
          StreamSupport.stream(sideInputs.spliterator(), false)
              .collect(Collectors.toMap(v -> v.getTagInternal().getId(), v -> v));
      return withSideInputs(mappedInputs);
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this {@link PTransform} but with
     * the specified additional side inputs. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Side Inputs above for more explanation.
     */
    public SingleOutput<InputT, OutputT> withSideInputs(
        Map<String, PCollectionView<?>> sideInputs) {
      return new SingleOutput<>(
          fn,
          ImmutableMap.<String, PCollectionView<?>>builder()
              .putAll(this.sideInputs)
              .putAll(sideInputs)
              .build(),
          fnDisplayData);
    }

    /**
     * Returns a new {@link ParDo} {@link PTransform} that's like this {@link PTransform} but with
     * the specified additional side inputs. Does not modify this {@link PTransform}.
     */
    public SingleOutput<InputT, OutputT> withSideInput(
        String tagId, PCollectionView<?> pCollectionView) {
      return withSideInputs(Collections.singletonMap(tagId, pCollectionView));
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform} that's like this {@link
     * PTransform} but with the specified output tags. Does not modify this {@link PTransform}.
     *
     * <p>See the discussion of Additional Outputs above for more explanation.
     */
    public MultiOutput<InputT, OutputT> withOutputTags(
        TupleTag<OutputT> mainOutputTag, TupleTagList additionalOutputTags) {
      return new MultiOutput<>(fn, sideInputs, mainOutputTag, additionalOutputTags, fnDisplayData);
    }

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      finishSpecifyingStateSpecs(fn, coderRegistry, schemaRegistry, input.getCoder());
      TupleTag<OutputT> mainOutput = new TupleTag<>(MAIN_OUTPUT_TAG);
      PCollection<OutputT> res =
          input.apply(withOutputTags(mainOutput, TupleTagList.empty())).get(mainOutput);

      TypeDescriptor<OutputT> outputTypeDescriptor = getFn().getOutputTypeDescriptor();
      try {
        res.setSchema(
            schemaRegistry.getSchema(outputTypeDescriptor),
            outputTypeDescriptor,
            schemaRegistry.getToRowFunction(outputTypeDescriptor),
            schemaRegistry.getFromRowFunction(outputTypeDescriptor));
      } catch (NoSuchSchemaException e) {
        try {
          res.setCoder(
              coderRegistry.getCoder(
                  outputTypeDescriptor,
                  getFn().getInputTypeDescriptor(),
                  ((PCollection<InputT>) input).getCoder()));
        } catch (CannotProvideCoderException e2) {
          // Ignore and leave coder unset.
        }
      }

      return res;
    }

    @Override
    protected String getKindString() {
      return String.format("ParDo(%s)", NameUtils.approximateSimpleName(getFn()));
    }

    /**
     * {@inheritDoc}
     *
     * <p>{@link ParDo} registers its internal {@link DoFn} as a subcomponent for display data.
     * {@link DoFn} implementations can register display data by overriding {@link
     * DoFn#populateDisplayData}.
     */
    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      ParDo.populateDisplayData(builder, fn, fnDisplayData);
    }

    public DoFn<InputT, OutputT> getFn() {
      return fn;
    }

    public Map<String, PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    /**
     * Returns the side inputs of this {@link ParDo}, tagged with the tag of the {@link
     * PCollectionView}. The values of the returned map will be equal to the result of {@link
     * #getSideInputs()}.
     */
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs.values());
    }

    @Override
    public String toString() {
      return fn.toString();
    }
  }

  /**
   * A {@link PTransform} that, when applied to a {@code PCollection<InputT>}, invokes a
   * user-specified {@code DoFn<InputT, OutputT>} on all its elements, which can emit elements to
   * any of the {@link PTransform}'s output {@code PCollection}s, which are bundled into a result
   * {@code PCollectionTuple}.
   *
   * @param <InputT> the type of the (main) input {@code PCollection} elements
   * @param <OutputT> the type of the main output {@code PCollection} elements
   */
  public static class MultiOutput<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollectionTuple> {
    private final Map<String, PCollectionView<?>> sideInputs;
    private final TupleTag<OutputT> mainOutputTag;
    private final TupleTagList additionalOutputTags;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final DoFn<InputT, OutputT> fn;

    MultiOutput(
        DoFn<InputT, OutputT> fn,
        Map<String, PCollectionView<?>> sideInputs,
        TupleTag<OutputT> mainOutputTag,
        TupleTagList additionalOutputTags,
        ItemSpec<? extends Class<?>> fnDisplayData) {
      this.sideInputs = sideInputs;
      this.mainOutputTag = mainOutputTag;
      this.additionalOutputTags = additionalOutputTags;
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform} that's like this {@link
     * PTransform} but with the specified additional side inputs. Does not modify this {@link
     * PTransform}.
     *
     * <p>See the discussion of Side Inputs above for more explanation.
     */
    public MultiOutput<InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    public MultiOutput<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      Map<String, PCollectionView<?>> mappedInputs =
          StreamSupport.stream(sideInputs.spliterator(), false)
              .collect(Collectors.toMap(v -> v.getTagInternal().getId(), v -> v));
      return withSideInputs(mappedInputs);
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform} that's like this {@link
     * PTransform} but with the specified additional side inputs. Does not modify this {@link
     * PTransform}.
     *
     * <p>See the discussion of Side Inputs above for more explanation.
     */
    public MultiOutput<InputT, OutputT> withSideInputs(Map<String, PCollectionView<?>> sideInputs) {
      return new MultiOutput<>(
          fn,
          ImmutableMap.<String, PCollectionView<?>>builder()
              .putAll(this.sideInputs)
              .putAll(sideInputs)
              .build(),
          mainOutputTag,
          additionalOutputTags,
          fnDisplayData);
    }

    /**
     * Returns a new multi-output {@link ParDo} {@link PTransform} that's like this {@link
     * PTransform} but with the specified additional side inputs. Does not modify this {@link
     * PTransform}.
     */
    public MultiOutput<InputT, OutputT> withSideInput(
        String tagId, PCollectionView<?> pCollectionView) {
      return withSideInputs(Collections.singletonMap(tagId, pCollectionView));
    }

    @Override
    public PCollectionTuple expand(PCollection<? extends InputT> input) {
      // SplittableDoFn should be forbidden on the runner-side.
      validateWindowType(input, fn);

      // Use coder registry to determine coders for all StateSpec defined in the fn signature.
      CoderRegistry coderRegistry = input.getPipeline().getCoderRegistry();
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      finishSpecifyingStateSpecs(fn, coderRegistry, schemaRegistry, input.getCoder());

      DoFnSignature signature = DoFnSignatures.getSignature(fn.getClass());
      if (signature.usesState() || signature.usesTimers()) {
        validateStateApplicableForInput(fn, input);
      }

      validateSideInputTypes(sideInputs, fn);

      // TODO: We should validate OutputReceiver<Row> only happens if the output PCollection
      // as schema. However coder/schema inference may not have happened yet at this point.
      // Need to figure out where to validate this.

      PCollectionTuple outputs =
          PCollectionTuple.ofPrimitiveOutputsInternal(
              input.getPipeline(),
              TupleTagList.of(mainOutputTag).and(additionalOutputTags.getAll()),
              // TODO
              Collections.emptyMap(),
              input.getWindowingStrategy(),
              input.isBounded().and(signature.isBoundedPerElement()));
      @SuppressWarnings("unchecked")
      Coder<InputT> inputCoder = ((PCollection<InputT>) input).getCoder();
      for (PCollection<?> out : outputs.getAll().values()) {
        try {
          out.setCoder(
              (Coder)
                  coderRegistry.getCoder(
                      out.getTypeDescriptor(), getFn().getInputTypeDescriptor(), inputCoder));
        } catch (CannotProvideCoderException e) {
          // Ignore and let coder inference happen later.
        }
      }

      // The fn will likely be an instance of an anonymous subclass
      // such as DoFn<Integer, String> { }, thus will have a high-fidelity
      // TypeDescriptor for the output type.
      outputs.get(mainOutputTag).setTypeDescriptor(getFn().getOutputTypeDescriptor());

      return outputs;
    }

    @Override
    protected String getKindString() {
      return String.format("ParMultiDo(%s)", NameUtils.approximateSimpleName(getFn()));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      ParDo.populateDisplayData(builder, fn, fnDisplayData);
    }

    public DoFn<InputT, OutputT> getFn() {
      return fn;
    }

    public TupleTag<OutputT> getMainOutputTag() {
      return mainOutputTag;
    }

    public TupleTagList getAdditionalOutputTags() {
      return additionalOutputTags;
    }

    public Map<String, PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    /**
     * Returns the side inputs of this {@link ParDo}, tagged with the tag of the {@link
     * PCollectionView}. The values of the returned map will be equal to the result of {@link
     * #getSideInputs()}.
     */
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs.values());
    }

    @Override
    public String toString() {
      return fn.toString();
    }
  }

  private static String stateDescription(StateSpec<?> spec) {
    return spec.match(
        new StateSpec.Cases<String>() {
          @Override
          public String dispatchValue(Coder<?> valueCoder) {
            return "ValueState<" + valueCoder + ">";
          }

          @Override
          public String dispatchBag(Coder<?> elementCoder) {
            return "BagState<" + elementCoder + ">";
          }

          @Override
          public String dispatchCombining(
              Combine.CombineFn<?, ?, ?> combineFn, Coder<?> accumCoder) {
            return "CombiningState<" + accumCoder + ">";
          }

          @Override
          public String dispatchMap(Coder<?> keyCoder, Coder<?> valueCoder) {
            return "MapState<" + keyCoder + ", " + valueCoder + ">";
          }

          @Override
          public String dispatchSet(Coder<?> elementCoder) {
            return "SetState<" + elementCoder + ">";
          }
        });
  }

  private static void populateDisplayData(
      DisplayData.Builder builder,
      DoFn<?, ?> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    builder.include("fn", fn).add(fnDisplayData);
    for (DoFnSignature.StateDeclaration stateDeclaration :
        DoFnSignatures.signatureForDoFn(fn).stateDeclarations().values()) {
      try {
        StateSpec<?> stateSpec = (StateSpec<?>) stateDeclaration.field().get(fn);
        builder.add(
            DisplayData.item("state_" + stateDeclaration.id(), stateDescription(stateSpec))
                .withLabel("State \"" + stateDeclaration.id() + "\""));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static boolean isSplittable(DoFn<?, ?> fn) {
    return DoFnSignatures.signatureForDoFn(fn).processElement().isSplittable();
  }
}
