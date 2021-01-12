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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code PTransform<InputT, OutputT>} is an operation that takes an {@code InputT} (some subtype
 * of {@link PInput}) and produces an {@code OutputT} (some subtype of {@link POutput}).
 *
 * <p>Common PTransforms include root PTransforms like {@link org.apache.beam.sdk.io.TextIO.Read},
 * {@link Create}, processing and conversion operations like {@link ParDo}, {@link GroupByKey},
 * {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, {@link Combine}, and {@link Count}, and
 * outputting PTransforms like {@link org.apache.beam.sdk.io.TextIO.Write}. Users also define their
 * own application-specific composite PTransforms.
 *
 * <p>Each {@code PTransform<InputT, OutputT>} has a single {@code InputT} type and a single {@code
 * OutputT} type. Many PTransforms conceptually transform one input value to one output value, and
 * in this case {@code InputT} and {@code Output} are typically instances of {@link
 * org.apache.beam.sdk.values.PCollection}. A root PTransform conceptually has no input; in this
 * case, conventionally a {@link org.apache.beam.sdk.values.PBegin} object produced by calling
 * {@link Pipeline#begin} is used as the input. An outputting PTransform conceptually has no output;
 * in this case, conventionally {@link org.apache.beam.sdk.values.PDone} is used as its output type.
 * Some PTransforms conceptually have multiple inputs and/or outputs; in these cases special
 * "bundling" classes like {@link org.apache.beam.sdk.values.PCollectionList}, {@link
 * org.apache.beam.sdk.values.PCollectionTuple} are used to combine multiple values into a single
 * bundle for passing into or returning from the PTransform.
 *
 * <p>A {@code PTransform<InputT, OutputT>} is invoked by calling {@code apply()} on its {@code
 * InputT}, returning its {@code OutputT}. Calls can be chained to concisely create linear pipeline
 * segments. For example:
 *
 * <pre>{@code
 * PCollection<T1> pc1 = ...;
 * PCollection<T2> pc2 =
 *     pc1.apply(ParDo.of(new MyDoFn<T1,KV<K,V>>()))
 *        .apply(GroupByKey.<K, V>create())
 *        .apply(Combine.perKey(new MyKeyedCombineFn<K,V>()))
 *        .apply(ParDo.of(new MyDoFn2<KV<K,V>,T2>()));
 * }</pre>
 *
 * <p>PTransform operations have unique names, which are used by the system when explaining what's
 * going on during optimization and execution. Each PTransform gets a system-provided default name,
 * but it's a good practice to specify a more informative explicit name when applying the transform.
 * For example:
 *
 * <pre>{@code
 * ...
 * .apply("Step1", ParDo.of(new MyDoFn3()))
 * ...
 * }</pre>
 *
 * <p>Each PCollection output produced by a PTransform, either directly or within a "bundling"
 * class, automatically gets its own name derived from the name of its producing PTransform.
 *
 * <p>Each PCollection output produced by a PTransform also records a {@link
 * org.apache.beam.sdk.coders.Coder} that specifies how the elements of that PCollection are to be
 * encoded as a byte string, if necessary. The PTransform may provide a default Coder for any of its
 * outputs, for instance by deriving it from the PTransform input's Coder. If the PTransform does
 * not specify the Coder for an output PCollection, the system will attempt to infer a Coder for it,
 * based on what's known at run-time about the Java type of the output's elements. The enclosing
 * {@link Pipeline}'s {@link org.apache.beam.sdk.coders.CoderRegistry} (accessible via {@link
 * Pipeline#getCoderRegistry}) defines the mapping from Java types to the default Coder to use, for
 * a standard set of Java types; users can extend this mapping for additional types, via {@link
 * org.apache.beam.sdk.coders.CoderRegistry#registerCoderProvider}. If this inference process fails,
 * either because the Java type was not known at run-time (e.g., due to Java's "erasure" of generic
 * types) or there was no default Coder registered, then the Coder should be specified manually by
 * calling {@link PCollection#setCoder} on the output PCollection. The Coder of every output
 * PCollection must be determined one way or another before that output is used as an input to
 * another PTransform, or before the enclosing Pipeline is run.
 *
 * <p>A small number of PTransforms are implemented natively by the Apache Beam SDK; such
 * PTransforms simply return an output value as their apply implementation. The majority of
 * PTransforms are implemented as composites of other PTransforms. Such a PTransform subclass
 * typically just implements {@link #expand}, computing its Output value from its {@code InputT}
 * value. User programs are encouraged to use this mechanism to modularize their own code. Such
 * composite abstractions get their own name, and navigating through the composition hierarchy of
 * PTransforms is supported by the monitoring interface. Examples of composite PTransforms can be
 * found in this directory and in examples. From the caller's point of view, there is no distinction
 * between a PTransform implemented natively and one implemented in terms of other PTransforms; both
 * kinds of PTransform are invoked in the same way, using {@code apply()}.
 *
 * <h3>Note on Serialization</h3>
 *
 * <p>{@code PTransform} doesn't actually support serialization, despite implementing {@code
 * Serializable}.
 *
 * <p>{@code PTransform} is marked {@code Serializable} solely because it is common for an anonymous
 * {@link DoFn}, instance to be created within an {@code apply()} method of a composite {@code
 * PTransform}.
 *
 * <p>Each of those {@code *Fn}s is {@code Serializable}, but unfortunately its instance state will
 * contain a reference to the enclosing {@code PTransform} instance, and so attempt to serialize the
 * {@code PTransform} instance, even though the {@code *Fn} instance never references anything about
 * the enclosing {@code PTransform}.
 *
 * <p>To allow such anonymous {@code *Fn}s to be written conveniently, {@code PTransform} is marked
 * as {@code Serializable}, and includes dummy {@code writeObject()} and {@code readObject()}
 * operations that do not save or restore any state.
 *
 * @see <a href= "https://beam.apache.org/documentation/programming-guide/#transforms" >Applying
 *     Transformations</a>
 * @param <InputT> the type of the input to this PTransform
 * @param <OutputT> the type of the output of this PTransform
 */
public abstract class PTransform<InputT extends PInput, OutputT extends POutput>
    implements Serializable /* See the note above */, HasDisplayData {
  /**
   * Override this method to specify how this {@code PTransform} should be expanded on the given
   * {@code InputT}.
   *
   * <p>NOTE: This method should not be called directly. Instead apply the {@code PTransform} should
   * be applied to the {@code InputT} using the {@code apply} method.
   *
   * <p>Composite transforms, which are defined in terms of other transforms, should return the
   * output of one of the composed transforms. Non-composite transforms, which do not apply any
   * transforms internally, should return a new unbound output and register evaluators (via
   * backend-specific registration methods).
   */
  public abstract OutputT expand(InputT input);

  /**
   * Called before running the Pipeline to verify this transform is fully and correctly specified.
   *
   * <p>By default, does nothing.
   */
  public void validate(@Nullable PipelineOptions options) {}

  /**
   * Returns all {@link PValue PValues} that are consumed as inputs to this {@link PTransform} that
   * are independent of the expansion of the {@link InputT} within {@link #expand(PInput)}.
   *
   * <p>For example, this can contain any side input consumed by this {@link PTransform}.
   */
  public Map<TupleTag<?>, PValue> getAdditionalInputs() {
    return Collections.emptyMap();
  }

  /**
   * Returns the transform name.
   *
   * <p>This name is provided by the transform creator and is not required to be unique.
   */
  public String getName() {
    return name != null ? name : getKindString();
  }

  /////////////////////////////////////////////////////////////////////////////

  // See the note about about PTransform's fake Serializability, to
  // understand why all of its instance state is transient.

  /**
   * The base name of this {@code PTransform}, e.g., from defaults, or {@code null} if not yet
   * assigned.
   */
  protected final transient @Nullable String name;

  protected PTransform() {
    this.name = null;
  }

  protected PTransform(@Nullable String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    if (name == null) {
      return getKindString();
    } else {
      return getName() + " [" + getKindString() + "]";
    }
  }

  /**
   * Returns the name to use by default for this {@code PTransform} (not including the names of any
   * enclosing {@code PTransform}s).
   *
   * <p>By default, returns the base name of this {@code PTransform}'s class.
   *
   * <p>The caller is responsible for ensuring that names of applied {@code PTransform}s are unique,
   * e.g., by adding a uniquifying suffix when needed.
   */
  protected String getKindString() {
    if (getClass().isAnonymousClass()) {
      return "AnonymousTransform";
    } else {
      return NameUtils.approximatePTransformName(getClass());
    }
  }

  private void writeObject(ObjectOutputStream oos) {
    // We don't really want to be serializing this object, but we
    // often have serializable anonymous DoFns nested within a
    // PTransform.
  }

  private void readObject(ObjectInputStream oos) {
    // We don't really want to be serializing this object, but we
    // often have serializable anonymous DoFns nested within a
    // PTransform.
  }

  /**
   * Returns the default {@code Coder} to use for the output of this single-output {@code
   * PTransform}.
   *
   * <p>By default, always throws
   *
   * @throws CannotProvideCoderException if no coder can be inferred
   * @deprecated Instead, the PTransform should explicitly call {@link PCollection#setCoder} on the
   *     returned PCollection.
   */
  @Deprecated
  protected Coder<?> getDefaultOutputCoder() throws CannotProvideCoderException {
    throw new CannotProvideCoderException("PTransform.getOutputCoder called.");
  }

  /**
   * Returns the default {@code Coder} to use for the output of this single-output {@code
   * PTransform} when applied to the given input.
   *
   * <p>By default, always throws.
   *
   * @throws CannotProvideCoderException if none can be inferred.
   * @deprecated Instead, the PTransform should explicitly call {@link PCollection#setCoder} on the
   *     returned PCollection.
   */
  @Deprecated
  protected Coder<?> getDefaultOutputCoder(@SuppressWarnings("unused") InputT input)
      throws CannotProvideCoderException {
    return getDefaultOutputCoder();
  }

  /**
   * Returns the default {@code Coder} to use for the given output of this single-output {@code
   * PTransform} when applied to the given input.
   *
   * <p>By default, always throws.
   *
   * @throws CannotProvideCoderException if none can be inferred.
   * @deprecated Instead, the PTransform should explicitly call {@link PCollection#setCoder} on the
   *     returned PCollection.
   */
  @Deprecated
  public <T> Coder<T> getDefaultOutputCoder(
      InputT input, @SuppressWarnings("unused") PCollection<T> output)
      throws CannotProvideCoderException {
    @SuppressWarnings("unchecked")
    Coder<T> defaultOutputCoder = (Coder<T>) getDefaultOutputCoder(input);
    return defaultOutputCoder;
  }

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method to
   * provide their own display data.
   */
  @Override
  public void populateDisplayData(Builder builder) {}

  /**
   * For a {@code SerializableFunction<InputT, OutputT>} {@code fn}, returns a {@code PTransform}
   * given by applying {@code fn.apply(v)} to the input {@code PCollection<InputT>}.
   *
   * <p>Allows users to define a concise composite transform using a Java 8 lambda expression. For
   * example:
   *
   * <pre>{@code
   * PCollection<String> words = wordsAndErrors.apply(
   *   (PCollectionTuple input) -> {
   *     input.get(errorsTag).apply(new WriteErrorOutput());
   *     return input.get(wordsTag);
   *   });
   * }</pre>
   */
  @Experimental
  public static <InputT extends PInput, OutputT extends POutput>
      PTransform<InputT, OutputT> compose(SerializableFunction<InputT, OutputT> fn) {
    return new PTransform<InputT, OutputT>() {
      @Override
      public OutputT expand(InputT input) {
        return fn.apply(input);
      }
    };
  }

  /** Like {@link #compose(SerializableFunction)}, but with a custom name. */
  @Experimental
  public static <InputT extends PInput, OutputT extends POutput>
      PTransform<InputT, OutputT> compose(String name, SerializableFunction<InputT, OutputT> fn) {
    return new PTransform<InputT, OutputT>(name) {
      @Override
      public OutputT expand(InputT input) {
        return fn.apply(input);
      }
    };
  }
}
