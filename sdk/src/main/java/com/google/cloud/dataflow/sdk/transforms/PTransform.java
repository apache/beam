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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.TypedPValue;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A {@code PTransform<Input, Output>} is an operation that takes an
 * {@code Input} (some subtype of {@link PInput}) and produces an
 * {@code Output} (some subtype of {@link POutput}).
 *
 * <p> Common PTransforms include root PTransforms like
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Read},
 * {@link Create}, processing and
 * conversion operations like {@link ParDo},
 * {@link GroupByKey},
 * {@link com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey},
 * {@link Combine}, and {@link Count}, and outputting
 * PTransforms like
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Write}.  Users also
 * define their own application-specific composite PTransforms.
 *
 * <p> Each {@code PTransform<Input, Output>} has a single
 * {@code Input} type and a single {@code Output} type.  Many
 * PTransforms conceptually transform one input value to one output
 * value, and in this case {@code Input} and {@code Output} are
 * typically instances of
 * {@link com.google.cloud.dataflow.sdk.values.PCollection}.
 * A root
 * PTransform conceptually has no input; in this case, conventionally
 * a {@link com.google.cloud.dataflow.sdk.values.PBegin} object
 * produced by calling {@link Pipeline#begin} is used as the input.
 * An outputting PTransform conceptually has no output; in this case,
 * conventionally {@link com.google.cloud.dataflow.sdk.values.PDone}
 * is used as its output type.  Some PTransforms conceptually have
 * multiple inputs and/or outputs; in these cases special "bundling"
 * classes like
 * {@link com.google.cloud.dataflow.sdk.values.PCollectionList},
 * {@link com.google.cloud.dataflow.sdk.values.PCollectionTuple}
 * are used
 * to combine multiple values into a single bundle for passing into or
 * returning from the PTransform.
 *
 * <p> A {@code PTransform<Input, Output>} is invoked by calling
 * {@code apply()} on its {@code Input}, returning its {@code Output}.
 * Calls can be chained to concisely create linear pipeline segments.
 * For example:
 *
 * <pre> {@code
 * PCollection<T1> pc1 = ...;
 * PCollection<T2> pc2 =
 *     pc1.apply(ParDo.of(new MyDoFn<T1,KV<K,V>>()))
 *        .apply(GroupByKey.<K, V>create())
 *        .apply(Combine.perKey(new MyKeyedCombineFn<K,V>()))
 *        .apply(ParDo.of(new MyDoFn2<KV<K,V>,T2>()));
 * } </pre>
 *
 * <p> PTransform operations have unique names, which are used by the
 * system when explaining what's going on during optimization and
 * execution.  Each PTransform gets a system-provided default name,
 * but it's a good practice to specify an explicit name, where
 * possible, using the {@code named()} method offered by some
 * PTransforms such as {@link ParDo}.  For example:
 *
 * <pre> {@code
 * ...
 * .apply(ParDo.named("Step1").of(new MyDoFn3()))
 * ...
 * } </pre>
 *
 * <p> Each PCollection output produced by a PTransform,
 * either directly or within a "bundling" class, automatically gets
 * its own name derived from the name of its producing PTransform.
 *
 * <p> Each PCollection output produced by a PTransform
 * also records a {@link com.google.cloud.dataflow.sdk.coders.Coder}
 * that specifies how the elements of that PCollection
 * are to be encoded as a byte string, if necessary.  The
 * PTransform may provide a default Coder for any of its outputs, for
 * instance by deriving it from the PTransform input's Coder.  If the
 * PTransform does not specify the Coder for an output PCollection,
 * the system will attempt to infer a Coder for it, based on
 * what's known at run-time about the Java type of the output's
 * elements.  The enclosing {@link Pipeline}'s
 * {@link com.google.cloud.dataflow.sdk.coders.CoderRegistry}
 * (accessible via {@link Pipeline#getCoderRegistry}) defines the
 * mapping from Java types to the default Coder to use, for a standard
 * set of Java types; users can extend this mapping for additional
 * types, via
 * {@link com.google.cloud.dataflow.sdk.coders.CoderRegistry#registerCoder}.
 * If this inference process fails, either because the Java type was
 * not known at run-time (e.g., due to Java's "erasure" of generic
 * types) or there was no default Coder registered, then the Coder
 * should be specified manually by calling
 * {@link com.google.cloud.dataflow.sdk.values.TypedPValue#setCoder}
 * on the output PCollection.  The Coder of every output
 * PCollection must be determined one way or another
 * before that output is used as an input to another PTransform, or
 * before the enclosing Pipeline is run.
 *
 * <p> A small number of PTransforms are implemented natively by the
 * Google Cloud Dataflow SDK; such PTransforms simply return an
 * output value as their apply implementation.
 * The majority of PTransforms are
 * implemented as composites of other PTransforms.  Such a PTransform
 * subclass typically just implements {@link #apply}, computing its
 * Output value from its Input value.  User programs are encouraged to
 * use this mechanism to modularize their own code.  Such composite
 * abstractions get their own name, and navigating through the
 * composition hierarchy of PTransforms is supported by the monitoring
 * interface.  Examples of composite PTransforms can be found in this
 * directory and in examples.  From the caller's point of view, there
 * is no distinction between a PTransform implemented natively and one
 * implemented in terms of other PTransforms; both kinds of PTransform
 * are invoked in the same way, using {@code apply()}.
 *
 * <h3>Note on Serialization</h3>
 *
 * {@code PTransform} doesn't actually support serialization, despite
 * implementing {@code Serializable}.
 *
 * <p> {@code PTransform} is marked {@code Serializable} solely
 * because it is common for an anonymous {@code DoFn},
 * instance to be created within an
 * {@code apply()} method of a composite {@code PTransform}.
 *
 * <p> Each of those {@code *Fn}s is {@code Serializable}, but
 * unfortunately its instance state will contain a reference to the
 * enclosing {@code PTransform} instance, and so attempt to serialize
 * the {@code PTransform} instance, even though the {@code *Fn}
 * instance never references anything about the enclosing
 * {@code PTransform}.
 *
 * <p> To allow such anonymous {@code *Fn}s to be written
 * conveniently, {@code PTransform} is marked as {@code Serializable},
 * and includes dummy {@code writeObject()} and {@code readObject()}
 * operations that do not save or restore any state.
 *
 * @see <a href=
 * "https://cloud.google.com/dataflow/java-sdk/applying-transforms"
 * >Applying Transformations</a>
 *
 * @param <Input> the type of the input to this PTransform
 * @param <Output> the type of the output of this PTransform
 */
public abstract class PTransform<Input extends PInput, Output extends POutput>
    implements Serializable /* See the note above */ {

  /**
   * Applies this {@code PTransform} on the given {@code Input}, and returns its
   * {@code Output}.
   *
   * <p> Composite transforms, which are defined in terms of other transforms,
   * should return the output of one of the composed transforms.  Non-composite
   * transforms, which do not apply any transforms internally, should return
   * a new unbound output and register evaluators (via backend-specific
   * registration methods).
   *
   * <p> The default implementation throws an exception.  A derived class must
   * either implement apply, or else each runner must supply a custom
   * implementation via
   * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner#apply}.
   */
  public Output apply(Input input) {
    throw new IllegalArgumentException(
        "Runner " + getPipeline().getRunner()
            + " has not registered an implementation for the required primitive operation "
            + this);
  }

  /**
   * Sets the base name of this {@code PTransform}.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Sets the base name of this {@code PTransform} and returns itself.
   *
   * <p> This is a shortcut for calling {@link #setName}, which allows method
   * chaining.
   */
  public PTransform<Input, Output> withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Returns the transform name.
   *
   * <p> This name is provided by the transform creator and is not required to be unique.
   */
  public String getName() {
    return name != null ? name : getDefaultName();
  }

  /**
   * Returns the owning {@link Pipeline} of this {@code PTransform}.
   *
   * @throws IllegalStateException if the owning {@code Pipeline} hasn't been
   * set yet
   */
  @Deprecated
  public Pipeline getPipeline() {
    if (pipeline == null) {
      throw new IllegalStateException("owning pipeline not set");
    }
    return pipeline;
  }

  /**
   * Returns the input of this transform.
   *
   * @throws IllegalStateException if this PTransform hasn't been applied yet
   */
  public Input getInput() {
    @SuppressWarnings("unchecked")
    Input input = (Input) getPipeline().getInput(this);
    return input;
  }

  /**
   * Returns the output of this transform.
   *
   * @throws IllegalStateException if this PTransform hasn't been applied yet
   */
  public Output getOutput() {
    @SuppressWarnings("unchecked")
    Output output = (Output) getPipeline().getOutput(this);
    return output;
  }

  /**
   * Returns the {@link CoderRegistry}, useful for inferring
   * {@link com.google.cloud.dataflow.sdk.coders.Coder}s.
   *
   * @throws IllegalStateException if the owning {@link Pipeline} hasn't been
   * set yet
   * @deprecated use pipeline.getCoderRegistry()
   */
  @Deprecated
  protected CoderRegistry getCoderRegistry() {
    return getPipeline().getCoderRegistry();
  }


  /////////////////////////////////////////////////////////////////////////////

  // See the note about about PTransform's fake Serializability, to
  // understand why all of its instance state is transient.

  /**
   * The base name of this {@code PTransform}, e.g., from
   * {@link ParDo#named(String)}, or from defaults, or {@code null} if not
   * yet assigned.
   */
  protected transient String name;

  /**
   * The {@link Pipeline} that owns this {@code PTransform}, or {@code null}
   * if not yet set.
   */
  private transient Pipeline pipeline;

  protected PTransform() {
    this.name = null;
  }

  protected PTransform(String name) {
    this.name = name;
  }

  /**
   * Associates this {@code PTransform} with the given {@code Pipeline}.
   *
   * <p> For internal use only.
   *
   * @throws IllegalArgumentException if this transform has already
   * been associated with a pipeline
   */
  @Deprecated
  public void setPipeline(Pipeline pipeline) {
    if (this.pipeline != null) {
      throw new IllegalStateException(
          "internal error: transform already initialized");
    }
    this.pipeline = pipeline;
  }

  @Override
  public String toString() {
    return getName() + " [" + getKindString() + "]";
  }

  /**
   * Returns the name to use by default for this {@code PTransform}
   * (not including the names of any enclosing {@code PTransform}s).
   *
   * <p> By default, returns {@link #getKindString}.
   *
   * <p> The caller is responsible for ensuring that names of applied
   * {@code PTransform}s are unique, e.g., by adding a uniquifying
   * suffix when needed.
   */
  protected String getDefaultName() {
    return getKindString();
  }

  /**
   * Returns a string describing what kind of {@code PTransform} this is.
   *
   * <p> By default, returns the base name of this
   * {@code PTransform}'s class.
   */
  protected String getKindString() {
    return StringUtils.approximateSimpleName(getClass());
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
   * After building, finalizes this {@code PTransform} to
   * make it ready for running.  Called automatically when its
   * output(s) are finished.
   *
   * <p> Not normally called by user code.
   */
  public void finishSpecifying() {
    getOutput().finishSpecifyingOutput();
  }

  /**
   * Returns the default {@code Coder} to use for the output of this
   * single-output {@code PTransform}, or {@code null} if
   * none can be inferred.
   *
   * <p> By default, returns {@code null}.
   */
  protected Coder<?> getDefaultOutputCoder() {
    return null;
  }

  /**
   * Returns the default {@code Coder} to use for the given output of
   * this single-output {@code PTransform}, or {@code null}
   * if none can be inferred.
   */
  public <T> Coder<T> getDefaultOutputCoder(TypedPValue<T> output) {
    if (output != getOutput()) {
      return null;
    } else {
      @SuppressWarnings("unchecked")
      Coder<T> defaultOutputCoder = (Coder<T>) getDefaultOutputCoder();
      return defaultOutputCoder;
    }
  }
}
