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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CannotProvideCoderException.ReasonCode;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PCollection PCollection&lt;T&gt;} is an immutable collection of values of type {@code
 * T}. A {@link PCollection} can contain either a bounded or unbounded number of elements. Bounded
 * and unbounded {@link PCollection PCollections} are produced as the output of {@link PTransform
 * PTransforms} (including root PTransforms like {@link Read} and {@link Create}), and can be passed
 * as the inputs of other PTransforms.
 *
 * <p>Some root transforms produce bounded {@code PCollections} and others produce unbounded ones.
 * For example, {@link GenerateSequence#from} with {@link GenerateSequence#to} produces a fixed set
 * of integers, so it produces a bounded {@link PCollection}. {@link GenerateSequence#from} without
 * a {@link GenerateSequence#to} produces all integers as an infinite stream, so it produces an
 * unbounded {@link PCollection}.
 *
 * <p>Each element in a {@link PCollection} has an associated timestamp. Readers assign timestamps
 * to elements when they create {@link PCollection PCollections}, and other {@link PTransform
 * PTransforms} propagate these timestamps from their input to their output. See the documentation
 * on {@link BoundedReader} and {@link UnboundedReader} for more information on how these readers
 * produce timestamps and watermarks.
 *
 * <p>Additionally, a {@link PCollection} has an associated {@link WindowFn} and each element is
 * assigned to a set of windows. By default, the windowing function is {@link GlobalWindows} and all
 * elements are assigned into a single default window. This default can be overridden with the
 * {@link Window} {@link PTransform}.
 *
 * <p>See the individual {@link PTransform} subclasses for specific information on how they
 * propagate timestamps and windowing.
 *
 * @param <T> the type of the elements of this {@link PCollection}
 */
public class PCollection<T> extends PValueBase implements PValue {

  /**
   * The {@link Coder} used by this {@link PCollection} to encode and decode the values stored in
   * it, or null if not specified nor inferred yet.
   */
  private CoderOrFailure<T> coderOrFailure =
      new CoderOrFailure<>(null, "No Coder was specified, and Coder Inference did not occur");

  private @Nullable TypeDescriptor<T> typeDescriptor;

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    this.coderOrFailure =
        inferCoderOrFail(
            input, transform, getPipeline().getCoderRegistry(), getPipeline().getSchemaRegistry());
    super.finishSpecifyingOutput(transformName, input, transform);
  }

  /**
   * After building, finalizes this {@link PValue} to make it ready for running. Automatically
   * invoked whenever the {@link PValue} is "used" (e.g., when apply() is called on it) and when the
   * Pipeline is run (useful if this is a {@link PValue} with no consumers).
   */
  @Override
  public void finishSpecifying(PInput input, PTransform<?, ?> transform) {
    if (isFinishedSpecifying()) {
      return;
    }
    this.coderOrFailure =
        inferCoderOrFail(
            input, transform, getPipeline().getCoderRegistry(), getPipeline().getSchemaRegistry());
    // Ensure that this TypedPValue has a coder by inferring the coder if none exists; If not,
    // this will throw an exception.
    getCoder();
    super.finishSpecifying(input, transform);
  }

  /**
   * Returns a {@link TypeDescriptor TypeDescriptor&lt;T&gt;} with some reflective information about
   * {@code T}, if possible. May return {@code null} if no information is available. Subclasses may
   * override this to enable better {@code Coder} inference.
   */
  public @Nullable TypeDescriptor<T> getTypeDescriptor() {
    return typeDescriptor;
  }

  /**
   * If the coder is not explicitly set, this sets the coder for this {@link PCollection} to the
   * best coder that can be inferred based upon the known {@link TypeDescriptor}. By default, this
   * is null, but can and should be improved by subclasses.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CoderOrFailure<T> inferCoderOrFail(
      PInput input,
      PTransform<?, ?> transform,
      CoderRegistry coderRegistry,
      SchemaRegistry schemaRegistry) {
    // First option for a coder: use the Coder set on this PValue.
    if (coderOrFailure.coder != null) {
      return coderOrFailure;
    }

    // Second option for a coder: use the default Coder from the producing PTransform.
    CannotProvideCoderException inputCoderException;
    try {
      return new CoderOrFailure<>(
          ((PTransform) transform).getDefaultOutputCoder(input, this), null);
    } catch (CannotProvideCoderException exc) {
      inputCoderException = exc;
    }

    TypeDescriptor<T> token = getTypeDescriptor();
    // If there is a schema registered for the type, attempt to create a SchemaCoder.
    if (token != null) {
      try {
        SchemaCoder<T> schemaCoder =
            SchemaCoder.of(
                schemaRegistry.getSchema(token),
                token,
                schemaRegistry.getToRowFunction(token),
                schemaRegistry.getFromRowFunction(token));
        return new CoderOrFailure<>(schemaCoder, null);
      } catch (NoSuchSchemaException esc) {
        // No schema.
      }
    }

    // Fourth option for a coder: Look in the coder registry.
    CannotProvideCoderException inferFromTokenException = null;
    if (token != null) {
      try {
        return new CoderOrFailure<>(coderRegistry.getCoder(token), null);
      } catch (CannotProvideCoderException exc) {
        inferFromTokenException = exc;
        // Attempt to detect when the token came from a TupleTag used for a ParDo output,
        // and provide a better error message if so. Unfortunately, this information is not
        // directly available from the TypeDescriptor, so infer based on the type of the PTransform
        // and the error message itself.
        if (transform instanceof ParDo.MultiOutput && exc.getReason() == ReasonCode.TYPE_ERASURE) {
          inferFromTokenException =
              new CannotProvideCoderException(
                  exc.getMessage()
                      + " If this error occurs for an output of the producing ParDo, verify that the "
                      + "TupleTag for this output is constructed with proper type information (see "
                      + "TupleTag Javadoc) or explicitly set the Coder to use if this is not possible.");
        }
      }
    }

    // Build up the error message and list of causes.
    StringBuilder messageBuilder =
        new StringBuilder()
            .append("Unable to return a default Coder for ")
            .append(this)
            .append(". Correct one of the following root causes:");

    // No exception, but give the user a message about .setCoder() has not been called.
    messageBuilder
        .append("\n  No Coder has been manually specified; ")
        .append(" you may do so using .setCoder().");

    if (inferFromTokenException != null) {
      messageBuilder
          .append("\n  Inferring a Coder from the CoderRegistry failed: ")
          .append(inferFromTokenException.getMessage());
    }

    if (inputCoderException != null) {
      messageBuilder
          .append("\n  Using the default output Coder from the producing PTransform failed: ")
          .append(inputCoderException.getMessage());
    }

    // Build and throw the exception.
    return new CoderOrFailure<>(null, messageBuilder.toString());
  }

  /** The enumeration of cases for whether a {@link PCollection} is bounded. */
  public enum IsBounded {
    /** Indicates that a {@link PCollection} contains a bounded number of elements. */
    BOUNDED,
    /** Indicates that a {@link PCollection} contains an unbounded number of elements. */
    UNBOUNDED;

    /**
     * Returns the composed IsBounded property.
     *
     * <p>The composed property is {@link #BOUNDED} only if all components are {@link #BOUNDED}.
     * Otherwise, it is {@link #UNBOUNDED}.
     */
    public IsBounded and(IsBounded that) {
      if (this == BOUNDED && that == BOUNDED) {
        return BOUNDED;
      } else {
        return UNBOUNDED;
      }
    }
  }

  /**
   * Returns the name of this {@link PCollection}.
   *
   * <p>By default, the name of a {@link PCollection} is based on the name of the {@link PTransform}
   * that produces it. It can be specified explicitly by calling {@link #setName}.
   *
   * @throws IllegalStateException if the name hasn't been set yet
   */
  @Override
  public String getName() {
    return super.getName();
  }

  @Override
  public final Map<TupleTag<?>, PValue> expand() {
    return Collections.singletonMap(tag, this);
  }

  /**
   * Sets the name of this {@link PCollection}. Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PCollection} has already been finalized and may no
   *     longer be set. Once {@link #apply} has been called, this will be the case.
   */
  @Override
  public PCollection<T> setName(String name) {
    super.setName(name);
    return this;
  }

  /**
   * Returns the {@link Coder} used by this {@link PCollection} to encode and decode the values
   * stored in it.
   *
   * @throws IllegalStateException if the {@link Coder} hasn't been set, and couldn't be inferred.
   */
  public Coder<T> getCoder() {
    checkState(coderOrFailure.coder != null, coderOrFailure.failure);
    return coderOrFailure.coder;
  }

  /**
   * Sets the {@link Coder} used by this {@link PCollection} to encode and decode the values stored
   * in it. Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PCollection} has already been finalized and may no
   *     longer be set. Once {@link #apply} has been called, this will be the case.
   */
  public PCollection<T> setCoder(Coder<T> coder) {
    checkState(!isFinishedSpecifying(), "cannot change the Coder of %s once it's been used", this);
    checkArgument(coder != null, "Cannot setCoder(null)");
    this.coderOrFailure = new CoderOrFailure<>(coder, null);
    return this;
  }

  /**
   * Sets a schema on this PCollection.
   *
   * <p>Can only be called on a {@link PCollection<Row>}.
   */
  @Experimental(Kind.SCHEMAS)
  public PCollection<T> setRowSchema(Schema schema) {
    return setCoder((SchemaCoder<T>) SchemaCoder.of(schema));
  }

  /** Sets a {@link Schema} on this {@link PCollection}. */
  @Experimental(Kind.SCHEMAS)
  public PCollection<T> setSchema(
      Schema schema,
      TypeDescriptor<T> typeDescriptor,
      SerializableFunction<T, Row> toRowFunction,
      SerializableFunction<Row, T> fromRowFunction) {
    return setCoder(SchemaCoder.of(schema, typeDescriptor, toRowFunction, fromRowFunction));
  }

  /** Returns whether this {@link PCollection} has an attached schema. */
  @Experimental(Kind.SCHEMAS)
  public boolean hasSchema() {
    return coderOrFailure.coder != null && coderOrFailure.coder instanceof SchemaCoder;
  }

  /** Returns the attached schema. */
  @Experimental(Kind.SCHEMAS)
  public Schema getSchema() {
    if (!hasSchema()) {
      throw new IllegalStateException("Cannot call getSchema when there is no schema");
    }
    return ((SchemaCoder) getCoder()).getSchema();
  }

  /** Returns the attached schema's toRowFunction. */
  @Experimental(Kind.SCHEMAS)
  public SerializableFunction<T, Row> getToRowFunction() {
    if (!hasSchema()) {
      throw new IllegalStateException("Cannot call getToRowFunction when there is no schema");
    }
    return ((SchemaCoder<T>) getCoder()).getToRowFunction();
  }

  /** Returns the attached schema's fromRowFunction. */
  @Experimental(Kind.SCHEMAS)
  public SerializableFunction<Row, T> getFromRowFunction() {
    if (!hasSchema()) {
      throw new IllegalStateException("Cannot call getFromRowFunction when there is no schema");
    }
    return ((SchemaCoder<T>) getCoder()).getFromRowFunction();
  }

  /**
   * of the {@link PTransform}.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this input {@link PCollection}, using {@code name} to
   * identify this specific application of the transform. This name is used in various places,
   * including the monitoring UI, logging, and to stably identify this application node in the job
   * graph.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /** Returns the {@link WindowingStrategy} of this {@link PCollection}. */
  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public IsBounded isBounded() {
    return isBounded;
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  /**
   * {@link WindowingStrategy} that will be used for merging windows and triggering output in this
   * {@link PCollection} and subsequence {@link PCollection PCollections} produced from this one.
   *
   * <p>By default, no merging is performed.
   */
  private WindowingStrategy<?, ?> windowingStrategy;

  private IsBounded isBounded;

  /** A local {@link TupleTag} used in the expansion of this {@link PValueBase}. */
  private final TupleTag<?> tag;

  private PCollection(Pipeline p, WindowingStrategy<?, ?> windowingStrategy, IsBounded isBounded) {
    super(p);
    this.windowingStrategy = windowingStrategy;
    this.isBounded = isBounded;
    this.tag = new TupleTag<>();
  }

  private PCollection(
      Pipeline p, WindowingStrategy<?, ?> windowingStrategy, IsBounded isBounded, TupleTag<?> tag) {
    super(p);
    this.windowingStrategy = windowingStrategy;
    this.isBounded = isBounded;
    this.tag = tag;
  }

  /**
   * Sets the {@link TypeDescriptor TypeDescriptor&lt;T&gt;} for this {@link PCollection
   * PCollection&lt;T&gt;}. This may allow the enclosing {@link PCollectionTuple}, {@link
   * PCollectionList}, or {@code PTransform<?, PCollection<T>>}, etc., to provide more detailed
   * reflective information.
   */
  public PCollection<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor) {
    this.typeDescriptor = typeDescriptor;
    return this;
  }

  /** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
  @Internal
  public PCollection<T> setWindowingStrategyInternal(WindowingStrategy<?, ?> windowingStrategy) {
    this.windowingStrategy = windowingStrategy;
    return this;
  }

  /** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
  @Internal
  public PCollection<T> setIsBoundedInternal(IsBounded isBounded) {
    this.isBounded = isBounded;
    return this;
  }

  /** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
  @Internal
  public static <T> PCollection<T> createPrimitiveOutputInternal(
      Pipeline pipeline,
      WindowingStrategy<?, ?> windowingStrategy,
      IsBounded isBounded,
      @Nullable Coder<T> coder) {
    PCollection<T> res = new PCollection<>(pipeline, windowingStrategy, isBounded);
    if (coder != null) {
      res.setCoder(coder);
    }
    return res;
  }

  /** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
  @Internal
  public static <T> PCollection<T> createPrimitiveOutputInternal(
      Pipeline pipeline,
      WindowingStrategy<?, ?> windowingStrategy,
      IsBounded isBounded,
      @Nullable Coder<T> coder,
      TupleTag<?> tag) {
    PCollection<T> res = new PCollection<>(pipeline, windowingStrategy, isBounded, tag);
    if (coder != null) {
      res.setCoder(coder);
    }
    return res;
  }

  private static class CoderOrFailure<T> {
    private final @Nullable Coder<T> coder;
    private final @Nullable String failure;

    public CoderOrFailure(@Nullable Coder<T> coder, @Nullable String failure) {
      this.coder = coder;
      this.failure = failure;
    }
  }
}
