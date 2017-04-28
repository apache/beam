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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.CannotProvideCoderException.ReasonCode;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * A {@link TypedPValue TypedPValue&lt;T&gt;} is the abstract base class of things that
 * store some number of values of type {@code T}.
 *
 * <p>Because we know the type {@code T}, this is the layer of the inheritance hierarchy where
 * we store a coder for objects of type {@code T}.
 *
 * @param <T> the type of the values stored in this {@link TypedPValue}
 */
public abstract class TypedPValue<T> extends PValueBase implements PValue {

  /**
   * Returns the {@link Coder} used by this {@link TypedPValue} to encode and decode
   * the values stored in it.
   *
   * @throws IllegalStateException if the {@link Coder} hasn't been set, and
   * couldn't be inferred.
   */
  public Coder<T> getCoder() {
    checkState(coderOrFailure.coder != null, coderOrFailure.failure);
    return coderOrFailure.coder;
  }

  /**
   * Sets the {@link Coder} used by this {@link TypedPValue} to encode and decode the
   * values stored in it. Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link TypedPValue} has already
   * been finalized and is no longer settable, e.g., by having
   * {@code apply()} called on it
   */
  public TypedPValue<T> setCoder(Coder<T> coder) {
    checkState(
        !isFinishedSpecifyingInternal(), "cannot change the Coder of %s once it's been used", this);
    checkArgument(coder != null, "Cannot setCoder(null)");
    this.coderOrFailure = new CoderOrFailure<>(coder, null);
    return this;
  }

  @Override
  public void finishSpecifyingOutput(PInput input, PTransform<?, ?> transform) {
    this.coderOrFailure = inferCoderOrFail(input, transform, getPipeline().getCoderRegistry());
  }

  /**
   * After building, finalizes this {@link PValue} to make it ready for
   * running.  Automatically invoked whenever the {@link PValue} is "used"
   * (e.g., when apply() is called on it) and when the Pipeline is
   * run (useful if this is a {@link PValue} with no consumers).
   */
  @Override
  public void finishSpecifying(PInput input, PTransform<?, ?> transform) {
    if (isFinishedSpecifyingInternal()) {
      return;
    }
    this.coderOrFailure = inferCoderOrFail(input, transform, getPipeline().getCoderRegistry());
    // Ensure that this TypedPValue has a coder by inferring the coder if none exists; If not,
    // this will throw an exception.
    getCoder();
    super.finishSpecifying(input, transform);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  /**
   * The {@link Coder} used by this {@link TypedPValue} to encode and decode the values stored in
   * it, or null if not specified nor inferred yet.
   */
  private CoderOrFailure<T> coderOrFailure =
      new CoderOrFailure<>(null, "No Coder was specified, and Coder Inference did not occur");

  protected TypedPValue(Pipeline p) {
    super(p);
  }

  private TypeDescriptor<T> typeDescriptor;

  /**
   * Returns a {@link TypeDescriptor TypeDescriptor&lt;T&gt;} with some reflective information
   * about {@code T}, if possible. May return {@code null} if no information
   * is available. Subclasses may override this to enable better
   * {@code Coder} inference.
   */
  public TypeDescriptor<T> getTypeDescriptor() {
    return typeDescriptor;
  }

  /**
   * Sets the {@link TypeDescriptor TypeDescriptor&lt;T&gt;} associated with this class. Better
   * reflective type information will lead to better {@link Coder}
   * inference.
   */
  public TypedPValue<T> setTypeDescriptor(TypeDescriptor<T> typeDescriptor) {
    this.typeDescriptor = typeDescriptor;
    return this;
  }

  /**
   * If the coder is not explicitly set, this sets the coder for this {@link TypedPValue} to the
   * best coder that can be inferred based upon the known {@link TypeDescriptor}. By default, this
   * is null, but can and should be improved by subclasses.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private CoderOrFailure<T> inferCoderOrFail(
      PInput input, PTransform<?, ?> transform, CoderRegistry registry) {
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

    // Third option for a coder: Look in the coder registry.
    TypeDescriptor<T> token = getTypeDescriptor();
    CannotProvideCoderException inferFromTokenException = null;
    if (token != null) {
      try {
        return new CoderOrFailure<>(registry.getDefaultCoder(token), null);
      } catch (CannotProvideCoderException exc) {
        inferFromTokenException = exc;
        // Attempt to detect when the token came from a TupleTag used for a ParDo output,
        // and provide a better error message if so. Unfortunately, this information is not
        // directly available from the TypeDescriptor, so infer based on the type of the PTransform
        // and the error message itself.
        if (transform instanceof ParDo.MultiOutput
            && exc.getReason() == ReasonCode.TYPE_ERASURE) {
          inferFromTokenException = new CannotProvideCoderException(exc.getMessage()
              + " If this error occurs for an output of the producing ParDo, verify that the "
              + "TupleTag for this output is constructed with proper type information (see "
              + "TupleTag Javadoc) or explicitly set the Coder to use if this is not possible.");
        }
      }
    }

    // Build up the error message and list of causes.
    StringBuilder messageBuilder = new StringBuilder()
        .append("Unable to return a default Coder for ").append(this)
        .append(". Correct one of the following root causes:");

    // No exception, but give the user a message about .setCoder() has not been called.
    messageBuilder.append("\n  No Coder has been manually specified; ")
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

  private static class CoderOrFailure<T> {
    @Nullable private final Coder<T> coder;
    @Nullable private final String failure;

    public CoderOrFailure(@Nullable Coder<T> coder, @Nullable String failure) {
      this.coder = coder;
      this.failure = failure;
    }
  }
}
