/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.values;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

/**
 * A {@code TypedPValue<T>} is the abstract base class of things that
 * store some number of values of type {@code T}. Because we know
 * the type {@code T}, this is the layer of the inheritance hierarchy where
 * we store a coder for objects of type {@code T}
 *
 * @param <T> the type of the values stored in this {@code TypedPValue}
 */
public abstract class TypedPValue<T> extends PValueBase implements PValue {

  /**
   * Returns the Coder used by this TypedPValue to encode and decode
   * the values stored in it.
   *
   * @throws IllegalStateException if the Coder hasn't been set, and
   * couldn't be inferred
   */
  public Coder<T> getCoder() {
    if (coder == null) {
      try {
        coder = inferCoderOrFail();
      } catch (CannotProvideCoderException exc) {
        throw new IllegalStateException(
            "Unable to infer a default Coder for " + this
            + "; either correct the root cause below "
            + "or use setCoder() to specify one explicitly. ",
            exc);
      }
    }
    return coder;
  }

  /**
   * Sets the Coder used by this TypedPValue to encode and decode the
   * values stored in it.  Returns {@code this}.
   *
   * @throws IllegalStateException if this TypedPValue has already
   * been finalized and is no longer settable, e.g., by having
   * {@code apply()} called on it
   */
  public TypedPValue<T> setCoder(Coder<T> coder) {
    if (isFinishedSpecifyingInternal()) {
      throw new IllegalStateException(
          "cannot change the Coder of " + this + " once it's been used");
    }
    if (coder == null) {
      throw new IllegalArgumentException(
          "Cannot setCoder(null)");
    }
    this.coder = coder;
    return this;
  }

  /**
   * After building, finalizes this PValue to make it ready for
   * running.  Automatically invoked whenever the PValue is "used"
   * (e.g., when apply() is called on it) and when the Pipeline is
   * run (useful if this is a PValue with no consumers).
   */
  @Override
  public void finishSpecifying() {
    if (isFinishedSpecifyingInternal()) {
      return;
    }
    super.finishSpecifying();
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  /**
   * The Coder used by this TypedPValue to encode and decode the
   * values stored in it, or null if not specified nor inferred yet.
   */
  private Coder<T> coder;

  protected TypedPValue(Pipeline p) {
    super(p);
  }

  private TypeDescriptor<T> typeDescriptor;

  /**
   * Returns a {@code TypeDescriptor<T>} with some reflective information
   * about {@code T}, if possible. May return {@code null} if no information
   * is available. Subclasses may override this to enable better
   * {@code Coder} inference.
   */
  public TypeDescriptor<T> getTypeDescriptor() {
    return typeDescriptor;
  }

  /**
   * Sets the {@code TypeDescriptor<T>} associated with this class. Better
   * reflective type information will lead to better {@code Coder}
   * inference.
   */
  public TypedPValue<T> setTypeDescriptorInternal(TypeDescriptor<T> typeDescriptor) {
    this.typeDescriptor = typeDescriptor;
    return this;
  }

  /**
   * If the coder is not explicitly set, this sets the coder for
   * this {@code TypedPValue<T>} to the best coder that can be inferred
   * based upon the known {@code TypeDescriptor<T>}. By default, this is null,
   * but can and should be improved by subclasses.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private Coder<T> inferCoderOrFail() throws CannotProvideCoderException {
    if (coder != null) {
      return coder;
    }

    TypeDescriptor<T> token = getTypeDescriptor();
    CoderRegistry registry = getPipeline().getCoderRegistry();

    try {
      if (token != null) {
        return registry.getDefaultCoder(token);
      }
    } catch (CannotProvideCoderException exc) {
        // try the next thing
    }

    return ((PTransform) getProducingTransformInternal()).getDefaultOutputCoder(
        getPipeline().getInput(getProducingTransformInternal()), this);
  }
}
