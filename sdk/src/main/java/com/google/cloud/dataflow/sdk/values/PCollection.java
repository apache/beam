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

package com.google.cloud.dataflow.sdk.values;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.common.reflect.TypeToken;

/**
 * A {@code PCollection<T>} is an immutable collection of values of type
 * {@code T}.  A {@code PCollection} can contain either a bounded or unbounded
 * number of elements.  Bounded and unbounded {@code PCollection}s are produced
 * as the output of {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s
 * (including root PTransforms like
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Read},
 * {@link com.google.cloud.dataflow.sdk.io.PubsubIO.Read} and
 * {@link com.google.cloud.dataflow.sdk.transforms.Create}), and can
 * be passed as the inputs of other PTransforms.
 *
 * <p> Some root transforms produce bounded {@code PCollections} and others
 * produce unbounded ones.  For example,
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Read} reads a static set
 * of files, so it produces a bounded {@code PCollection}.
 * {@link com.google.cloud.dataflow.sdk.io.PubsubIO.Read}, on the other hand,
 * receives a potentially infinite stream of Pubsub messages, so it produces
 * an unbounded {@code PCollection}.
 *
 * <p> Each element in a {@code PCollection} may have an associated implicit
 * timestamp.  Readers assign timestamps to elements when they create
 * {@code PCollection}s, and other {@code PTransform}s propagate these
 * timestamps from their input to their output. For example, PubsubIO.Read
 * assigns pubsub message timestamps to elements, and TextIO.Read assigns
 * the default value {@code Long.MIN_VALUE} to elements. User code can
 * explicitly assign timestamps to elements with
 * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.Context#outputWithTimestamp}.
 *
 * <p> Additionally, a {@code PCollection} has an associated
 * {@link WindowFn} and each element is assigned to a set of windows.
 * By default, the windowing function is
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows}
 * and all elements are assigned into a single default window.
 * This default can be overridden with the
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window}
 * {@code PTransform}. Dataflow pipelines run in classic batch MapReduce style
 * with the default GlobalWindow strategy if timestamps are ignored.
 *
 * <p> See the individual {@code PTransform} subclasses for specific information
 * on how they propagate timestamps and windowing.
 *
 * @param <T> the type of the elements of this PCollection
 */
public class PCollection<T> extends TypedPValue<T> {
  /**
   * Returns the name of this PCollection.
   *
   * <p> By default, the name of a PCollection is based on the name of the
   * PTransform that produces it.  It can be specified explicitly by
   * calling {@link #setName}.
   *
   * @throws IllegalStateException if the name hasn't been set yet
   */
  @Override
  public String getName() {
    return super.getName();
  }

  /**
   * Sets the name of this PCollection.  Returns {@code this}.
   *
   * @throws IllegalStateException if this PCollection has already been
   * finalized and is no longer settable, e.g., by having
   * {@code apply()} called on it
   */
  @Override
  public PCollection<T> setName(String name) {
    super.setName(name);
    return this;
  }

  /**
   * Returns the Coder used by this PCollection to encode and decode
   * the values stored in it.
   *
   * @throws IllegalStateException if the Coder hasn't been set, and
   * couldn't be inferred
   */
  @Override
  public Coder<T> getCoder() {
    return super.getCoder();
  }

  /**
   * Sets the Coder used by this PCollection to encode and decode the
   * values stored in it.  Returns {@code this}.
   *
   * @throws IllegalStateException if this PCollection has already
   * been finalized and is no longer settable, e.g., by having
   * {@code apply()} called on it
   */
  @Override
  public PCollection<T> setCoder(Coder<T> coder) {
    super.setCoder(coder);
    return this;
  }

  /**
   * Returns whether or not the elements of this PCollection have a
   * well-defined and fixed order, such that subsequent reading of the
   * PCollection is guaranteed to process the elements in order.
   *
   * <p> Requiring a fixed order can limit optimization opportunities.
   *
   * <p> By default, PCollections do not have a well-defined or fixed order.
   */
  public boolean isOrdered() {
    return isOrdered;
  }

  /**
   * Sets whether or not this PCollection should preserve the order in
   * which elements are put in it, such that subsequent parallel
   * reading of the PCollection is guaranteed to process the elements
   * in order.
   *
   * <p> Requiring a fixed order can limit optimization opportunities.
   *
   * <p> Returns {@code this}.
   *
   * @throws IllegalStateException if this PCollection has already
   * been finalized and is no longer settable, e.g., by having
   * {@code apply()} called on it
   */
  public PCollection<T> setOrdered(boolean isOrdered) {
    if (this.isOrdered != isOrdered) {
      if (isFinishedSpecifyingInternal()) {
        throw new IllegalStateException(
            "cannot change the orderedness of " + this + " once it's been used");
      }
      this.isOrdered = isOrdered;
    }
    return this;
  }

  /**
   * Applies the given PTransform to this input PCollection, and
   * returns the PTransform's Output.
   */
  public <Output extends POutput> Output apply(PTransform<? super PCollection<T>, Output> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Returns the {@link WindowFn} of this {@code PCollection}.
   */
  public WindowFn<?, ?> getWindowFn() {
    return windowFn;
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  /**
   * Whether or not the elements of this PCollection have a
   * well-defined and fixed order, such that subsequent reading of the
   * PCollection is guaranteed to process the elements in order.
   */
  private boolean isOrdered = false;

  /**
   * {@link WindowFn} that will be used to merge windows in
   * this {@code PCollection} and subsequent {@code PCollection}s produced
   * from this one.
   *
   * <p> By default, no merging is performed.
   */
  private WindowFn<?, ?> windowFn;

  private PCollection() {}

  /**
   * Sets the {@code TypeToken<T>} for this {@code PCollection<T>}, so that
   * the enclosing {@code PCollectionTuple}, {@code PCollectionList<T>},
   * or {@code PTransform<?, PCollection<T>>}, etc., can provide
   * more detailed reflective information.
   */
  @Override
  public PCollection<T> setTypeTokenInternal(TypeToken<T> typeToken) {
    super.setTypeTokenInternal(typeToken);
    return this;
  }

  /**
   * Sets the {@link WindowFn} of this {@code PCollection}.
   *
   * <p> For use by primitive transformations only.
   */
  public PCollection<T> setWindowFnInternal(WindowFn<?, ?> windowFn) {
     this.windowFn = windowFn;
     return this;
  }

  /**
   * Sets the {@link Pipeline} for this {@code PCollection}.
   *
   * <p> For use by primitive transformations only.
   */
  @Override
  public PCollection<T> setPipelineInternal(Pipeline pipeline) {
    super.setPipelineInternal(pipeline);
    return this;
  }

  /**
   * Creates and returns a new PCollection for a primitive output.
   *
   * <p> For use by primitive transformations only.
   */
  public static <T> PCollection<T> createPrimitiveOutputInternal(
      WindowFn<?, ?> windowFn) {
    return new PCollection<T>().setWindowFnInternal(windowFn);
  }
}
