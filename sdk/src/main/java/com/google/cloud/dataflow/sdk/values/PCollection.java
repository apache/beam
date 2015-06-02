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
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;

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
   * The PCollection IsBounded property.
   */
  public enum IsBounded {
    /**
     * {@code PCollection} contains bounded data elements, such as
     * {@code PCollection}s from {@code TextIO}, {@code BigQueryIO},
     * {@code Create} e.t.c.
     */
    BOUNDED,
    /**
     * {@code PCollection} contains unbounded data elements, such as
     * {@code PCollection}s from {@code PubsubIO}.
     */
    UNBOUNDED;

    /**
     * Returns the composed IsBounded property.
     *
     * <p> The composed property is BOUNDED only if all components are BOUNDED.
     * Otherwise, it is UNBOUNDED.
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
   * Like {@link IsBounded#apply(String, PTransform)} but defaulting to the name
   * of the {@link PTransform}.
   */
  public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@code PTransform} to this input {@code PCollection<T>},
   * using {@code name} to identify this specific application of the transform.
   * This name is used in various places, including the monitoring UI, logging,
   * and to stably identify this application node in the job graph.
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /**
   * Returns the {@link WindowingStrategy} of this {@code PCollection}.
   */
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
   * {@code PCollection} and subsequence {@code PCollection}s produced from this one.
   *
   * <p> By default, no merging is performed.
   */
  private WindowingStrategy<?, ?> windowingStrategy;

  private IsBounded isBounded;

  private PCollection(Pipeline p) {
    super(p);
  }

  /**
   * Sets the {@code TypeDescriptor<T>} for this {@code PCollection<T>}, so that
   * the enclosing {@code PCollectionTuple}, {@code PCollectionList<T>},
   * or {@code PTransform<?, PCollection<T>>}, etc., can provide
   * more detailed reflective information.
   */
  @Override
  public PCollection<T> setTypeDescriptorInternal(TypeDescriptor<T> typeDescriptor) {
    super.setTypeDescriptorInternal(typeDescriptor);
    return this;
  }

  /**
   * Sets the {@link WindowingStrategy} of this {@code PCollection}.
   *
   * <p> For use by primitive transformations only.
   */
  public PCollection<T> setWindowingStrategyInternal(WindowingStrategy<?, ?> windowingStrategy) {
     this.windowingStrategy = windowingStrategy;
     return this;
  }

  /**
   * Sets the {@link PCollection.IsBounded} of this {@code PCollection}.
   */
  private PCollection<T> setIsBoundedInternal(IsBounded isBounded) {
    this.isBounded = isBounded;
    return this;
  }

  /**
   * Creates and returns a new PCollection for a primitive output.
   *
   * <p> For use by primitive transformations only.
   */
  public static <T> PCollection<T> createPrimitiveOutputInternal(
      Pipeline pipeline,
      WindowingStrategy<?, ?> windowingStrategy,
      IsBounded isBounded) {
    return new PCollection<T>(pipeline)
        .setWindowingStrategyInternal(windowingStrategy)
        .setIsBoundedInternal(isBounded);
  }
}
