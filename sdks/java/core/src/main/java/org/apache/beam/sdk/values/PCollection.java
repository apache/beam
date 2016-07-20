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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowingStrategy;

/**
 * A {@link PCollection PCollection&lt;T&gt;} is an immutable collection of values of type
 * {@code T}.  A {@link PCollection} can contain either a bounded or unbounded
 * number of elements.  Bounded and unbounded {@link PCollection PCollections} are produced
 * as the output of {@link PTransform PTransforms}
 * (including root PTransforms like {@link Read} and {@link Create}), and can
 * be passed as the inputs of other PTransforms.
 *
 * <p>Some root transforms produce bounded {@code PCollections} and others
 * produce unbounded ones. For example, {@link CountingInput#upTo} produces a fixed set of integers,
 * so it produces a bounded {@link PCollection}. {@link CountingInput#unbounded} produces all
 * integers as an infinite stream, so it produces an unbounded {@link PCollection}.
 *
 * <p>Each element in a {@link PCollection} has an associated timestamp. Readers assign timestamps
 * to elements when they create {@link PCollection PCollections}, and other
 * {@link PTransform PTransforms} propagate these timestamps from their input to their output. See
 * the documentation on {@link BoundedReader} and {@link UnboundedReader} for more information on
 * how these readers produce timestamps and watermarks.
 *
 * <p>Additionally, a {@link PCollection} has an associated
 * {@link WindowFn} and each element is assigned to a set of windows.
 * By default, the windowing function is {@link GlobalWindows}
 * and all elements are assigned into a single default window.
 * This default can be overridden with the {@link Window}
 * {@link PTransform}.
 *
 * <p>See the individual {@link PTransform} subclasses for specific information
 * on how they propagate timestamps and windowing.
 *
 * @param <T> the type of the elements of this {@link PCollection}
 */
public class PCollection<T> extends TypedPValue<T> {

  /**
   * The enumeration of cases for whether a {@link PCollection} is bounded.
   */
  public enum IsBounded {
    /**
     * Indicates that a {@link PCollection} contains a bounded number of elements.
     */
    BOUNDED,
    /**
     * Indicates that a {@link PCollection} contains an unbounded number of elements.
     */
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
   * <p>By default, the name of a {@link PCollection} is based on the name of the
   * {@link PTransform} that produces it.  It can be specified explicitly by
   * calling {@link #setName}.
   *
   * @throws IllegalStateException if the name hasn't been set yet
   */
  @Override
  public String getName() {
    return super.getName();
  }

  /**
   * Sets the name of this {@link PCollection}.  Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PCollection} has already been
   * finalized and may no longer be set.
   * Once {@link #apply} has been called, this will be the case.
   */
  @Override
  public PCollection<T> setName(String name) {
    super.setName(name);
    return this;
  }

  /**
   * Returns the {@link Coder} used by this {@link PCollection} to encode and decode
   * the values stored in it.
   *
   * @throws IllegalStateException if the {@link Coder} hasn't been set, and
   * couldn't be inferred.
   */
  @Override
  public Coder<T> getCoder() {
    return super.getCoder();
  }

  /**
   * Sets the {@link Coder} used by this {@link PCollection} to encode and decode the
   * values stored in it. Returns {@code this}.
   *
   * @throws IllegalStateException if this {@link PCollection} has already
   * been finalized and may no longer be set.
   * Once {@link #apply} has been called, this will be the case.
   */
  @Override
  public PCollection<T> setCoder(Coder<T> coder) {
    super.setCoder(coder);
    return this;
  }

  /**
   * Like {@link IsBounded#apply(String, PTransform)} but defaulting to the name
   * of the {@link PTransform}.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this input {@link PCollection},
   * using {@code name} to identify this specific application of the transform.
   * This name is used in various places, including the monitoring UI, logging,
   * and to stably identify this application node in the job graph.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PCollection<T>, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /**
   * Returns the {@link WindowingStrategy} of this {@link PCollection}.
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
   * {@link PCollection} and subsequence {@link PCollection PCollections} produced from this one.
   *
   * <p>By default, no merging is performed.
   */
  private WindowingStrategy<?, ?> windowingStrategy;

  private IsBounded isBounded;

  private PCollection(Pipeline p) {
    super(p);
  }

  /**
   * Sets the {@link TypeDescriptor TypeDescriptor&lt;T&gt;} for this
   * {@link PCollection PCollection&lt;T&gt;}. This may allow the enclosing
   * {@link PCollectionTuple}, {@link PCollectionList}, or {@code PTransform<?, PCollection<T>>},
   * etc., to provide more detailed reflective information.
   */
  @Override
  public PCollection<T> setTypeDescriptorInternal(TypeDescriptor<T> typeDescriptor) {
    super.setTypeDescriptorInternal(typeDescriptor);
    return this;
  }

  /**
   * Sets the {@link WindowingStrategy} of this {@link PCollection}.
   *
   * <p>For use by primitive transformations only.
   */
  public PCollection<T> setWindowingStrategyInternal(WindowingStrategy<?, ?> windowingStrategy) {
     this.windowingStrategy = windowingStrategy;
     return this;
  }

  /**
   * Sets the {@link PCollection.IsBounded} of this {@link PCollection}.
   *
   * <p>For use by internal transformations only.
   */
  public PCollection<T> setIsBoundedInternal(IsBounded isBounded) {
    this.isBounded = isBounded;
    return this;
  }

  /**
   * Creates and returns a new {@link PCollection} for a primitive output.
   *
   * <p>For use by primitive transformations only.
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
