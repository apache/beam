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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PCollectionTuple} is an immutable tuple of heterogeneously-typed {@link PCollection
 * PCollections}, "keyed" by {@link TupleTag TupleTags}. A {@link PCollectionTuple} can be used as
 * the input or output of a {@link PTransform} taking or producing multiple PCollection inputs or
 * outputs that can be of different types, for instance a {@link ParDo} with multiple outputs.
 *
 * <p>A {@link PCollectionTuple} can be created and accessed like follows:
 *
 * <pre>{@code
 * PCollection<String> pc1 = ...;
 * PCollection<Integer> pc2 = ...;
 * PCollection<Iterable<String>> pc3 = ...;
 *
 * // Create TupleTags for each of the PCollections to put in the
 * // PCollectionTuple (the type of the TupleTag enables tracking the
 * // static type of each of the PCollections in the PCollectionTuple):
 * TupleTag<String> tag1 = new TupleTag<>();
 * TupleTag<Integer> tag2 = new TupleTag<>();
 * TupleTag<Iterable<String>> tag3 = new TupleTag<>();
 *
 * // Create a PCollectionTuple with three PCollections:
 * PCollectionTuple pcs =
 *     PCollectionTuple.of(tag1, pc1)
 *                     .and(tag2, pc2)
 *                     .and(tag3, pc3);
 *
 * // Create an empty PCollectionTuple:
 * Pipeline p = ...;
 * PCollectionTuple pcs2 = PCollectionTuple.empty(p);
 *
 * // Get PCollections out of a PCollectionTuple, using the same tags
 * // that were used to put them in:
 * PCollection<Integer> pcX = pcs.get(tag2);
 * PCollection<String> pcY = pcs.get(tag1);
 * PCollection<Iterable<String>> pcZ = pcs.get(tag3);
 *
 * // Get a map of all PCollections in a PCollectionTuple:
 * Map<TupleTag<?>, PCollection<?>> allPcs = pcs.getAll();
 * }</pre>
 */
public class PCollectionTuple implements PInput, POutput {
  /**
   * Returns an empty {@link PCollectionTuple} that is part of the given {@link Pipeline}.
   *
   * <p>A {@link PCollectionTuple} containing additional elements can be created by calling {@link
   * #and} on the result.
   */
  public static PCollectionTuple empty(Pipeline pipeline) {
    return new PCollectionTuple(pipeline);
  }

  /**
   * Returns a singleton {@link PCollectionTuple} containing the given {@link PCollection} keyed by
   * the given {@link TupleTag}.
   *
   * <p>A {@link PCollectionTuple} containing additional elements can be created by calling {@link
   * #and} on the result.
   */
  public static <T> PCollectionTuple of(TupleTag<T> tag, PCollection<T> pc) {
    return empty(pc.getPipeline()).and(tag, pc);
  }

  /**
   * A version of {@link #of(TupleTag, PCollection)} that takes in a String instead of a {@link
   * TupleTag}.
   *
   * <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
   * PCollection, for example when using schema transforms.
   */
  public static <T> PCollectionTuple of(String tag, PCollection<T> pc) {
    return of(new TupleTag<>(tag), pc);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in two PCollections of the same type.
   */
  public static <T> PCollectionTuple of(
      String tag1, PCollection<T> pc1, String tag2, PCollection<T> pc2) {
    return of(tag1, pc1).and(tag2, pc2);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in three PCollections of the same
   * type.
   */
  public static <T> PCollectionTuple of(
      String tag1,
      PCollection<T> pc1,
      String tag2,
      PCollection<T> pc2,
      String tag3,
      PCollection<T> pc3) {
    return of(tag1, pc1, tag2, pc2).and(tag3, pc3);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in four PCollections of the same type.
   */
  public static <T> PCollectionTuple of(
      String tag1,
      PCollection<T> pc1,
      String tag2,
      PCollection<T> pc2,
      String tag3,
      PCollection<T> pc3,
      String tag4,
      PCollection<T> pc4) {
    return of(tag1, pc1, tag2, pc2, tag3, pc3).and(tag4, pc4);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in five PCollections of the same type.
   */
  public static <T> PCollectionTuple of(
      String tag1,
      PCollection<T> pc1,
      String tag2,
      PCollection<T> pc2,
      String tag3,
      PCollection<T> pc3,
      String tag4,
      PCollection<T> pc4,
      String tag5,
      PCollection<T> pc5) {
    return of(tag1, pc1, tag2, pc2, tag3, pc3, tag4, pc4).and(tag5, pc5);
  }

  // To create a PCollectionTuple with more than five inputs, use the and() builder method.

  /**
   * Returns a new {@link PCollectionTuple} that has each {@link PCollection} and {@link TupleTag}
   * of this {@link PCollectionTuple} plus the given {@link PCollection} associated with the given
   * {@link TupleTag}.
   *
   * <p>The given {@link TupleTag} should not already be mapped to a {@link PCollection} in this
   * {@link PCollectionTuple}.
   *
   * <p>Each {@link PCollection} in the resulting {@link PCollectionTuple} must be part of the same
   * {@link Pipeline}.
   */
  public <T> PCollectionTuple and(TupleTag<T> tag, PCollection<T> pc) {
    if (pc.getPipeline() != pipeline) {
      throw new IllegalArgumentException("PCollections come from different Pipelines");
    }

    return new PCollectionTuple(
        pipeline,
        new ImmutableMap.Builder<TupleTag<?>, PCollection<?>>()
            .putAll(pcollectionMap)
            .put(tag, pc)
            .build());
  }

  /**
   * A version of {@link #and(TupleTag, PCollection)} that takes in a String instead of a TupleTag.
   *
   * <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
   * PCollection, for example when using schema transforms.
   */
  public <T> PCollectionTuple and(String tag, PCollection<T> pc) {
    return and(new TupleTag<>(tag), pc);
  }

  /**
   * Returns whether this {@link PCollectionTuple} contains a {@link PCollection} with the given
   * tag.
   */
  public <T> boolean has(TupleTag<T> tag) {
    return pcollectionMap.containsKey(tag);
  }

  /**
   * Returns whether this {@link PCollectionTuple} contains a {@link PCollection} with the given
   * tag.
   */
  public <T> boolean has(String tag) {
    return has(new TupleTag<>(tag));
  }

  /**
   * Returns the {@link PCollection} associated with the given {@link TupleTag} in this {@link
   * PCollectionTuple}. Throws {@link IllegalArgumentException} if there is no such {@link
   * PCollection}, i.e., {@code !has(tag)}.
   */
  public <T> PCollection<T> get(TupleTag<T> tag) {
    @SuppressWarnings("unchecked")
    PCollection<T> pcollection = (PCollection<T>) pcollectionMap.get(tag);
    if (pcollection == null) {
      throw new IllegalArgumentException("TupleTag not found in this PCollectionTuple tuple");
    }
    return pcollection;
  }

  /**
   * Returns the {@link PCollection} associated with the given tag in this {@link PCollectionTuple}.
   * Throws {@link IllegalArgumentException} if there is no such {@link PCollection}, i.e., {@code
   * !has(tag)}.
   */
  public <T> PCollection<T> get(String tag) {
    return get(new TupleTag<>(tag));
  }

  /**
   * Returns an immutable Map from {@link TupleTag} to corresponding {@link PCollection}, for all
   * the members of this {@link PCollectionTuple}.
   */
  public Map<TupleTag<?>, PCollection<?>> getAll() {
    return pcollectionMap;
  }

  /**
   * Like {@link #apply(String, PTransform)} but defaulting to the name of the {@link PTransform}.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(PTransform<? super PCollectionTuple, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this input {@link PCollectionTuple}, using {@code name}
   * to identify this specific application of the transform. This name is used in various places,
   * including the monitoring UI, logging, and to stably identify this application node in the job
   * graph.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PCollectionTuple, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  final Pipeline pipeline;
  final Map<TupleTag<?>, PCollection<?>> pcollectionMap;

  PCollectionTuple(Pipeline pipeline) {
    this(pipeline, new LinkedHashMap<>());
  }

  PCollectionTuple(Pipeline pipeline, Map<TupleTag<?>, PCollection<?>> pcollectionMap) {
    this.pipeline = pipeline;
    this.pcollectionMap = Collections.unmodifiableMap(pcollectionMap);
  }

  /**
   * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
   *
   * <p>Returns a {@link PCollectionTuple} with each of the given tags mapping to a new output
   * {@link PCollection}.
   *
   * <p>For use by primitive transformations only.
   */
  @Internal
  public static PCollectionTuple ofPrimitiveOutputsInternal(
      Pipeline pipeline,
      TupleTagList outputTags,
      Map<TupleTag<?>, Coder<?>> coders,
      WindowingStrategy<?, ?> windowingStrategy,
      IsBounded isBounded) {
    Map<TupleTag<?>, PCollection<?>> pcollectionMap = new LinkedHashMap<>();
    for (TupleTag<?> outputTag : outputTags.tupleTags) {
      if (pcollectionMap.containsKey(outputTag)) {
        throw new IllegalArgumentException("TupleTag already present in this tuple");
      }

      // In fact, `token` and `outputCollection` should have
      // types TypeDescriptor<T> and PCollection<T> for some
      // unknown T. It is safe to create `outputCollection`
      // with type PCollection<Object> because it has the same
      // erasure as the correct type. When a transform adds
      // elements to `outputCollection` they will be of type T.
      @SuppressWarnings("unchecked")
      PCollection outputCollection =
          PCollection.createPrimitiveOutputInternal(
                  pipeline, windowingStrategy, isBounded, (Coder) coders.get(outputTag))
              .setTypeDescriptor(outputTag.getTypeDescriptor());

      pcollectionMap.put(outputTag, outputCollection);
    }
    return new PCollectionTuple(pipeline, pcollectionMap);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.copyOf(pcollectionMap);
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    // All component PCollections will already have been finished. Update their names if
    // appropriate.
    int i = 0;
    for (Map.Entry<TupleTag<?>, PCollection<?>> entry : pcollectionMap.entrySet()) {
      TupleTag<?> tag = entry.getKey();
      PCollection<?> pc = entry.getValue();
      if (pc.getName().equals(PValueBase.defaultName(transformName))) {
        pc.setName(String.format("%s.%s", transformName, tag.getOutName(i)));
      }
      i++;
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof PCollectionTuple)) {
      return false;
    }
    PCollectionTuple that = (PCollectionTuple) other;
    return this.pipeline.equals(that.pipeline) && this.pcollectionMap.equals(that.pcollectionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.pipeline, this.pcollectionMap);
  }
}
