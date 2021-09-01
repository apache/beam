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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PCollectionRowTuple} is an immutable tuple of heterogeneously-typed {@link PCollection
 * PCollections}, "keyed" by {@link TupleTag TupleTags}. A {@link PCollectionRowTuple} can be used
 * as the input or output of a {@link PTransform} taking or producing multiple PCollection inputs or
 * outputs that can be of different types, for instance a {@link ParDo} with multiple outputs.
 *
 * <p>A {@link PCollectionRowTuple} can be created and accessed like follows:
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
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class PCollectionRowTuple implements PInput, POutput {
  /**
   * Returns an empty {@link PCollectionRowTuple} that is part of the given {@link Pipeline}.
   *
   * <p>A {@link PCollectionRowTuple} containing additional elements can be created by calling
   * {@link #and} on the result.
   */
  public static PCollectionRowTuple empty(Pipeline pipeline) {
    return new PCollectionRowTuple(pipeline);
  }

  /**
   * Returns a singleton {@link PCollectionRowTuple} containing the given {@link PCollection} keyed
   * by the given {@link TupleTag}.
   *
   * <p>A {@link PCollectionRowTuple} containing additional elements can be created by calling
   * {@link #and} on the result.
   */
  public static PCollectionRowTuple of(TupleTag<Row> tag, PCollection<Row> pc) {
    return empty(pc.getPipeline()).and(tag, pc);
  }

  /**
   * A version of {@link #of(TupleTag, PCollection)} that takes in a String instead of a {@link
   * TupleTag}.
   *
   * <p>This method is simpler for cases when a typed tuple-tag is not needed to extract a
   * PCollection, for example when using schema transforms.
   */
  public static PCollectionRowTuple of(String tag, PCollection<Row> pc) {
    return of(new TupleTag<>(tag), pc);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in two PCollections of the same type.
   */
  public static PCollectionRowTuple of(
      String tag1, PCollection<Row> pc1, String tag2, PCollection<Row> pc2) {
    return of(tag1, pc1).and(tag2, pc2);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in three PCollections of the same
   * type.
   */
  public static PCollectionRowTuple of(
      String tag1,
      PCollection<Row> pc1,
      String tag2,
      PCollection<Row> pc2,
      String tag3,
      PCollection<Row> pc3) {
    return of(tag1, pc1, tag2, pc2).and(tag3, pc3);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in four PCollections of the same type.
   */
  public static PCollectionRowTuple of(
      String tag1,
      PCollection<Row> pc1,
      String tag2,
      PCollection<Row> pc2,
      String tag3,
      PCollection<Row> pc3,
      String tag4,
      PCollection<Row> pc4) {
    return of(tag1, pc1, tag2, pc2, tag3, pc3).and(tag4, pc4);
  }

  /**
   * A version of {@link #of(String, PCollection)} that takes in five PCollections of the same type.
   */
  public static PCollectionRowTuple of(
      String tag1,
      PCollection<Row> pc1,
      String tag2,
      PCollection<Row> pc2,
      String tag3,
      PCollection<Row> pc3,
      String tag4,
      PCollection<Row> pc4,
      String tag5,
      PCollection<Row> pc5) {
    return of(tag1, pc1, tag2, pc2, tag3, pc3, tag4, pc4).and(tag5, pc5);
  }

  // To create a PCollectionTuple with more than five inputs, use the and() builder method.

  /**
   * Returns a new {@link PCollectionRowTuple} that has each {@link PCollection} and {@link
   * TupleTag} of this {@link PCollectionRowTuple} plus the given {@link PCollection} associated
   * with the given {@link TupleTag}.
   *
   * <p>The given {@link TupleTag} should not already be mapped to a {@link PCollection} in this
   * {@link PCollectionRowTuple}.
   *
   * <p>Each {@link PCollection} in the resulting {@link PCollectionRowTuple} must be part of the
   * same {@link Pipeline}.
   */
  public PCollectionRowTuple and(TupleTag<Row> tag, PCollection<Row> pc) {
    if (pc.getPipeline() != pipeline) {
      throw new IllegalArgumentException("PCollections come from different Pipelines");
    }

    return new PCollectionRowTuple(
        pipeline,
        new ImmutableMap.Builder<TupleTag<Row>, PCollection<Row>>()
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
  public PCollectionRowTuple and(String tag, PCollection<Row> pc) {
    return and(new TupleTag<>(tag), pc);
  }

  /**
   * Returns whether this {@link PCollectionRowTuple} contains a {@link PCollection} with the given
   * tag.
   */
  public boolean has(TupleTag<Row> tag) {
    return pcollectionMap.containsKey(tag);
  }

  /**
   * Returns whether this {@link PCollectionRowTuple} contains a {@link PCollection} with the given
   * tag.
   */
  public boolean has(String tag) {
    return has(new TupleTag<>(tag));
  }

  /**
   * Returns the {@link PCollection} associated with the given {@link TupleTag} in this {@link
   * PCollectionRowTuple}. Throws {@link IllegalArgumentException} if there is no such {@link
   * PCollection}, i.e., {@code !has(tag)}.
   */
  public PCollection<Row> get(TupleTag<Row> tag) {
    @SuppressWarnings("unchecked")
    PCollection<Row> pcollection = pcollectionMap.get(tag);
    if (pcollection == null) {
      throw new IllegalArgumentException("TupleTag not found in this PCollectionTuple tuple");
    }
    return pcollection;
  }

  /**
   * Returns the {@link PCollection} associated with the given tag in this {@link
   * PCollectionRowTuple}. Throws {@link IllegalArgumentException} if there is no such {@link
   * PCollection}, i.e., {@code !has(tag)}.
   */
  public PCollection<Row> get(String tag) {
    return get(new TupleTag<Row>(tag));
  }

  /**
   * Returns an immutable Map from {@link TupleTag} to corresponding {@link PCollection}, for all
   * the members of this {@link PCollectionRowTuple}.
   */
  public Map<TupleTag<Row>, PCollection<Row>> getAll() {
    return pcollectionMap;
  }

  /**
   * Like {@link #apply(String, PTransform)} but defaulting to the name of the {@link PTransform}.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      PTransform<? super PCollectionRowTuple, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this input {@link PCollectionRowTuple}, using {@code
   * name} to identify this specific application of the transform. This name is used in various
   * places, including the monitoring UI, logging, and to stably identify this application node in
   * the job graph.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PCollectionRowTuple, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  final Pipeline pipeline;
  final Map<TupleTag<Row>, PCollection<Row>> pcollectionMap;

  PCollectionRowTuple(Pipeline pipeline) {
    this(pipeline, new LinkedHashMap<>());
  }

  PCollectionRowTuple(Pipeline pipeline, Map<TupleTag<Row>, PCollection<Row>> pcollectionMap) {
    this.pipeline = pipeline;
    this.pcollectionMap = Collections.unmodifiableMap(pcollectionMap);
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
    for (Map.Entry<TupleTag<Row>, PCollection<Row>> entry : pcollectionMap.entrySet()) {
      TupleTag<Row> tag = entry.getKey();
      PCollection<Row> pc = entry.getValue();
      if (pc.getName().equals(PValueBase.defaultName(transformName))) {
        pc.setName(String.format("%s.%s", transformName, tag.getOutName(i)));
      }
      i++;
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof PCollectionRowTuple)) {
      return false;
    }
    PCollectionRowTuple that = (PCollectionRowTuple) other;
    return this.pipeline.equals(that.pipeline) && this.pcollectionMap.equals(that.pcollectionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.pipeline, this.pcollectionMap);
  }
}
