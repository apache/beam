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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PCollectionRowTuple} is an immutable tuple of {@link PCollection PCollection<Row>s},
 * "keyed" by a string tag. A {@link PCollectionRowTuple} can be used as the input or output of a
 * {@link PTransform} taking or producing multiple {@code PCollection<Row>} inputs or outputs.
 *
 * <p>A {@link PCollectionRowTuple} can be created and accessed like follows:
 *
 * <pre>{@code
 * PCollection<Row> pc1 = ...;
 * PCollection<Row> pc2 = ...;
 *
 * // Create tags for each of the PCollections to put in the PCollectionRowTuple:
 * String tag1 = "pc1";
 * String tag2 = "pc2";
 * String tag3 = "pc3";
 *
 * // Create a PCollectionRowTuple with three PCollections:
 * PCollectionRowTuple pcs = PCollectionRowTuple.of(tag1, pc1).and(tag2, pc2).and(tag3, pc3);
 *
 * // Create an empty PCollectionRowTuple:
 * Pipeline p = ...;
 * PCollectionRowTuple pcs2 = PCollectionRowTuple.empty(p);
 *
 * // Get PCollections out of a PCollectionRowTuple, using the same tags that were used to put them in:
 * PCollection<Row> pcX = pcs.get(tag2);
 * PCollection<Row> pcY = pcs.get(tag1);
 *
 * // Get a map of all PCollections in a PCollectionRowTuple:
 * Map<String, PCollection<Row>> allPcs = pcs.getAll();
 * }</pre>
 */
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
   * by the given tag.
   *
   * <p>A {@link PCollectionRowTuple} containing additional elements can be created by calling
   * {@link #and} on the result.
   */
  public static PCollectionRowTuple of(String tag, PCollection<Row> pc) {
    return empty(pc.getPipeline()).and(tag, pc);
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

  // To create a PCollectionRowTuple with more than five inputs, use the and() builder method.

  /**
   * Returns a new {@link PCollectionRowTuple} that has each {@link PCollection} and tag of this
   * {@link PCollectionRowTuple} plus the given {@link PCollection} associated with the given tag.
   *
   * <p>The given tag should not already be mapped to a {@link PCollection} in this {@link
   * PCollectionRowTuple}.
   *
   * <p>Each {@link PCollection} in the resulting {@link PCollectionRowTuple} must be part of the
   * same {@link Pipeline}.
   */
  public PCollectionRowTuple and(String tag, PCollection<Row> pc) {
    if (pc.getPipeline() != pipeline) {
      throw new IllegalArgumentException("PCollections come from different Pipelines");
    }

    return new PCollectionRowTuple(
        pipeline,
        new ImmutableMap.Builder<String, PCollection<Row>>()
            .putAll(pcollectionMap)
            .put(tag, pc)
            .build());
  }

  /**
   * Returns whether this {@link PCollectionRowTuple} contains a {@link PCollection} with the given
   * tag.
   */
  public boolean has(String tag) {
    return pcollectionMap.containsKey(tag);
  }

  /**
   * Returns the {@link PCollection} associated with the given {@link String} in this {@link
   * PCollectionRowTuple}. Throws {@link IllegalArgumentException} if there is no such {@link
   * PCollection}, i.e., {@code !has(tag)}.
   */
  public PCollection<Row> get(String tag) {
    @SuppressWarnings("unchecked")
    PCollection<Row> pcollection = pcollectionMap.get(tag);
    if (pcollection == null) {
      throw new IllegalArgumentException("Tag not found in this PCollectionRowTuple tuple");
    }
    return pcollection;
  }

  /**
   * Like {@link #get(String)}, but is a convenience method to get a single PCollection without
   * providing a tag for that output. Use only when there is a single collection in this tuple.
   *
   * <p>Throws {@link IllegalStateException} if more than one output exists in the {@link
   * PCollectionRowTuple}.
   */
  public PCollection<Row> getSinglePCollection() {
    Preconditions.checkState(
        pcollectionMap.size() == 1,
        "Expected exactly one output PCollection<Row>, but found %s. "
            + "Please try retrieving a specified output using get(<tag>) instead.",
        pcollectionMap.size());
    return get(pcollectionMap.entrySet().iterator().next().getKey());
  }

  /**
   * Returns an immutable Map from tag to corresponding {@link PCollection}, for all the members of
   * this {@link PCollectionRowTuple}.
   */
  public Map<String, PCollection<Row>> getAll() {
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
  final Map<String, PCollection<Row>> pcollectionMap;

  PCollectionRowTuple(Pipeline pipeline) {
    this(pipeline, new LinkedHashMap<>());
  }

  PCollectionRowTuple(Pipeline pipeline, Map<String, PCollection<Row>> pcollectionMap) {
    this.pipeline = pipeline;
    this.pcollectionMap = Collections.unmodifiableMap(pcollectionMap);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> builder = ImmutableMap.builder();
    pcollectionMap.forEach((tag, value) -> builder.put(new TupleTag<Row>(tag), value));
    return builder.build();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {
    // All component PCollections will already have been finished. Update their names if
    // appropriate.
    for (Map.Entry<String, PCollection<Row>> entry : pcollectionMap.entrySet()) {
      String tag = entry.getKey();
      PCollection<Row> pc = entry.getValue();
      if (pc.getName().equals(PValueBase.defaultName(transformName))) {
        pc.setName(String.format("%s.%s", transformName, tag));
      }
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
