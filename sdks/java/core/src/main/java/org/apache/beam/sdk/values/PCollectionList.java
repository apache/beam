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
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Partition;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link PCollectionList PCollectionList&lt;T&gt;} is an immutable list of homogeneously
 * typed {@link PCollection PCollection&lt;T&gt;s}. A {@link PCollectionList} is used, for
 * instance, as the input to
 * {@link Flatten} or the output of {@link Partition}.
 *
 * <p>PCollectionLists can be created and accessed like follows:
 * <pre> {@code
 * PCollection<String> pc1 = ...;
 * PCollection<String> pc2 = ...;
 * PCollection<String> pc3 = ...;
 *
 * // Create a PCollectionList with three PCollections:
 * PCollectionList<String> pcs = PCollectionList.of(pc1).and(pc2).and(pc3);
 *
 * // Create an empty PCollectionList:
 * Pipeline p = ...;
 * PCollectionList<String> pcs2 = PCollectionList.<String>empty(p);
 *
 * // Get PCollections out of a PCollectionList, by index (origin 0):
 * PCollection<String> pcX = pcs.get(1);
 * PCollection<String> pcY = pcs.get(0);
 * PCollection<String> pcZ = pcs.get(2);
 *
 * // Get a list of all PCollections in a PCollectionList:
 * List<PCollection<String>> allPcs = pcs.getAll();
 * } </pre>
 *
 * @param <T> the type of the elements of all the {@link PCollection PCollections} in this list
 */
public class PCollectionList<T> implements PInput, POutput {
  /**
   * Returns an empty {@link PCollectionList} that is part of the given {@link Pipeline}.
   *
   * <p>Longer {@link PCollectionList PCollectionLists} can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> empty(Pipeline pipeline) {
    return new PCollectionList<>(pipeline);
  }

  /**
   * Returns a singleton {@link PCollectionList} containing the given {@link PCollection}.
   *
   * <p>Longer {@link PCollectionList PCollectionLists} can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> of(PCollection<T> pc) {
    return new PCollectionList<T>(pc.getPipeline()).and(pc);
  }

  /**
   * Returns a {@link PCollectionList} containing the given {@link PCollection PCollections},
   * in order.
   *
   * <p>The argument list cannot be empty.
   *
   * <p>All the {@link PCollection PCollections} in the resulting {@link PCollectionList} must be
   * part of the same {@link Pipeline}.
   *
   * <p>Longer PCollectionLists can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> of(Iterable<PCollection<T>> pcs) {
    Iterator<PCollection<T>> pcsIter = pcs.iterator();
    if (!pcsIter.hasNext()) {
      throw new IllegalArgumentException(
          "must either have a non-empty list of PCollections, "
          + "or must first call empty(Pipeline)");
    }
    return new PCollectionList<T>(pcsIter.next().getPipeline()).and(pcs);
  }

  /**
   * Returns a new {@link PCollectionList} that has all the {@link PCollection PCollections} of
   * this {@link PCollectionList} plus the given {@link PCollection} appended to the end.
   *
   * <p>All the {@link PCollection PCollections} in the resulting {@link PCollectionList} must be
   * part of the same {@link Pipeline}.
   */
  public PCollectionList<T> and(PCollection<T> pc) {
    if (pc.getPipeline() != pipeline) {
      throw new IllegalArgumentException(
          "PCollections come from different Pipelines");
    }
    return new PCollectionList<>(pipeline,
        new ImmutableList.Builder<PCollection<T>>()
            .addAll(pcollections)
            .add(pc)
            .build());
  }

  /**
   * Returns a new {@link PCollectionList} that has all the {@link PCollection PCollections} of
   * this {@link PCollectionList} plus the given {@link PCollection PCollections} appended to the
   * end, in order.
   *
   * <p>All the {@link PCollection PCollections} in the resulting {@link PCollectionList} must be
   * part of the same {@link Pipeline}.
   */
  public PCollectionList<T> and(Iterable<PCollection<T>> pcs) {
    List<PCollection<T>> copy = new ArrayList<>(pcollections);
    for (PCollection<T> pc : pcs) {
      if (pc.getPipeline() != pipeline) {
        throw new IllegalArgumentException(
            "PCollections come from different Pipelines");
      }
      copy.add(pc);
    }
    return new PCollectionList<>(pipeline, copy);
  }

  /**
   * Returns the number of {@link PCollection PCollections} in this {@link PCollectionList}.
   */
  public int size() {
    return pcollections.size();
  }

  /**
   * Returns the {@link PCollection} at the given index (origin zero).
   *
   * @throws IndexOutOfBoundsException if the index is out of the range
   * {@code [0..size()-1]}.
   */
  public PCollection<T> get(int index) {
    return pcollections.get(index);
  }

  /**
   * Returns an immutable List of all the {@link PCollection PCollections} in this
   * {@link PCollectionList}.
   */
  public List<PCollection<T>> getAll() {
    return pcollections;
  }

  /**
   * Like {@link #apply(String, PTransform)} but defaulting to the name
   * of the {@code PTransform}.
   */
  public <OutputT extends POutput> OutputT apply(
      PTransform<PCollectionList<T>, OutputT> t) {
    return Pipeline.applyTransform(this, t);
  }

  /**
   * Applies the given {@link PTransform} to this input {@link PCollectionList},
   * using {@code name} to identify this specific application of the transform.
   * This name is used in various places, including the monitoring UI, logging,
   * and to stably identify this application node in the job graph.
   *
   * @return the output of the applied {@link PTransform}
   */
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<PCollectionList<T>, OutputT> t) {
    return Pipeline.applyTransform(name, this, t);
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  final Pipeline pipeline;
  final List<PCollection<T>> pcollections;

  PCollectionList(Pipeline pipeline) {
    this(pipeline, new ArrayList<PCollection<T>>());
  }

  PCollectionList(Pipeline pipeline, List<PCollection<T>> pcollections) {
    this.pipeline = pipeline;
    this.pcollections = Collections.unmodifiableList(pcollections);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Collection<? extends PValue> expand() {
    return pcollections;
  }

  @Override
  public void recordAsOutput(AppliedPTransform<?, ?, ?> transform) {
    int i = 0;
    for (PCollection<T> pc : pcollections) {
      pc.recordAsOutput(transform, "out" + i);
      i++;
    }
  }

  @Override
  public void finishSpecifying() {
    for (PCollection<T> pc : pcollections) {
      pc.finishSpecifying();
    }
  }

  @Override
  public void finishSpecifyingOutput() {
    for (PCollection<T> pc : pcollections) {
      pc.finishSpecifyingOutput();
    }
  }
}
