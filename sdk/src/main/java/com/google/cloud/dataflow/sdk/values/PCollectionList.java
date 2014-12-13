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
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A {@code PCollectionList<T>} is an immutable list of homogeneously
 * typed {@code PCollection<T>}s.  A PCollectionList is used, for
 * instance, as the input to
 * {@link com.google.cloud.dataflow.sdk.transforms.Flatten} or the
 * output of
 * {@link com.google.cloud.dataflow.sdk.transforms.Partition}.
 *
 * <p> PCollectionLists can be created and accessed like follows:
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
 * @param <T> the type of the elements of all the PCollections in this list
 */
public class PCollectionList<T> implements PInput, POutput {
  /**
   * Returns an empty PCollectionList that is part of the given Pipeline.
   *
   * <p> Longer PCollectionLists can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> empty(Pipeline pipeline) {
    return new PCollectionList<>(pipeline);
  }

  /**
   * Returns a singleton PCollectionList containing the given PCollection.
   *
   * <p> Longer PCollectionLists can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> of(PCollection<T> pc) {
    return new PCollectionList<T>(pc.getPipeline()).and(pc);
  }

  /**
   * Returns a PCollectionList containing the given PCollections, in order.
   *
   * <p> The argument list cannot be empty.
   *
   * <p> All the PCollections in the resulting PCollectionList must be
   * part of the same Pipeline.
   *
   * <p> Longer PCollectionLists can be created by calling
   * {@link #and} on the result.
   */
  public static <T> PCollectionList<T> of(Iterable<PCollection<T>> pcs) {
    Iterator<PCollection<T>> pcsIter = pcs.iterator();
    if (!pcsIter.hasNext()) {
      throw new IllegalArgumentException(
          "must either have a non-empty list of PCollections, " +
          "or must first call empty(Pipeline)");
    }
    return new PCollectionList<T>(pcsIter.next().getPipeline()).and(pcs);
  }

  /**
   * Returns a new PCollectionList that has all the PCollections of
   * this PCollectionList plus the given PCollection appended to the end.
   *
   * <p> All the PCollections in the resulting PCollectionList must be
   * part of the same Pipeline.
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
   * Returns a new PCollectionList that has all the PCollections of
   * this PCollectionList plus the given PCollections appended to the end,
   * in order.
   *
   * <p> All the PCollections in the resulting PCollectionList must be
   * part of the same Pipeline.
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
   * Returns the number of PCollections in this PCollectionList.
   */
  public int size() {
    return pcollections.size();
  }

  /**
   * Returns the PCollection at the given index (origin zero).  Throws
   * IndexOutOfBounds if the index is out of the range
   * {@code [0..size()-1]}.
   */
  public PCollection<T> get(int index) {
    return pcollections.get(index);
  }

  /**
   * Returns an immutable List of all the PCollections in this PCollectionList.
   */
  public List<PCollection<T>> getAll() {
    return pcollections;
  }

  /**
   * Applies the given PTransform to this input {@code PCollectionList<T>},
   * and returns the PTransform's Output.
   */
  public <Output extends POutput> Output apply(
      PTransform<PCollectionList<T>, Output> t) {
    return Pipeline.applyTransform(this, t);
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
  public void recordAsOutput(Pipeline pipeline,
                             PTransform<?, ?> transform) {
    if (this.pipeline != null && this.pipeline != pipeline) {
      throw new AssertionError(
          "not expecting to change the Pipeline owning a PCollectionList");
    }
    int i = 0;
    for (PCollection<T> pc : pcollections) {
      pc.recordAsOutput(pipeline, transform, "out" + i);
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
