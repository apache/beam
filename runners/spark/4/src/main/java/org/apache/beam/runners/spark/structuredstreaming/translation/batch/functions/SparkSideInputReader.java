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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.sdk.transforms.Materializations.ITERABLE_MATERIALIZATION_URN;
import static org.apache.beam.sdk.transforms.Materializations.MULTIMAP_MATERIALIZATION_URN;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.spark.broadcast.Broadcast;
import org.checkerframework.checker.nullness.qual.Nullable;

/** SideInputReader using broadcasted {@link SideInputValues}. */
public class SparkSideInputReader implements SideInputReader, Serializable {
  private static final SideInputReader EMPTY_READER = new EmptyReader();

  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(ITERABLE_MATERIALIZATION_URN, MULTIMAP_MATERIALIZATION_URN);

  // Map of PCollectionView tag id to broadcasted SideInputValues
  private final Map<String, Broadcast<SideInputValues<?>>> sideInputs;

  public static SideInputReader empty() {
    return EMPTY_READER;
  }

  /**
   * Creates a {@link SideInputReader} for Spark from a map of PCollectionView {@link
   * TupleTag#getId() tag ids} and the corresponding broadcasted {@link SideInputValues}.
   *
   * <p>Note, the materialization of respective {@link PCollectionView PCollectionViews} should be
   * validated ahead of time before any costly creation and broadcast of {@link SideInputValues}.
   */
  public static SideInputReader create(Map<String, Broadcast<SideInputValues<?>>> sideInputs) {
    return sideInputs.isEmpty() ? empty() : new SparkSideInputReader(sideInputs);
  }

  public static void validateMaterializations(Iterable<PCollectionView<?>> views) {
    for (PCollectionView<?> view : views) {
      String viewUrn = view.getViewFn().getMaterialization().getUrn();
      checkArgument(
          SUPPORTED_MATERIALIZATIONS.contains(viewUrn),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          SUPPORTED_MATERIALIZATIONS,
          viewUrn,
          view.getTagInternal().getId());
    }
  }

  private SparkSideInputReader(Map<String, Broadcast<SideInputValues<?>>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  private static <V, T> T iterableView(
      ViewFn<IterableView<V>, T> viewFn, @Nullable List<V> values) {
    return values != null ? viewFn.apply(() -> values) : viewFn.apply(Collections::emptyList);
  }

  private static <K, V, T> T multimapView(
      ViewFn<MultimapView<K, V>, T> viewFn, Coder<K> keyCoder, @Nullable List<KV<K, V>> values) {
    return values != null && !values.isEmpty()
        ? viewFn.apply(InMemoryMultimapSideInputView.fromIterable(keyCoder, values))
        : viewFn.apply(InMemoryMultimapSideInputView.empty());
  }

  @Override
  @SuppressWarnings("unchecked") //
  public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
    Broadcast<SideInputValues<?>> broadcast =
        checkStateNotNull(
            sideInputs.get(view.getTagInternal().getId()), "View %s not available.", view);

    @Nullable List<?> values = broadcast.value().get(window);
    switch (view.getViewFn().getMaterialization().getUrn()) {
      case ITERABLE_MATERIALIZATION_URN:
        return (T) iterableView((ViewFn) view.getViewFn(), values);
      case MULTIMAP_MATERIALIZATION_URN:
        Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
        return (T) multimapView((ViewFn) view.getViewFn(), keyCoder, (List) values);
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown materialization urn '%s'",
                view.getViewFn().getMaterialization().getUrn()));
    }
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal().getId());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  private static class EmptyReader implements SideInputReader, Serializable {
    @Override
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      throw new IllegalArgumentException("Cannot get view from empty SideInputReader");
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }
}
