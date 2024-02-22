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
package org.apache.beam.sdk.util.construction;

import java.util.Map;
import java.util.Objects;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link PCollectionView} created from the components of a {@link SideInput}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RunnerPCollectionView<T> extends PValueBase implements PCollectionView<T> {
  private final TupleTag<Iterable<WindowedValue<?>>> tag;
  private final ViewFn<Iterable<WindowedValue<?>>, T> viewFn;
  private final WindowMappingFn<?> windowMappingFn;
  private final @Nullable WindowingStrategy<?, ?> windowingStrategy;
  private final @Nullable Coder<?> coder;
  private final transient @Nullable PCollection<?> pCollection;

  /** Create a new {@link RunnerPCollectionView} from the provided components. */
  public RunnerPCollectionView(
      @Nullable PCollection<?> pCollection,
      TupleTag<Iterable<WindowedValue<?>>> tag,
      ViewFn<Iterable<WindowedValue<?>>, T> viewFn,
      WindowMappingFn<?> windowMappingFn,
      @Nullable WindowingStrategy<?, ?> windowingStrategy,
      @Nullable Coder<?> coder) {
    this.pCollection = pCollection;
    this.tag = tag;
    this.viewFn = viewFn;
    this.windowMappingFn = windowMappingFn;
    this.windowingStrategy = windowingStrategy;
    this.coder = coder;
  }

  @Override
  public PCollection<?> getPCollection() {
    return pCollection;
  }

  @Override
  public TupleTag<Iterable<WindowedValue<?>>> getTagInternal() {
    return tag;
  }

  @Override
  public ViewFn<Iterable<WindowedValue<?>>, T> getViewFn() {
    return viewFn;
  }

  @Override
  public WindowMappingFn<?> getWindowMappingFn() {
    return windowMappingFn;
  }

  @Override
  public WindowingStrategy<?, ?> getWindowingStrategyInternal() {
    return windowingStrategy;
  }

  @Override
  public Coder<?> getCoderInternal() {
    return coder;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    throw new UnsupportedOperationException(
        String.format("A %s cannot be expanded", RunnerPCollectionView.class.getSimpleName()));
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (!(other instanceof PCollectionView)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    PCollectionView<?> otherView = (PCollectionView<?>) other;
    return tag.equals(otherView.getTagInternal());
  }

  @Override
  public int hashCode() {
    return Objects.hash(tag);
  }
}
