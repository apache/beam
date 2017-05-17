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

package org.apache.beam.runners.core.construction;

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.SideInput;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/** A {@link PCollectionView} created from the components of a {@link SideInput}. */
class RunnerPCollectionView<T> extends PValueBase implements PCollectionView<T> {
  private final TupleTag<Iterable<WindowedValue<?>>> tag;
  private final ViewFn<Iterable<WindowedValue<?>>, T> viewFn;
  private final WindowMappingFn<?> windowMappingFn;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Coder<Iterable<WindowedValue<?>>> coder;

  /**
   * Create a new {@link RunnerPCollectionView} from the provided components.
   */
  RunnerPCollectionView(
      TupleTag<Iterable<WindowedValue<?>>> tag,
      ViewFn<Iterable<WindowedValue<?>>, T> viewFn,
      WindowMappingFn<?> windowMappingFn,
      @Nullable WindowingStrategy<?, ?> windowingStrategy,
      @Nullable Coder<Iterable<WindowedValue<?>>> coder) {
    this.tag = tag;
    this.viewFn = viewFn;
    this.windowMappingFn = windowMappingFn;
    this.windowingStrategy = windowingStrategy;
    this.coder = coder;
  }

  @Nullable
  @Override
  public PCollection<?> getPCollection() {
    throw new IllegalStateException(
        String.format("Cannot call getPCollection on a %s", getClass().getSimpleName()));
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
  public Coder<Iterable<WindowedValue<?>>> getCoderInternal() {
    return coder;
  }
}
