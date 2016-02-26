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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;

import java.io.Serializable;

/**
 * A {@link PCollectionView PCollectionView&lt;T&gt;} is an immutable view of a {@link PCollection}
 * as a value of type {@code T} that can be accessed
 * as a side input to a {@link ParDo} transform.
 *
 * <p>A {@link PCollectionView} should always be the output of a
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}. It is the joint responsibility of
 * this transform and each {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner} to implement
 * the view in a runner-specific manner.
 *
 * <p>The most common case is using the {@link View} transforms to prepare a {@link PCollection}
 * for use as a side input to {@link ParDo}. See {@link View#asSingleton()},
 * {@link View#asIterable()}, and {@link View#asMap()} for more detail on specific views
 * available in the SDK.
 *
 * @param <T> the type of the value(s) accessible via this {@link PCollectionView}
 */
public interface PCollectionView<T> extends PValue, Serializable {
  /**
   * A unique identifier, for internal use.
   */
  public TupleTag<Iterable<WindowedValue<?>>> getTagInternal();

  /**
   * For internal use only.
   */
  public T fromIterableInternal(Iterable<WindowedValue<?>> contents);

  /**
   * For internal use only.
   */
  public WindowingStrategy<?, ?> getWindowingStrategyInternal();

  /**
   * For internal use only.
   */
  public Coder<Iterable<WindowedValue<?>>> getCoderInternal();
}
