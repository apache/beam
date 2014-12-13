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

import com.google.cloud.dataflow.sdk.util.WindowedValue;

import java.io.Serializable;

/**
 * A {@code PCollectionView<T, WT>} is an immutable view of a
 * {@link PCollection} that can be accessed e.g. as a
 * side input to a {@link DoFn}.
 *
 * <p> A {@PCollectionView} should always be the output of a {@link PTransform}. It is
 * the joint responsibility of this transform and each {@link PipelineRunner} to
 * implement the view in a runner-specific manner.
 *
 * @param <T> the type of the value(s) accessible via this {@code PCollectionView}
 * @param <WT> the type of the windowed value(s) accessible via this {@code PCollectionView}
 */
public interface PCollectionView<T, WT> extends PValue, Serializable {
  /**
   * A unique identifier, for internal use.
   */
  public TupleTag<Iterable<WindowedValue<?>>> getTagInternal();

  /**
   * For internal use only.
   */
  public T fromIterableInternal(Iterable<WindowedValue<?>> contents);
}
