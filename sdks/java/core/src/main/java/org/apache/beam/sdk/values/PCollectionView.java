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

import java.io.Serializable;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PCollectionView PCollectionView&lt;T&gt;} is an immutable view of a {@link PCollection}
 * as a value of type {@code T} that can be accessed as a side input to a {@link ParDo} transform.
 *
 * <p>A {@link PCollectionView} should always be the output of a {@link
 * org.apache.beam.sdk.transforms.PTransform}. It is the joint responsibility of this transform and
 * each {@link PipelineRunner} to implement the view in a runner-specific manner.
 *
 * <p>The most common case is using the {@link View} transforms to prepare a {@link PCollection} for
 * use as a side input to {@link ParDo}. See {@link View#asSingleton()}, {@link View#asIterable()},
 * and {@link View#asMap()} for more detail on specific views available in the SDK.
 *
 * <p>NOTE: View methods should not be considered to be user-accessible. They are implementation
 * details of how a Runner obtains information required to construct a view, and are subject to
 * change at any time.
 *
 * @param <T> the type of the value(s) accessible via this {@link PCollectionView}
 */
public interface PCollectionView<T> extends PValue, Serializable {
  /**
   * <b>For internal use only.</b>
   *
   * <p>Gets the {@link PCollection} this {@link PCollectionView} was created from.
   *
   * <p>The {@link PCollection} may not be available in all contexts.
   */
  @Nullable
  @Internal
  PCollection<?> getPCollection();
  /**
   * <b>For internal use only.</b>
   *
   * @deprecated this method will be removed entirely. The {@link PCollection} underlying a side
   *     input, is part of the side input's specification with a {@link ParDo} transform, which will
   *     obtain that information via a package-private channel.
   */
  @Deprecated
  @Internal
  TupleTag<?> getTagInternal();

  /**
   * <b>For internal use only.</b>
   *
   * @deprecated this method will be removed entirely. The {@link ViewFn} for a side input is an
   *     attribute of the side input's specification with a {@link ParDo} transform, which will
   *     obtain this specification via a package-private channel.
   */
  @Deprecated
  @Internal
  ViewFn<?, T> getViewFn();

  /**
   * <b>For internal use only.</b>
   *
   * <p>Returns the {@link WindowMappingFn} used to map windows from a main input to the side input
   * of this {@link PCollectionView}.
   */
  @Experimental(Kind.CORE_RUNNERS_ONLY)
  @Internal
  WindowMappingFn<?> getWindowMappingFn();

  /**
   * <b>For internal use only.</b>
   *
   * @deprecated this method will be removed entirely. The {@link PCollection} underlying a side
   *     input, including its {@link WindowingStrategy}, is part of the side input's specification
   *     with a {@link ParDo} transform, which will obtain that information via a package-private
   *     channel.
   */
  @Deprecated
  @Internal
  WindowingStrategy<?, ?> getWindowingStrategyInternal();

  /**
   * <b>For internal use only.</b>
   *
   * @deprecated this method will be removed entirely. The {@link PCollection} underlying a side
   *     input, including its {@link Coder}, is part of the side input's specification with a {@link
   *     ParDo} transform, which will obtain that information via a package-private channel.
   */
  @Deprecated
  @Internal
  Coder<?> getCoderInternal();
}
