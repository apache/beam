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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.local.Bundle;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Part of a {@link PCollection}. Elements are output to an {@link UncommittedBundle}, which will
 * eventually committed. Committed elements are executed by the {@link PTransform PTransforms} that
 * consume the {@link PCollection} this bundle is a part of at a later point.
 *
 * @param <T> the type of elements contained within this bundle
 */
interface CommittedBundle<T> extends Bundle<T, PCollection<T>> {
  /** Returns the PCollection that the elements of this bundle belong to. */
  @Override
  @Nullable
  PCollection<T> getPCollection();

  /**
   * Returns the key that was output in the most recent {@code GroupByKey} in the execution of this
   * bundle.
   */
  @Override
  StructuralKey<?> getKey();

  /**
   * Returns an {@link Iterable} containing all of the elements that have been added to this {@link
   * CommittedBundle}.
   */
  Iterable<WindowedValue<T>> getElements();

  /**
   * Return the minimum timestamp among elements in this bundle.
   *
   * <p>This should be equivalent to iterating over all of the elements within a bundle and
   * selecting the minimum timestamp from among them.
   */
  @Override
  Instant getMinimumTimestamp();

  /**
   * Returns the processing time output watermark at the time the producing {@code Executable}
   * committed this bundle. Downstream synchronized processing time watermarks cannot progress past
   * this point before consuming this bundle.
   *
   * <p>This value is no greater than the earliest incomplete processing time or synchronized
   * processing time {@link TimerData timer} at the time this bundle was committed, including any
   * timers that fired to produce this bundle.
   */
  @Override
  Instant getSynchronizedProcessingOutputWatermark();
  /**
   * Return a new {@link CommittedBundle} that is like this one, except calls to {@link
   * #getElements()} will return the provided elements. This bundle is unchanged.
   *
   * <p>The value of the {@link #getSynchronizedProcessingOutputWatermark() synchronized processing
   * output watermark} of the returned {@link CommittedBundle} is equal to the value returned from
   * the current bundle. This is used to ensure a {@link PTransform} that could not complete
   * processing on input elements properly holds the synchronized processing time to the appropriate
   * value.
   */
  CommittedBundle<T> withElements(Iterable<WindowedValue<T>> elements);
}
