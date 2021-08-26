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

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Part of a {@link PCollection}. Elements are output to a bundle, which will cause them to be
 * executed by {@link PTransform PTransforms} that consume the {@link PCollection} this bundle is a
 * part of at a later point. This is an uncommitted bundle and can have elements added to it.
 *
 * @param <T> the type of elements that can be added to this bundle
 */
interface UncommittedBundle<T> {
  /** Returns the PCollection that the elements of this {@link UncommittedBundle} belong to. */
  @Nullable
  PCollection<T> getPCollection();

  /**
   * Outputs an element to this bundle.
   *
   * @param element the element to add to this bundle
   * @return this bundle
   */
  UncommittedBundle<T> add(WindowedValue<T> element);

  /**
   * Commits this {@link UncommittedBundle}, returning an immutable {@link CommittedBundle}
   * containing all of the elements that were added to it. The {@link #add(WindowedValue)} method
   * will throw an {@link IllegalStateException} if called after a call to commit.
   *
   * @param synchronizedProcessingTime the synchronized processing time at which this bundle was
   *     committed
   */
  CommittedBundle<T> commit(Instant synchronizedProcessingTime);
}
