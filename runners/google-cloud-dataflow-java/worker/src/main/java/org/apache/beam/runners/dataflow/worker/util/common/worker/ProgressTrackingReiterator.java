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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.runners.dataflow.worker.util.common.ForwardingReiterator;
import org.apache.beam.sdk.util.common.Reiterator;

/**
 * Implements a {@link Reiterator} that uses a {@link ProgressTrackerGroup.Tracker ProgressTracker}
 * to track how far it's gotten through some base {@code Reiterator}. {@link
 * ProgressTrackingReiterator#copy} copies the {@code ProgressTracker}, allowing for an independent
 * progress state.
 *
 * @param <T> the type of the elements of this iterator
 */
public final class ProgressTrackingReiterator<T> extends ForwardingReiterator<T> {
  private ProgressTracker<T> tracker;

  public ProgressTrackingReiterator(Reiterator<T> base, ProgressTracker<T> tracker) {
    super(base);
    this.tracker = checkNotNull(tracker);
  }

  @Override
  public T next() {
    T result = super.next();
    tracker.saw(result);
    return result;
  }

  @Override
  protected ProgressTrackingReiterator<T> clone() {
    ProgressTrackingReiterator<T> result = (ProgressTrackingReiterator<T>) super.clone();
    result.tracker = tracker.copy();
    return result;
  }
}
