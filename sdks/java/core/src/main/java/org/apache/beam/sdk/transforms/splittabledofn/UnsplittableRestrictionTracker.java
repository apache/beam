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
package org.apache.beam.sdk.transforms.splittabledofn;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link RestrictionTracker} for wrapping a {@link RestrictionTracker} with unsplittable
 * restrictions.
 *
 * <p>A restriction is considered unsplittable when restrictions of an element must not be processed
 * simultaneously (e.g., Kafka topic partition).
 */
public class UnsplittableRestrictionTracker<RestrictionT, PositionT>
    extends RestrictionTracker<RestrictionT, PositionT> implements RestrictionTracker.HasProgress {
  private final RestrictionTracker<RestrictionT, PositionT> tracker;

  public UnsplittableRestrictionTracker(RestrictionTracker<RestrictionT, PositionT> tracker) {
    this.tracker = tracker;
  }

  @Override
  public boolean tryClaim(PositionT position) {
    return tracker.tryClaim(position);
  }

  @Override
  public RestrictionT currentRestriction() {
    return tracker.currentRestriction();
  }

  @Override
  public @Nullable SplitResult<RestrictionT> trySplit(double fractionOfRemainder) {
    return fractionOfRemainder > 0.0 && fractionOfRemainder < 1.0
        ? null
        : tracker.trySplit(fractionOfRemainder);
  }

  @Override
  public void checkDone() throws IllegalStateException {
    tracker.checkDone();
  }

  @Override
  public IsBounded isBounded() {
    return tracker.isBounded();
  }

  @Override
  public Progress getProgress() {
    return tracker instanceof RestrictionTracker.HasProgress
        ? ((RestrictionTracker.HasProgress) tracker).getProgress()
        : Progress.NONE;
  }
}
