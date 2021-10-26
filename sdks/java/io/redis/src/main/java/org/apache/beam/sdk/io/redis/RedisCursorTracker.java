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
package org.apache.beam.sdk.io.redis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RedisCursorTracker extends RestrictionTracker<RedisCursorRange, RedisCursor>
    implements RestrictionTracker.HasProgress {

  private RedisCursorRange range;

  private @Nullable RedisCursor lastAttemptedKey;
  private @Nullable RedisCursor lastClaimedKey;

  private RedisCursorTracker(RedisCursorRange range) {
    this.range = range;
  }

  public static RedisCursorTracker of(RedisCursorRange range) {
    return new RedisCursorTracker(range);
  }

  @Override
  public boolean tryClaim(RedisCursor position) {
    if (!position.getCursor().equals(RedisCursor.END_CURSOR)) {
      checkArgument(
          position.compareTo(range.getEndPosition()) >= 0,
          "Trying to claim cursor %s[%s] while range end was %s[%s]",
          position.getCursor(),
          position.getIndex(),
          range.getEndPosition().getCursor(),
          range.getEndPosition().getIndex());
    }
    range = range.fromEndPosition(position);
    return true;
  }

  @Override
  public RedisCursorRange currentRestriction() {
    return range;
  }

  @Override
  public SplitResult<RedisCursorRange> trySplit(double fractionOfRemainder) {
    /*if (fractionOfRemainder != 0.0 || range.getEndPosition().getCursor().equals(range.getStartPosition().getCursor())) {
      return null;
    }
    return SplitResult.of(
        RedisCursorRange.of(range.getStartPosition(), range.getStartPosition()),
        range);*/
    return null;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    if (range.getStartPosition().compareTo(range.getEndPosition()) >= 0) {
      return;
    }
    if (range.getStartPosition().getCursor().equals(RedisCursor.START_CURSOR)
        && range.getStartPosition().getCursor().equals(RedisCursor.END_CURSOR)) {
      return;
    }
    throw new IllegalStateException("not done iterating cursors");
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @Override
  public Progress getProgress() {
    return null;
  }
}
