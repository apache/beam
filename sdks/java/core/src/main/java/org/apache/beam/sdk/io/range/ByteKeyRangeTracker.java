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
package com.google.cloud.dataflow.sdk.io.range;

import static com.google.common.base.MoreObjects.toStringHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * A {@link RangeTracker} for {@link ByteKey ByteKeys} in {@link ByteKeyRange ByteKeyRanges}.
 *
 * @see ByteKey
 * @see ByteKeyRange
 */
public final class ByteKeyRangeTracker implements RangeTracker<ByteKey> {
  private static final Logger logger = LoggerFactory.getLogger(ByteKeyRangeTracker.class);

  /** Instantiates a new {@link ByteKeyRangeTracker} with the specified range. */
  public static ByteKeyRangeTracker of(ByteKeyRange range) {
    return new ByteKeyRangeTracker(range);
  }

  @Override
  public synchronized ByteKey getStartPosition() {
    return range.getStartKey();
  }

  @Override
  public synchronized ByteKey getStopPosition() {
    return range.getEndKey();
  }

  @Override
  public synchronized boolean tryReturnRecordAt(boolean isAtSplitPoint, ByteKey recordStart) {
    if (isAtSplitPoint && !range.containsKey(recordStart)) {
      return false;
    }
    position = recordStart;
    return true;
  }

  @Override
  public synchronized boolean trySplitAtPosition(ByteKey splitPosition) {
    // Unstarted.
    if (position == null) {
      logger.warn(
          "{}: Rejecting split request at {} because no records have been returned.",
          this,
          splitPosition);
      return false;
    }

    // Started, but not after current position.
    if (splitPosition.compareTo(position) <= 0) {
      logger.warn(
          "{}: Rejecting split request at {} because it is not after current position {}.",
          this,
          splitPosition,
          position);
      return false;
    }

    // Sanity check.
    if (!range.containsKey(splitPosition)) {
      logger.warn(
          "{}: Rejecting split request at {} because it is not within the range.",
          this,
          splitPosition);
      return false;
    }

    range = range.withEndKey(splitPosition);
    return true;
  }

  @Override
  public synchronized double getFractionConsumed() {
    if (position == null) {
      return 0;
    }
    return range.estimateFractionForKey(position);
  }

  ///////////////////////////////////////////////////////////////////////////////
  private ByteKeyRange range;
  @Nullable private ByteKey position;

  private ByteKeyRangeTracker(ByteKeyRange range) {
    this.range = range;
    this.position = null;
  }

  @Override
  public String toString() {
    return toStringHelper(ByteKeyRangeTracker.class)
        .add("range", range)
        .add("position", position)
        .toString();
  }
}
