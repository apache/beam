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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

/**
 * A {@link ReadableState} cell containing a list of values sorted by timestamp. Timestamped values
 * can be added to the list, and subranges can be read out in order. Subranges of the list can be
 * deleted as well.
 */
@Experimental(Kind.STATE)
public interface OrderedListState<T>
    extends GroupingState<TimestampedValue<T>, Iterable<TimestampedValue<T>>> {
  /**
   * Read a timestamp-limited subrange of the list. The result is ordered by timestamp.
   *
   * <p>All values with timestamps >= minTimestamp and < limitTimestamp will be in the resuling
   * iterable. This means that only timestamps strictly less than
   * Instant.ofEpochMilli(Long.MAX_VALUE) can be used as timestamps.
   */
  Iterable<TimestampedValue<T>> readRange(Instant minTimestamp, Instant limitTimestamp);

  /**
   * Clear a timestamp-limited subrange of the list.
   *
   * <p>All values with timestamps >= minTimestamp and < limitTimestamp will be removed from the
   * list.
   */
  void clearRange(Instant minTimestamp, Instant limitTimestamp);

  /**
   * Call to indicate that a specific range will be read from the list, allowing runners to batch
   * multiple range reads.
   */
  OrderedListState<T> readRangeLater(Instant minTimestamp, Instant limitTimestamp);
}
