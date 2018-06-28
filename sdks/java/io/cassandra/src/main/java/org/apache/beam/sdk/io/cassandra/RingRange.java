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
package org.apache.beam.sdk.io.cassandra;

import java.math.BigInteger;

/** Models a Cassandra token range. */
final class RingRange {
  private final BigInteger start;
  private final BigInteger end;

  RingRange(BigInteger start, BigInteger end) {
    this.start = start;
    this.end = end;
  }

  BigInteger getStart() {
    return start;
  }

  BigInteger getEnd() {
    return end;
  }

  /**
   * Returns the size of this range.
   *
   * @return size of the range, max - range, in case of wrap
   */
  BigInteger span(BigInteger ringSize) {
    return (start.compareTo(end) >= 0) ? end.subtract(start).add(ringSize) : end.subtract(start);
  }

  /** @return true if 0 is inside of this range. Note that if start == end, then wrapping is true */
  public boolean isWrapping() {
    return start.compareTo(end) >= 0;
  }

  @Override
  public String toString() {
    return String.format("(%s,%s]", start.toString(), end.toString());
  }
}
