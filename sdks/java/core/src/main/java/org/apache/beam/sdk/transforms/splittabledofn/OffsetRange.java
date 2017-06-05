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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;

/** A restriction represented by a range of integers [from, to). */
public class OffsetRange
    implements Serializable, HasDefaultTracker<OffsetRange, OffsetRangeTracker> {
  private final long from;
  private final long to;

  public OffsetRange(long from, long to) {
    checkArgument(from <= to, "Malformed range [%s, %s)", from, to);
    this.from = from;
    this.to = to;
  }

  public long getFrom() {
    return from;
  }

  public long getTo() {
    return to;
  }

  @Override
  public OffsetRangeTracker newTracker() {
    return new OffsetRangeTracker(this);
  }

  @Override
  public String toString() {
    return "[" + from + ", " + to + ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OffsetRange that = (OffsetRange) o;

    if (from != that.from) {
      return false;
    }
    return to == that.to;
  }

  @Override
  public int hashCode() {
    int result = (int) (from ^ (from >>> 32));
    result = 31 * result + (int) (to ^ (to >>> 32));
    return result;
  }
}
