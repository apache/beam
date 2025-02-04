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
package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;

/** A range of contiguous event sequences and the latest timestamp of the events in the range. */
@AutoValue
public abstract class ContiguousSequenceRange
    implements Serializable, Comparable<ContiguousSequenceRange> {

  public static final ContiguousSequenceRange EMPTY =
      ContiguousSequenceRange.of(
          Long.MIN_VALUE, Long.MIN_VALUE, Instant.ofEpochMilli(Long.MIN_VALUE));

  /** @return inclusive starting sequence */
  public abstract long getStart();

  /** @return exclusive end sequence */
  public abstract long getEnd();

  /** @return latest timestamp of all events in the range */
  public abstract Instant getTimestamp();

  @Override
  public int compareTo(ContiguousSequenceRange other) {
    if (other == null) {
      throw new IllegalArgumentException("Can't compare " + this + " with NULL");
    }

    int startCompare = Long.compare(this.getStart(), other.getStart());
    return startCompare == 0 ? Long.compare(this.getEnd(), other.getEnd()) : startCompare;
  }

  public static ContiguousSequenceRange largestRange(
      Iterable<ContiguousSequenceRange> rangeIterable) {
    ContiguousSequenceRange result = EMPTY;

    Iterator<ContiguousSequenceRange> iterator = rangeIterable.iterator();
    while (iterator.hasNext()) {
      ContiguousSequenceRange next = iterator.next();
      if (next.compareTo(result) > 0) {
        result = next;
      }
    }
    return result;
  }

  public static ContiguousSequenceRange of(long start, long end, Instant timestamp) {
    return new AutoValue_ContiguousSequenceRange(start, end, timestamp);
  }

  static class ContiguousSequenceRangeCoder extends CustomCoder<ContiguousSequenceRange> {

    private static final ContiguousSequenceRangeCoder INSTANCE = new ContiguousSequenceRangeCoder();

    static ContiguousSequenceRangeCoder of() {
      return INSTANCE;
    }

    private ContiguousSequenceRangeCoder() {}

    @Override
    public void encode(
        ContiguousSequenceRange value, @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      VarLongCoder.of().encode(value.getStart(), outStream);
      VarLongCoder.of().encode(value.getEnd(), outStream);
      InstantCoder.of().encode(value.getTimestamp(), outStream);
    }

    @Override
    public ContiguousSequenceRange decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws @UnknownKeyFor @NonNull @Initialized CoderException, @UnknownKeyFor @NonNull
            @Initialized IOException {
      long start = VarLongCoder.of().decode(inStream);
      long end = VarLongCoder.of().decode(inStream);
      Instant timestamp = InstantCoder.of().decode(inStream);
      return ContiguousSequenceRange.of(start, end, timestamp);
    }
  }
}
