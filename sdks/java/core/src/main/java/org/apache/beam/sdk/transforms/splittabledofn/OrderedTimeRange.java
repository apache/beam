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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** A restriction represented by a range of Instants [from, to). */
public class OrderedTimeRange
    implements Serializable, HasDefaultTracker<OrderedTimeRange, OrderedTimeRangeTracker> {
  private final Instant from;
  private final Instant to;

  public OrderedTimeRange(Instant from, Instant to) {
    checkArgument(from.isBefore(to) || from.isEqual(to), "Malformed range [%s, %s)", from, to);
    this.from = from;
    this.to = to;
  }

  public OrderedTimeRange(long from, long to) {
    checkArgument(from <= to, "Malformed range [%s, %s)", from, to);
    this.from = Instant.ofEpochMilli(from);
    this.to = Instant.ofEpochMilli(to);
  }

  public Instant getFrom() {
    return from;
  }

  public Instant getTo() {
    return to;
  }

  @Override
  public OrderedTimeRangeTracker newTracker() {
    return new OrderedTimeRangeTracker(this);
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
    OrderedTimeRange that = (OrderedTimeRange) o;
    return Objects.equal(getFrom(), that.getFrom()) && Objects.equal(getTo(), that.getTo());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFrom(), getTo());
  }

  public List<OrderedTimeRange> split(
      Duration desiredDurationPerSplit, Duration minDurationPerSplit) {
    List<OrderedTimeRange> res = new ArrayList<>();
    Instant start = getFrom();
    Instant maxEnd = getTo();

    while (start.isBefore(maxEnd)) {
      Instant end = start.plus(desiredDurationPerSplit);
      end = end.isBefore(maxEnd) ? end : maxEnd;
      // Avoid having a too small range at the end and ensure that we respect minNumOffsetPerSplit.
      Duration remaining = new Duration(end, maxEnd);
      if ((remaining.getMillis() < desiredDurationPerSplit.getMillis() / 4)
          || (remaining.isShorterThan(minDurationPerSplit))) {
        end = maxEnd;
      }
      res.add(new OrderedTimeRange(start, end));
      start = end;
    }
    return res;
  }

  /** A coder for {@link OrderedTimeRange}s. */
  public static class Coder extends AtomicCoder<OrderedTimeRange> {
    private static final Coder INSTANCE = new Coder();
    private static final TypeDescriptor<OrderedTimeRange> TYPE_DESCRIPTOR =
        new TypeDescriptor<OrderedTimeRange>() {};
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();

    public static Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(OrderedTimeRange value, OutputStream outStream)
        throws CoderException, IOException {
      INSTANT_CODER.encode(value.from, outStream);
      INSTANT_CODER.encode(value.to, outStream);
    }

    @Override
    public OrderedTimeRange decode(InputStream inStream) throws CoderException, IOException {
      return new OrderedTimeRange(INSTANT_CODER.decode(inStream), INSTANT_CODER.decode(inStream));
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(OrderedTimeRange value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(OrderedTimeRange value) throws Exception {
      return (long) VarInt.getLength(value.from.getMillis())
          + VarInt.getLength(value.to.getMillis());
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public TypeDescriptor<OrderedTimeRange> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  }
}
