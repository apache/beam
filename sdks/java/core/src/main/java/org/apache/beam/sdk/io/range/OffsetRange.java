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
package org.apache.beam.sdk.io.range;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A restriction represented by a range of integers [from, to). */
public class OffsetRange
    implements Serializable,
        HasDefaultTracker<
            OffsetRange, org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker> {
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
  public org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker newTracker() {
    return new org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker(this);
  }

  @Override
  public String toString() {
    return "[" + from + ", " + to + ')';
  }

  @Override
  public boolean equals(@Nullable Object o) {
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

  public List<OffsetRange> split(long desiredNumOffsetsPerSplit, long minNumOffsetPerSplit) {
    List<OffsetRange> res = new ArrayList<>();
    long start = getFrom();
    long maxEnd = getTo();

    while (start < maxEnd) {
      long end = start + desiredNumOffsetsPerSplit;
      end = Math.min(end, maxEnd);
      // Avoid having a too small range at the end and ensure that we respect minNumOffsetPerSplit.
      long remaining = maxEnd - end;
      if ((remaining < desiredNumOffsetsPerSplit / 4) || (remaining < minNumOffsetPerSplit)) {
        end = maxEnd;
      }
      res.add(new OffsetRange(start, end));
      start = end;
    }
    return res;
  }

  /** A coder for {@link OffsetRange}s. */
  public static class Coder extends AtomicCoder<OffsetRange> {
    private static final Coder INSTANCE = new Coder();
    private static final TypeDescriptor<OffsetRange> TYPE_DESCRIPTOR =
        new TypeDescriptor<OffsetRange>() {};

    public static Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(OffsetRange value, OutputStream outStream)
        throws CoderException, IOException {
      VarInt.encode(value.from, outStream);
      VarInt.encode(value.to, outStream);
    }

    @Override
    public OffsetRange decode(InputStream inStream) throws CoderException, IOException {
      return new OffsetRange(VarInt.decodeLong(inStream), VarInt.decodeLong(inStream));
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(OffsetRange value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(OffsetRange value) throws Exception {
      return (long) VarInt.getLength(value.from) + VarInt.getLength(value.to);
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public TypeDescriptor<OffsetRange> getEncodedTypeDescriptor() {
      return TYPE_DESCRIPTOR;
    }
  }
}
