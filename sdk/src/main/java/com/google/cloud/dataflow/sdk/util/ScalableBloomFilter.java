/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Bloom filter implementation with an expected false positive probability of {@code 0.000001}
 * which grows dynamically with the number of insertions. For less than {@code 2^20} insertions
 * which would modify a Bloom filter, we brute force all the Bloom filter combinations in powers of
 * {@code 2} to only produce a scalable Bloom filter with one slice.
 *
 * <p>Otherwise, we use an implementation of
 * <a href="http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf">Scalable Bloom Filters</a>
 * by Paulo Sergio Almeida, Carlos Baquero, Nuno Preguica, David Hutchison. Our implementation
 * has an effective positive probability of {@code 0.000001}, given that we use a ratio of
 * {@code 0.9} and a scaling factor of {@code 2}.
 */
public class ScalableBloomFilter implements Serializable {
  /**
   * A {@link Coder} for scalable Bloom filters. The encoded format is:
   * <ul>
   *   <li>var int encoding of number of Bloom filter slices
   *   <li>N Bloom filter slices
   * </ul>
   *
   * <p>The encoded form of each Bloom filter slice is:
   * <ul>
   *   <li>1 signed byte for the strategy
   *   <li>1 unsigned byte for the number of hash functions
   *   <li>1 big endian int, the number of longs in our bitset
   *   <li>N big endian longs of our bitset
   * </ul>
   */
  public static class ScalableBloomFilterCoder extends AtomicCoder<ScalableBloomFilter> {
    private static final ScalableBloomFilterCoder INSTANCE = new ScalableBloomFilterCoder();

    @JsonCreator
    public static ScalableBloomFilterCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ScalableBloomFilter value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      VarInt.encode(value.bloomFilterSlices.size(), outStream);
      for (BloomFilter<ByteBuffer> bloomFilter : value.bloomFilterSlices) {
        bloomFilter.writeTo(outStream);
      }
    }

    @Override
    public ScalableBloomFilter decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      int numberOfBloomFilters = VarInt.decodeInt(inStream);
      List<BloomFilter<ByteBuffer>> bloomFilters = new ArrayList<>(numberOfBloomFilters);
      for (int i = 0; i < numberOfBloomFilters; ++i) {
        bloomFilters.add(BloomFilter.readFrom(inStream, ByteBufferFunnel.INSTANCE));
      }
      return new ScalableBloomFilter(bloomFilters);
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }
  }

  private final List<BloomFilter<ByteBuffer>> bloomFilterSlices;
  private ScalableBloomFilter(List<BloomFilter<ByteBuffer>> bloomFilters) {
    this.bloomFilterSlices = bloomFilters;
  }

  /**
   * Returns false if the Bloom filter definitely does not contain the byte
   * representation of an element contained in {@code buf} from {@code [offset, offset + length)}.
   */
  public boolean mightContain(byte[] buf, int offset, int length) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf, offset, length);
    return mightContain(byteBuffer);
  }

  /**
   * Returns false if the Bloom filter definitely does not contain the byte
   * representation of an element contained in {@code byteBuffer} from {@code [position, limit)}.
   */
  public boolean mightContain(ByteBuffer byteBuffer) {
    for (int i = bloomFilterSlices.size() - 1; i >= 0; i--) {
      if (bloomFilterSlices.get(i).mightContain(byteBuffer)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof ScalableBloomFilter)) {
      return false;
    }
    ScalableBloomFilter scalableBloomFilter = (ScalableBloomFilter) other;
    if (bloomFilterSlices.size() != scalableBloomFilter.bloomFilterSlices.size()) {
      return false;
    }
    for (int i = 0; i < bloomFilterSlices.size(); ++i) {
      if (!bloomFilterSlices.get(i).equals(scalableBloomFilter.bloomFilterSlices.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return bloomFilterSlices.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ScalableBloomFilter.class)
        .add("bloomFilterSlices", bloomFilterSlices)
        .toString();
  }

  @VisibleForTesting
  int numberOfBloomFilterSlices() {
    return bloomFilterSlices.size();
  }

  /**
   * Returns a scalable Bloom filter builder allowing one to construct a Bloom filter
   * with an expected false positive probability of {@code 0.000001} irrespective
   * of the number of elements inserted.
   */
  public static Builder builder() {
    return builder(Builder.MAX_INSERTIONS_FOR_ADD_TO_ALL_MODE_LOG_2);
  }

  @VisibleForTesting
  static Builder builder(int maxInsertionsForAddToAllModeLog2) {
    return new Builder(maxInsertionsForAddToAllModeLog2);
  }

  /**
   * A scalable Bloom filter builder which during the build process will attempt to
   * create a Bloom filter no larger than twice the required size for small Bloom filters.
   * For large Bloom filters, we create a list of Bloom filters which are successively twice as
   * large as the previous which we insert elements into.
   *
   * <p>This scalable Bloom filter builder uses 8mb of memory per instance to start when
   * fewer than {@code 2^20} elements have been inserted. Afterwards, it increases in space usage
   * by a factor of {@code 2.2} for every doubling in the number of unique insertions.
   */
  public static class Builder {
    private static final long MAX_ELEMENTS = 1L << 62;
    private static final int MAX_INSERTIONS_FOR_ADD_TO_ALL_MODE_LOG_2 = 20;
    private static final double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.000001;
    private static final double RATIO = 0.9;

    private enum Mode {
      ADD_TO_ALL, ADD_TO_LAST
    }

    private final List<BloomFilter<ByteBuffer>> bloomFilters;
    private Mode mode;
    private long numberOfInsertions;

    private Builder(int maxInsertionsForAddToAllModeLog2) {
      checkArgument(maxInsertionsForAddToAllModeLog2 < Long.SIZE - 1,
          "%s does not support an initial size with more than 2^63 elements.",
          ScalableBloomFilter.class.getSimpleName());
      this.bloomFilters = new ArrayList<>();
      this.mode = Mode.ADD_TO_ALL;
      // 1, 2, 4, 8, 16, 32, ...
      for (int i = 0; i <= maxInsertionsForAddToAllModeLog2; ++i) {
        bloomFilters.add(BloomFilter.<ByteBuffer>create(
            ByteBufferFunnel.INSTANCE,
            1 << i,
            DEFAULT_FALSE_POSITIVE_PROBABILITY));
      }
    }

    /**
     * Returns true if the Bloom filter was modified by inserting the byte
     * representation of an element contained in {@code buf} from {@code [offset, offset + length)}.
     */
    public boolean put(final byte[] buf, final int off, final int len) {
      ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
      return put(buffer);
    }

    /**
     * Returns true if the Bloom filter was modified by inserting the byte
     * representation of an element contained in {@code byteBuffer} from {@code [position, limit)}.
     */
    public boolean put(final ByteBuffer byteBuffer) {
      // Check to see if we gain any information by adding this element.
      switch (mode) {
        case ADD_TO_ALL:
          if (bloomFilters.get(bloomFilters.size() - 1).mightContain(byteBuffer)) {
            // We do not gain any information by adding this element
            return false;
          }
          break;
        case ADD_TO_LAST:
          for (int i = bloomFilters.size() - 1; i >= 0; i--) {
            if (bloomFilters.get(i).mightContain(byteBuffer)) {
              // One of the Bloom filters already considers that this element exists so skip
              // adding it.
              return false;
            }
          }
          break;
        default:
          throw new IllegalStateException("Unknown builder mode: " + mode);
      }

      // We now need to add the element to the appropriate Bloom filter(s) depending on the mode.
      switch (mode) {
        case ADD_TO_ALL:
          int bloomFilterToStartWith =
              Long.SIZE - Long.numberOfLeadingZeros(numberOfInsertions);
          // If we were to attempt to add to a non-existent Bloom filter, we need to
          // swap to the other mode.
          if (bloomFilterToStartWith == bloomFilters.size()) {
            BloomFilter<ByteBuffer> last = bloomFilters.get(bloomFilters.size() - 1);
            bloomFilters.clear();
            bloomFilters.add(last);
            mode = Mode.ADD_TO_LAST;
            addToLast(byteBuffer);
          } else {
            for (int i = bloomFilterToStartWith; i < bloomFilters.size(); ++i) {
              bloomFilters.get(i).put(byteBuffer);
            }
          }
          break;
        case ADD_TO_LAST:
          addToLast(byteBuffer);
          break;
        default:
          throw new IllegalStateException("Unknown builder mode: " + mode);
      }
      numberOfInsertions += 1;
      return true;
    }

    /**
     * Returns a scalable Bloom filter with the elements that were added.
     */
    public ScalableBloomFilter build() {
      switch (mode) {
        case ADD_TO_ALL:
          int bloomFilterToUse = Long.SIZE - Long.numberOfLeadingZeros(numberOfInsertions);
          if (Long.bitCount(numberOfInsertions) == 1) {
            bloomFilterToUse -= 1;
          }
          return new ScalableBloomFilter(Arrays.asList(bloomFilters.get(bloomFilterToUse)));
        case ADD_TO_LAST:
          return new ScalableBloomFilter(bloomFilters);
        default:
          throw new IllegalStateException("Unknown builder mode: " + mode);
      }
    }

    private void addToLast(ByteBuffer byteBuffer) {
      // If we are a power of 2, we have hit the number of expected insertions
      // for the last Bloom filter and we have to add a new one.
      if (Long.bitCount(numberOfInsertions) == 1) {
        checkArgument(numberOfInsertions <= MAX_ELEMENTS,
            "%s does not support Bloom filter slices with more than 2^63 elements.",
            ScalableBloomFilter.class);
        bloomFilters.add(BloomFilter.<ByteBuffer>create(
            ByteBufferFunnel.INSTANCE,
            numberOfInsertions,
            DEFAULT_FALSE_POSITIVE_PROBABILITY * Math.pow(RATIO, bloomFilters.size())));
      }
      BloomFilter<ByteBuffer> last = bloomFilters.get(bloomFilters.size() - 1);
      last.put(byteBuffer);
    }
  }

  /**
   * Writes {@link ByteBuffer}s to {@link PrimitiveSink}s and meant to be used
   * with Guava's {@link BloomFilter} API. This {@link Funnel} does not modify the
   * underlying byte buffer and assumes that {@code ByteBuffer#array} returns the backing data.
   */
  private static class ByteBufferFunnel implements Funnel<ByteBuffer> {
    private static final ByteBufferFunnel INSTANCE = new ByteBufferFunnel();
    @Override
    public void funnel(ByteBuffer from, PrimitiveSink into) {
      into.putBytes(from.array(), from.position(), from.remaining());
    }
  }
}

