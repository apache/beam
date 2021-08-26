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
package org.apache.beam.runners.dataflow.worker.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.BloomFilter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.PrimitiveSink;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.DoubleMath;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.LongMath;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Bloom filter implementation that maintains an expected false probability of {@code 0.000001}
 * while growing with the number of inserations up to a specified size or insertion limit. Once the
 * limit is reached, the expected false positive probability can no longer be honored. This limit
 * prevents the Bloom filter from growing too large when there are many insertions. The Bloom filter
 * that is built is the smaller of twice the number of insertions or the user specified limit.
 */
public class ScalableBloomFilter implements Serializable {
  /**
   * A {@link Coder} for scalable Bloom filters. The encoded format is (see {@link
   * BloomFilter#writeTo(OutputStream)}):
   *
   * <ul>
   *   <li>1 signed byte for the strategy
   *   <li>1 unsigned byte for the number of hash functions
   *   <li>1 big endian int, the number of longs in our bitset
   *   <li>N big endian longs of our bitset
   * </ul>
   */
  public static class ScalableBloomFilterCoder extends AtomicCoder<ScalableBloomFilter> {
    private static final ScalableBloomFilterCoder INSTANCE = new ScalableBloomFilterCoder();

    public static ScalableBloomFilterCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ScalableBloomFilter value, OutputStream outStream)
        throws CoderException, IOException {
      value.bloomFilter.writeTo(outStream);
    }

    @Override
    public ScalableBloomFilter decode(InputStream inStream) throws CoderException, IOException {
      return new ScalableBloomFilter(BloomFilter.readFrom(inStream, ByteBufferFunnel.INSTANCE));
    }

    @Override
    public void verifyDeterministic() {}

    @Override
    public boolean consistentWithEquals() {
      return true;
    }
  }

  private static final long MAX_ELEMENTS = 1L << 62;
  private static final double DEFAULT_FALSE_POSITIVE_PROBABILITY = 0.000001;

  private final BloomFilter<ByteBuffer> bloomFilter;

  private ScalableBloomFilter(BloomFilter<ByteBuffer> bloomFilter) {
    this.bloomFilter = bloomFilter;
  }

  /**
   * Returns false if the Bloom filter definitely does not contain the byte representation of an
   * element contained in {@code buf} from {@code [offset, offset + length)}.
   */
  public boolean mightContain(byte[] buf, int offset, int length) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buf, offset, length);
    return mightContain(byteBuffer);
  }

  /**
   * Returns false if the Bloom filter definitely does not contain the byte representation of an
   * element contained in {@code byteBuffer} from {@code [position, limit]}.
   */
  public boolean mightContain(ByteBuffer byteBuffer) {
    return bloomFilter.mightContain(byteBuffer);
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof ScalableBloomFilter)) {
      return false;
    }
    ScalableBloomFilter scalableBloomFilter = (ScalableBloomFilter) other;
    return Objects.equals(bloomFilter, scalableBloomFilter.bloomFilter);
  }

  @Override
  public int hashCode() {
    return bloomFilter.hashCode();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(ScalableBloomFilter.class)
        .add("bloomFilter", bloomFilter)
        .toString();
  }

  /**
   * Returns a scalable Bloom filter builder allowing one to construct a Bloom filter with the
   * specified size limit. Each insertion may grow the Bloom filter up to the specified size limit.
   * While within the size limit, the expected false positive probability is {@code 0.000001}. Once
   * the size limit is surpassed, the Bloom filter will no longer honor the expected false positive
   * probability of {@code 0.000001}.
   */
  public static Builder withMaximumSizeBytes(long maxBloomFilterSizeBytes) {
    checkArgument(maxBloomFilterSizeBytes > 0, "Expected Bloom filter size limit to be positive.");
    long optimalNumberOfElements =
        optimalNumInsertions(maxBloomFilterSizeBytes, DEFAULT_FALSE_POSITIVE_PROBABILITY);
    checkArgument(
        optimalNumberOfElements <= MAX_ELEMENTS,
        "The specified size limit would attempt to create a Bloom filter builder larger than "
            + "the maximum supported size of 2^63.");

    return withMaximumNumberOfInsertionsForOptimalBloomFilter(optimalNumberOfElements);
  }

  /**
   * Returns a scalable Bloom filter builder allowing one to construct a Bloom filter that will
   * maintain the expected false positive probability of {@code 0.000001} as long as there have been
   * fewer insertions then the specified limit. Once the limit is surpassed, the Bloom filter will
   * no longer honor the expected false positive probability of {@code 0.000001}.
   */
  public static Builder withMaximumNumberOfInsertionsForOptimalBloomFilter(
      long maximumBloomFilterInsertionsForOptimalBloomFilter) {
    checkArgument(
        maximumBloomFilterInsertionsForOptimalBloomFilter > 0,
        "Expected Bloom filter insertion limit to be positive.");
    checkArgument(
        maximumBloomFilterInsertionsForOptimalBloomFilter <= MAX_ELEMENTS,
        "The specified size is larger than the maximum supported size of 2^63 elements.");

    return new Builder(maximumBloomFilterInsertionsForOptimalBloomFilter);
  }

  /** Calculate the number of optimal insertions based upon the number of bytes. */
  private static long optimalNumInsertions(long bytes, double p) {
    checkArgument(p > 0, "Expected false positive probability to be positive.");
    return LongMath.checkedMultiply(
        8,
        DoubleMath.roundToLong(
            -bytes * Math.log(2) * Math.log(2) / Math.log(p), RoundingMode.DOWN));
  }

  /**
   * A scalable Bloom filter builder which uses approximately double the amount of memory for the
   * specified optimal number of insertions. The returned Bloom filter will scale with the number of
   * insertions honoring the expected false positive probability rate of {@code 0.000001} until the
   * limit is reached. The Bloom filter that is built is the smaller of twice the number of
   * insertions or the user specified limit.
   */
  public static class Builder {
    private final List<BloomFilter<ByteBuffer>> bloomFilters;
    private long numberOfInsertions;

    private Builder(long maximumBloomFilterInsertions) {
      this.bloomFilters = new ArrayList<>();

      // 1, 2, 4, 8, 16, 32, ...
      for (long i = 1; i < maximumBloomFilterInsertions; i = i << 1) {
        bloomFilters.add(
            BloomFilter.<ByteBuffer>create(
                ByteBufferFunnel.INSTANCE, i, DEFAULT_FALSE_POSITIVE_PROBABILITY));
      }

      // Add the largest Bloom filter we will scale to based off of what the user requested.
      bloomFilters.add(
          BloomFilter.<ByteBuffer>create(
              ByteBufferFunnel.INSTANCE,
              maximumBloomFilterInsertions,
              DEFAULT_FALSE_POSITIVE_PROBABILITY));
    }

    /**
     * Returns true if the Bloom filter was modified by inserting the byte representation of an
     * element contained in {@code buf} from {@code [offset, offset + length)}.
     */
    public boolean put(final byte[] buf, final int off, final int len) {
      ByteBuffer buffer = ByteBuffer.wrap(buf, off, len);
      return put(buffer);
    }

    /**
     * Returns true if the Bloom filter was modified by inserting the byte representation of an
     * element contained in {@code byteBuffer} from {@code [position, limit)}.
     */
    public boolean put(final ByteBuffer byteBuffer) {
      if (bloomFilters.get(bloomFilters.size() - 1).mightContain(byteBuffer)) {
        // We do not gain any information by adding this element
        return false;
      }

      int bloomFilterToStartWith =
          Math.min(
              Long.SIZE - Long.numberOfLeadingZeros(numberOfInsertions), bloomFilters.size() - 1);
      for (int i = bloomFilterToStartWith; i < bloomFilters.size(); ++i) {
        bloomFilters.get(i).put(byteBuffer);
      }
      numberOfInsertions += 1;
      return true;
    }

    /** Returns a scalable Bloom filter with the elements that were added. */
    public ScalableBloomFilter build() {
      int bloomFilterToUse = Long.SIZE - Long.numberOfLeadingZeros(numberOfInsertions);
      if (Long.bitCount(numberOfInsertions) == 1) {
        bloomFilterToUse -= 1;
      }
      bloomFilterToUse = Math.min(bloomFilterToUse, bloomFilters.size() - 1);
      return new ScalableBloomFilter(bloomFilters.get(bloomFilterToUse));
    }
  }

  /**
   * Writes {@link ByteBuffer}s to {@link PrimitiveSink}s and meant to be used with Guava's {@link
   * BloomFilter} API. This {@link Funnel} does not modify the underlying byte buffer and assumes
   * that {@code ByteBuffer#array} returns the backing data.
   */
  private static class ByteBufferFunnel implements Funnel<ByteBuffer> {
    private static final ByteBufferFunnel INSTANCE = new ByteBufferFunnel();

    @Override
    @SuppressWarnings("ByteBufferBackingArray")
    public void funnel(ByteBuffer from, PrimitiveSink into) {
      into.putBytes(from.array(), from.position(), from.remaining());
    }
  }
}
