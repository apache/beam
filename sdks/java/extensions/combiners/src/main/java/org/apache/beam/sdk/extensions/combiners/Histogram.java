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
package org.apache.beam.sdk.extensions.combiners;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.apache.commons.lang3.ArrayUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A histogram transform with a combiner that efficiently constructs linear, exponential or explicit
 * histograms from large datasets of input data. Bucket bounds can be specified using the {@link
 * BucketBounds} class.
 */
public class Histogram {

  private Histogram() {
    // do not instantiate
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<T>} and returns a {@code
   * PCollection<List<Long>>} with a single element per window. The values of this list represent
   * the number of elements within each bucket of a histogram, as defined by {@link BucketBounds}.
   * The first and the last elements of the list are numbers of elements in underflow and overflow
   * buckets.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Double> pc = ...;
   * PCollection<List<Long>> bucketCounts =
   *     pc.apply(Histogram.globally(BucketBounds.linear(1.0, 2.0, 100)));
   *
   * }</pre>
   *
   * @param <T> the type of the elements in the input {@code PCollection}
   * @param bucketBounds the instance of the {@link BucketBounds} class with desired parameters of
   *     the histogram.
   */
  public static <T extends Number> Combine.Globally<T, List<Long>> globally(
      BucketBounds bucketBounds) {
    return Combine.globally(HistogramCombineFn.create(bucketBounds));
  }

  /**
   * Returns a {@code PTransform} that takes a {@code PCollection<KV<K, V>>} and returns a {@code
   * PCollection<KV<K, List<Long>>>} that contains an output element mapping each distinct key in
   * the input {@code PCollection} to a {@code List}. The values of this list represent the number
   * of elements within each bucket of a histogram, as defined by {@link BucketBounds}. The first
   * and the last elements of the list are numbers of elements in underflow and overflow buckets.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Integer>> pc = ...;
   * PCollection<KV<String, List<Long>>> bucketCounts =
   *     pc.apply(Histogram.perKey(BucketBounds.linear(1.0, 2.0, 100)));
   *
   * }</pre>
   *
   * @param <K> the type of the keys in the input and output {@code PCollection}s
   * @param <V> the type of the values in the input {@code PCollection}
   * @param bucketBounds the instance of the {@link BucketBounds} class with desired parameters of
   *     the histogram.
   */
  public static <K, V extends Number> Combine.PerKey<K, V, List<Long>> perKey(
      BucketBounds bucketBounds) {
    return Combine.perKey(HistogramCombineFn.create(bucketBounds));
  }

  /**
   * Defines the bounds for histogram buckets.
   *
   * <p>Use the provided static factory methods to create new instances of {@link BucketBounds}.
   */
  @AutoValue
  public abstract static class BucketBounds {

    // Package-private because users should use static factory methods to instantiate new instances.
    BucketBounds() {}

    public abstract List<Double> getBounds();

    public abstract BoundsInclusivity getBoundsInclusivity();

    /**
     * Static factory method for defining bounds of exponential histograms and calculating bounds
     * based on the parameters.
     *
     * <p>For BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE, the list that the
     * HistogramCombineFn combiner returns contains the number of elements in the following buckets:
     *
     * <pre>
     * 0-th: (-inf, scale) - underflow bucket
     * 1-st: [scale, scale * growthFactor)
     * 2-nd: [scale * growthFactor, scale * growthFactor^2)
     * ...
     * i-th: [scale * growthFactor^(i-1), scale * growthFactor^i)
     * ...
     * numBoundedBuckets: [scale * growthFactor^(numBoundedBuckets-1), scale *
     * growthFactor^numBoundedBuckets)
     * numBoundedBuckets + 1: [scale * growthFactor^numBoundedBuckets), +inf) - overflow bucket.
     * </pre>
     *
     * <p>For BoundsInclusivity.LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE, the list that the
     * HistogramCombineFn combiner returns contains the number of elements in the following buckets:
     *
     * <pre>
     * 0-th: (-inf, scale] - underflow bucket
     * 1-st: (scale, scale * growthFactor]
     * 2-nd: (scale * growthFactor, scale * growthFactor^2]
     * ...
     * i-th: (scale * growthFactor^(i-1), scale * growthFactor^i]
     * ...
     * numBoundedBuckets: (scale * growthFactor^(numBoundedBuckets-1), scale *
     * growthFactor^numBoundedBuckets]
     * numBoundedBuckets + 1: (scale * growthFactor^numBoundedBuckets), +inf) - overflow bucket.
     * </pre>
     *
     * @param scale the value of the lower bound for the first bounded bucket.
     * @param growthFactor value by which the bucket bounds are exponentially increased.
     * @param numBoundedBuckets integer determining the total number of bounded buckets within the
     *     histogram.
     * @param boundsInclusivity enum value which defines if lower or upper bounds are
     *     inclusive/exclusive.
     */
    public static BucketBounds exponential(
        double scale,
        double growthFactor,
        int numBoundedBuckets,
        BoundsInclusivity boundsInclusivity) {
      checkArgument(scale > 0.0, "scale should be positive.");
      checkArgument(growthFactor > 1.0, "growth factor should be greater than 1.0.");
      checkArgument(
          numBoundedBuckets > 0, "number of bounded buckets should be greater than zero.");
      checkArgument(
          numBoundedBuckets <= Integer.MAX_VALUE - 2,
          "number of bounded buckets should be less than max value of integer.");

      ImmutableList.Builder<Double> boundsCalculated = new ImmutableList.Builder<>();
      // The number of bounds is equal to the numBoundedBuckets + 1.
      for (int i = 0; i <= numBoundedBuckets; i++) {
        double bound = scale * Math.pow(growthFactor, i);
        if (Double.isInfinite(bound)) {
          throw new IllegalArgumentException("the bound has overflown double type.");
        }
        boundsCalculated.add(bound);
      }

      return new AutoValue_Histogram_BucketBounds(boundsCalculated.build(), boundsInclusivity);
    }

    /**
     * Like {@link #exponential(double, double, int, BoundsInclusivity)}, but sets
     * BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE value for the boundsInclusivity
     * parameter.
     */
    public static BucketBounds exponential(
        double scale, double growthFactor, int numBoundedBuckets) {
      return exponential(
          scale,
          growthFactor,
          numBoundedBuckets,
          BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE);
    }

    /**
     * Static factory method for defining bounds of linear histogram and calculating bounds based on
     * the parameters.
     *
     * @param offset value of the lower bound for the first bounded bucket.
     * @param width bucket width.
     * @param numBoundedBuckets integer determining the total number of bounded buckets within the
     *     histogram.
     * @param boundsInclusivity enum value which defines if lower or upper bounds are
     *     inclusive/exclusive.
     */
    public static BucketBounds linear(
        double offset, double width, int numBoundedBuckets, BoundsInclusivity boundsInclusivity) {
      checkArgument(width > 0.0, "width of buckets should be positive.");
      checkArgument(numBoundedBuckets > 0, "number of bounded buckets should be more than zero.");
      checkArgument(
          numBoundedBuckets <= Integer.MAX_VALUE - 2,
          "number of bounded buckets should be less than max value of integer.");

      ImmutableList.Builder<Double> boundsCalculated = new ImmutableList.Builder<>();
      // The number of bounds is equal to the numBoundedBuckets + 1.
      for (int i = 0; i <= numBoundedBuckets; i++) {
        double bound = offset + i * width;
        if (Double.isInfinite(bound)) {
          throw new IllegalArgumentException("the bound has overflown double type.");
        }
        boundsCalculated.add(bound);
      }

      return new AutoValue_Histogram_BucketBounds(boundsCalculated.build(), boundsInclusivity);
    }

    /**
     * Like {@link #linear(double, double, int, BoundsInclusivity)}, but sets
     * BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE value for the boundsInclusivity
     * parameter.
     */
    public static BucketBounds linear(double offset, double width, int numBoundedBuckets) {
      return linear(
          offset,
          width,
          numBoundedBuckets,
          BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE);
    }

    /**
     * Static factory method for defining bounds of explicit histogram.
     *
     * @param bounds array of explicit bounds of the buckets.
     * @param boundsInclusivity enum value which defines if lower or upper bounds are
     *     inclusive/exclusive.
     */
    public static BucketBounds explicit(List<Double> bounds, BoundsInclusivity boundsInclusivity) {
      checkNotNull(bounds, "the bounds array should not be null.");
      checkArgument(bounds.size() > 0, "the bounds array should not be empty.");

      for (int i = 1; i < bounds.size(); i++) {
        if (bounds.get(i - 1) >= bounds.get(i)) {
          throw new IllegalArgumentException(
              "bounds should be in ascending order without duplicates.");
        }
      }

      return new AutoValue_Histogram_BucketBounds(ImmutableList.copyOf(bounds), boundsInclusivity);
    }

    /**
     * Like {@link #explicit(List, BoundsInclusivity)}, but sets
     * BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE value for the boundsInclusivity
     * parameter.
     */
    public static BucketBounds explicit(List<Double> bounds) {
      return explicit(bounds, BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE);
    }
  }

  /**
   * Combiner for calculating histograms.
   *
   * <p>The HistogramCombineFn class can be used with GroupBy transform to aggregate the input
   * values in the KV pair.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<ParsedMessage> pc = ...;
   * PCollection<Row> rows =
   *    pc.apply(Group.byFieldNames("dimension1", "dimension2").aggregateField("value",
   *      HistogramCombineFn.<Double>create(BucketBounds.linear(1.0, 2.0, 160)),
   *      Field.of("bucketCounts", FieldType.array(FieldType.INT64))));
   *
   * }</pre>
   */
  public static final class HistogramCombineFn<T>
      extends Combine.CombineFn<T, HistogramAccumulator, List<Long>> {

    private final double[] bounds;
    private final BoundsInclusivity boundsInclusivity;

    private HistogramCombineFn(double[] bounds, BoundsInclusivity boundsInclusivity) {
      this.bounds = bounds;
      this.boundsInclusivity = boundsInclusivity;
    }

    /**
     * Returns a histogram combiner with the given {@link BucketBounds}.
     *
     * @param bucketBounds the instance of the {@link BucketBounds} class with desired parameters of
     *     the histogram.
     */
    public static <T extends Number> HistogramCombineFn<T> create(BucketBounds bucketBounds) {
      return new HistogramCombineFn<>(
          ArrayUtils.toPrimitive(bucketBounds.getBounds().toArray(new Double[0])),
          bucketBounds.getBoundsInclusivity());
    }

    @Override
    public HistogramAccumulator createAccumulator() {
      return new HistogramAccumulator(bounds.length + 1);
    }

    @Override
    public HistogramAccumulator addInput(HistogramAccumulator accumulator, T input)
        throws IllegalArgumentException {
      if (input == null) {
        throw new NullPointerException("input should not be null.");
      }

      Double inputDoubleValue = ((Number) input).doubleValue();
      if (inputDoubleValue.isNaN() || inputDoubleValue.isInfinite()) {
        throw new IllegalArgumentException("input should not be NaN or infinite.");
      }
      int index = Arrays.binarySearch(bounds, inputDoubleValue);
      if (index < 0) {
        accumulator.counts[-index - 1]++;
      } else {
        // This means the value is on bound, can be handled based on the bound inclusivity.
        if (boundsInclusivity == BoundsInclusivity.LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE) {
          accumulator.counts[index + 1]++;
        } else {
          accumulator.counts[index]++;
        }
      }
      return accumulator;
    }

    @Override
    public HistogramAccumulator mergeAccumulators(Iterable<HistogramAccumulator> accumulators) {
      Iterator<HistogramAccumulator> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }

      HistogramAccumulator merged = iter.next();
      int countsLength = merged.counts.length;
      while (iter.hasNext()) {
        HistogramAccumulator histogramAccumulator = iter.next();
        checkArgument(
            countsLength == histogramAccumulator.counts.length,
            "number of buckets in the merging accumulators should be the same.");
        for (int i = 0; i < countsLength; ++i) {
          merged.counts[i] += histogramAccumulator.counts[i];
        }
      }
      return merged;
    }

    @Override
    public List<Long> extractOutput(HistogramAccumulator accumulator) throws NullPointerException {
      checkNotNull(accumulator, "can not output from null histogram.");
      return Longs.asList(accumulator.counts);
    }

    @Override
    public Coder<HistogramAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return new HistogramAccumulatorCoder();
    }

    @Override
    public Coder<List<Long>> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return ListCoder.of(VarLongCoder.of());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("numBuckets", bounds.length + 1).withLabel("Number of buckets"));
    }
  }

  /** Accumulator of the Histogram combiner. */
  static final class HistogramAccumulator {

    private long[] counts;

    public HistogramAccumulator(int numBuckets) {
      checkArgument(
          numBuckets > 2,
          "number of buckets should be greater than two - underflow bucket and overflow bucket.");
      this.counts = new long[numBuckets];
    }

    @Override
    public boolean equals(@Nullable Object object) {
      if (object instanceof HistogramAccumulator) {
        HistogramAccumulator other = (HistogramAccumulator) object;
        return Arrays.equals(counts, other.counts);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(counts));
    }
  }

  /** Coder for {@link HistogramAccumulator}. */
  static final class HistogramAccumulatorCoder extends CustomCoder<HistogramAccumulator> {

    private static final VarLongCoder LONG_CODER = VarLongCoder.of();
    private static final VarIntCoder INT_CODER = VarIntCoder.of();

    /**
     * Index to indicate method used where only non-empty buckets are encoded with their indices or
     * where all buckets are encoded sequentially.
     */
    private enum CoderType {
      NON_EMPTY_BUCKETS_CODER,
      ALL_BUCKETS_CODER
    }

    /** Encoded size of 0 in bytes. */
    private static final long ENCODED_ZERO_SIZE = VarInt.getLength(0);

    @Override
    public void encode(HistogramAccumulator value, OutputStream outStream) throws IOException {
      checkNotNull(value, "can not encode a null histogram.");

      int numEmptyBucketsAtTheEnd = 0;
      for (int i = value.counts.length - 1; i >= 0; i--) {
        if (value.counts[i] != 0) {
          break;
        }
        numEmptyBucketsAtTheEnd++;
      }

      if (shouldEncodeNonEmptyBucketsOnly(value, numEmptyBucketsAtTheEnd)) {
        // As we have two different encoding methods, the first byte indicates the coder used.
        outStream.write(CoderType.NON_EMPTY_BUCKETS_CODER.ordinal());
        encodeNonEmptyBuckets(value, numEmptyBucketsAtTheEnd, outStream);
      } else {
        outStream.write(CoderType.ALL_BUCKETS_CODER.ordinal());
        encodeAllBuckets(value, numEmptyBucketsAtTheEnd, outStream);
      }
    }

    /**
     * Estimates the size of the accumulator and returns whether encoding only non-empty buckets
     * with indices produces a smaller accumulator.
     *
     * <p>To check the difference in sizes between these two encoding methods, we do not add the
     * size of non-empty encoded bucket count because it is added to both, and we just cancel it for
     * both sides. So, comparison is done between sizes of encoded indices and sizes of intermediate
     * zero counts (not trailing zeros).
     */
    private boolean shouldEncodeNonEmptyBucketsOnly(
        HistogramAccumulator value, int numEmptyBucketsAtTheEnd) {
      int numBuckets = value.counts.length;
      long nonEmptyBucketIndicesEncodedSize = 0;
      long numZeroBuckets = 0;

      for (int j = 0; j < numBuckets - numEmptyBucketsAtTheEnd; j++) {
        if (value.counts[j] != 0) {
          nonEmptyBucketIndicesEncodedSize += VarInt.getLength(j);
        } else {
          numZeroBuckets++;
        }
      }

      return nonEmptyBucketIndicesEncodedSize < numZeroBuckets * ENCODED_ZERO_SIZE;
    }

    private void encodeNonEmptyBuckets(
        HistogramAccumulator value, int numEmptyBucketsAtTheEnd, OutputStream outStream)
        throws IOException {
      int numBuckets = value.counts.length;
      INT_CODER.encode(numBuckets, outStream);

      List<Integer> indices = new ArrayList<>();
      List<Long> counts = new ArrayList<>();

      for (int i = 0; i < numBuckets - numEmptyBucketsAtTheEnd; i++) {
        if (value.counts[i] != 0) {
          indices.add(i);
          counts.add(value.counts[i]);
        }
      }

      INT_CODER.encode(indices.size(), outStream);
      for (int i = 0; i < indices.size(); i++) {
        INT_CODER.encode(indices.get(i), outStream);
        LONG_CODER.encode(counts.get(i), outStream);
      }
    }

    private void encodeAllBuckets(
        HistogramAccumulator value, int numEmptyBucketsAtTheEnd, OutputStream outStream)
        throws IOException {
      int numBuckets = value.counts.length;
      INT_CODER.encode(numBuckets, outStream);

      INT_CODER.encode(numBuckets - numEmptyBucketsAtTheEnd, outStream);
      for (int i = 0; i < numBuckets - numEmptyBucketsAtTheEnd; i++) {
        LONG_CODER.encode(value.counts[i], outStream);
      }
    }

    @Override
    public HistogramAccumulator decode(InputStream inStream) throws IOException {
      int coder = inStream.read();
      int numBuckets = INT_CODER.decode(inStream);

      if (coder == CoderType.NON_EMPTY_BUCKETS_CODER.ordinal()) {
        return decodeNonEmptyBuckets(numBuckets, inStream);
      } else if (coder == CoderType.ALL_BUCKETS_CODER.ordinal()) {
        return decodeAllBuckets(numBuckets, inStream);
      } else {
        throw new CoderException("wrong decoding sequence found.");
      }
    }

    private HistogramAccumulator decodeNonEmptyBuckets(int numBuckets, InputStream inStream)
        throws IOException {
      HistogramAccumulator histogramAccumulator = new HistogramAccumulator(numBuckets);
      int nonEmptyBucketCounts = INT_CODER.decode(inStream);

      for (int i = 0; i < nonEmptyBucketCounts; i++) {
        histogramAccumulator.counts[INT_CODER.decode(inStream)] = LONG_CODER.decode(inStream);
      }

      return histogramAccumulator;
    }

    private HistogramAccumulator decodeAllBuckets(int numBuckets, InputStream inStream)
        throws IOException {
      HistogramAccumulator histogramAccumulator = new HistogramAccumulator(numBuckets);
      int numNonEmptyBucketsAtTheBeginning = INT_CODER.decode(inStream);
      for (int i = 0; i < numNonEmptyBucketsAtTheBeginning; i++) {
        histogramAccumulator.counts[i] = LONG_CODER.decode(inStream);
      }
      return histogramAccumulator;
    }
  }

  /**
   * Enum for setting whether the lower (and upper) bounds of bucket intervals are inclusive or
   * exclusive.
   */
  public enum BoundsInclusivity {
    LOWER_BOUND_EXCLUSIVE_UPPER_BOUND_INCLUSIVE,
    LOWER_BOUND_INCLUSIVE_UPPER_BOUND_EXCLUSIVE
  }
}
