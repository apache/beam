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
package org.apache.beam.runners.core.metrics;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.datasketches.quantiles.DoublesUnionBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * Data describing the the distribution. This should retain enough detail that it can be combined
 * with other {@link DistributionData}.
 *
 * <p>Datasketch library is used to compute percentiles. See {@linktourl
 * https://datasketches.apache.org/}.
 *
 * <p>This is kept distinct from {@link DistributionResult} since this may be extended to include
 * data necessary to approximate quantiles, etc. while {@link DistributionResult} would just include
 * the approximate value of those quantiles.
 */
public class DistributionData implements Serializable {
  // k = 256 should yield an approximate error ε of less than 1%
  private static final int SKETCH_SUMMARY_SIZE = 256;

  private final Set<Double> percentiles;
  private long sum;
  private long count;
  private long min;
  private long max;
  private transient Optional<UpdateDoublesSketch> sketch;

  /** Creates an instance of DistributionData with custom percentiles. */
  public static DistributionData withPercentiles(Set<Double> percentiles) {
    return new DistributionData(0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, percentiles);
  }

  /** Backward compatible static factory method. */
  public static DistributionData empty() {
    return new DistributionData(0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, ImmutableSet.of());
  }

  /** Static factory method primary used for testing. */
  @VisibleForTesting
  public static DistributionData create(long sum, long count, long min, long max) {
    return new DistributionData(sum, count, min, max, ImmutableSet.of());
  }

  private DistributionData(long sum, long count, long min, long max, Set<Double> percentiles) {
    this.sum = sum;
    this.count = count;
    this.min = min;
    this.max = max;
    this.percentiles = percentiles;
    if (!percentiles.isEmpty()) {
      final DoublesSketchBuilder doublesSketchBuilder = new DoublesSketchBuilder();
      this.sketch = Optional.of(doublesSketchBuilder.setK(SKETCH_SUMMARY_SIZE).build());
    } else {
      this.sketch = Optional.empty();
    }
  }

  public static DistributionData singleton(long value) {
    final DistributionData distributionData = empty();
    distributionData.update(value);
    return distributionData;
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Getters

  public long sum() {
    return sum;
  }

  public long count() {
    return count;
  }

  public long min() {
    return min;
  }

  public long max() {
    return max;
  }

  /** Gets the percentiles and the percentiles values as a map. */
  public Map<Double, Double> percentiles() {
    if (!sketch.isPresent() || sketch.get().getN() == 0) {
      // if the sketch is not present or is empty, do not compute the percentile
      return ImmutableMap.of();
    }

    double[] quantiles = percentiles.stream().mapToDouble(i -> i / 100).toArray();
    double[] quantileResults = sketch.get().getQuantiles(quantiles);

    final ImmutableMap.Builder<Double, Double> resultBuilder = ImmutableMap.builder();
    for (int k = 0; k < quantiles.length; k++) {
      resultBuilder.put(quantiles[k] * 100, quantileResults[k]);
    }
    return resultBuilder.build();
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Updates the distribution with a value. For percentiles, only add the value to the sketch.
   * Percentile will be computed prior to calling {@link DistributionCell#getCumulative()} or in
   * {@link #extractResult()}.
   *
   * @param value value to update the distribution with.
   */
  public void update(long value) {
    ++count;
    min = Math.min(min, value);
    max = Math.max(max, value);
    sum += value;
    sketch.ifPresent(currSketch -> currSketch.update(value));
  }

  /** Merges two distributions. */
  public DistributionData combine(DistributionData other) {
    if (sketch.isPresent()
        && other.sketch.isPresent()
        && sketch.get().getN() > 0
        && other.sketch.get().getN() > 0) {
      final DoublesUnion union = new DoublesUnionBuilder().build();
      union.update(sketch.get());
      union.update(other.sketch.get());
      sketch = Optional.of(union.getResult());
    } else if (other.sketch.isPresent() && other.sketch.get().getN() > 0) {
      sketch = other.sketch;
    }
    sum += other.sum;
    count += other.count;
    max = Math.max(max, other.max);
    min = Math.min(min, other.min);
    return this;
  }

  public DistributionData reset() {
    this.sum = 0L;
    this.count = 0L;
    this.min = Long.MAX_VALUE;
    this.max = Long.MIN_VALUE;
    if (!this.percentiles.isEmpty()) {
      final DoublesSketchBuilder doublesSketchBuilder = new DoublesSketchBuilder();
      this.sketch = Optional.of(doublesSketchBuilder.setK(SKETCH_SUMMARY_SIZE).build());
    } else {
      this.sketch = Optional.empty();
    }
    return this;
  }

  /** Generates DistributionResult from DistributionData. */
  public DistributionResult extractResult() {
    return DistributionResult.create(sum(), count(), min(), max(), percentiles());
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof DistributionData) {
      DistributionData other = (DistributionData) object;
      return Objects.equals(max, other.max())
          && Objects.equals(min, other.min())
          && Objects.equals(count, other.count())
          && Objects.equals(sum, other.sum())
          && Objects.equals(percentiles(), other.percentiles());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max, sum, count, percentiles());
  }

  @Override
  public String toString() {
    return "DistributionData{"
        + "sum="
        + sum
        + ", "
        + "count="
        + count
        + ", "
        + "min="
        + min
        + ", "
        + "max="
        + max
        + ", "
        + "percentiles="
        + percentiles()
        + "}";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    if (sketch.isPresent()) {
      byte[] bytes = sketch.get().toByteArray();
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    if (!this.percentiles.isEmpty()) {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.read(bytes);
      this.sketch = Optional.of(UpdateDoublesSketch.heapify(Memory.wrap(bytes)));
    } else {
      this.sketch = Optional.empty();
    }
  }
}
