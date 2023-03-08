package org.apache.beam.runners.core.metrics;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DistributionData extends DistributionData {

  private final long sum;

  private final long count;

  private final long min;

  private final long max;

  AutoValue_DistributionData(
      long sum,
      long count,
      long min,
      long max) {
    this.sum = sum;
    this.count = count;
    this.min = min;
    this.max = max;
  }

  @Override
  public long sum() {
    return sum;
  }

  @Override
  public long count() {
    return count;
  }

  @Override
  public long min() {
    return min;
  }

  @Override
  public long max() {
    return max;
  }

  @Override
  public String toString() {
    return "DistributionData{"
        + "sum=" + sum + ", "
        + "count=" + count + ", "
        + "min=" + min + ", "
        + "max=" + max
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DistributionData) {
      DistributionData that = (DistributionData) o;
      return this.sum == that.sum()
          && this.count == that.count()
          && this.min == that.min()
          && this.max == that.max();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((sum >>> 32) ^ sum);
    h$ *= 1000003;
    h$ ^= (int) ((count >>> 32) ^ count);
    h$ *= 1000003;
    h$ ^= (int) ((min >>> 32) ^ min);
    h$ *= 1000003;
    h$ ^= (int) ((max >>> 32) ^ max);
    return h$;
  }

}
