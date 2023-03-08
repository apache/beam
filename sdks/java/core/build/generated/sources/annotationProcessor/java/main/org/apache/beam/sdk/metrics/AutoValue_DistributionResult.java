package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DistributionResult extends DistributionResult {

  private final long sum;

  private final long count;

  private final long min;

  private final long max;

  AutoValue_DistributionResult(
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
  public long getSum() {
    return sum;
  }

  @Override
  public long getCount() {
    return count;
  }

  @Override
  public long getMin() {
    return min;
  }

  @Override
  public long getMax() {
    return max;
  }

  @Override
  public String toString() {
    return "DistributionResult{"
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
    if (o instanceof DistributionResult) {
      DistributionResult that = (DistributionResult) o;
      return this.sum == that.getSum()
          && this.count == that.getCount()
          && this.min == that.getMin()
          && this.max == that.getMax();
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
