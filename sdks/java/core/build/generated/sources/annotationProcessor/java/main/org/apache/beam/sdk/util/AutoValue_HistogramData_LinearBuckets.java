package org.apache.beam.sdk.util;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_HistogramData_LinearBuckets extends HistogramData.LinearBuckets {

  private final double start;

  private final double width;

  private final int numBuckets;

  AutoValue_HistogramData_LinearBuckets(
      double start,
      double width,
      int numBuckets) {
    this.start = start;
    this.width = width;
    this.numBuckets = numBuckets;
  }

  @Override
  public double getStart() {
    return start;
  }

  @Override
  public double getWidth() {
    return width;
  }

  @Override
  public int getNumBuckets() {
    return numBuckets;
  }

  @Override
  public String toString() {
    return "LinearBuckets{"
        + "start=" + start + ", "
        + "width=" + width + ", "
        + "numBuckets=" + numBuckets
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof HistogramData.LinearBuckets) {
      HistogramData.LinearBuckets that = (HistogramData.LinearBuckets) o;
      return Double.doubleToLongBits(this.start) == Double.doubleToLongBits(that.getStart())
          && Double.doubleToLongBits(this.width) == Double.doubleToLongBits(that.getWidth())
          && this.numBuckets == that.getNumBuckets();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((Double.doubleToLongBits(start) >>> 32) ^ Double.doubleToLongBits(start));
    h$ *= 1000003;
    h$ ^= (int) ((Double.doubleToLongBits(width) >>> 32) ^ Double.doubleToLongBits(width));
    h$ *= 1000003;
    h$ ^= numBuckets;
    return h$;
  }

}
