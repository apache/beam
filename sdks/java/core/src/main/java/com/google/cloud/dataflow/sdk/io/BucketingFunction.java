package com.google.cloud.dataflow.sdk.io;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

/**
 * Keep track of the minimum/maximum/sum of a set of timestamped long values.
 * For efficiency, bucket values by their timestamp.
 */
public class BucketingFunction {
  private static class Bucket {
    private int numSamples;
    private long combinedValue;

    public Bucket(BucketingFunction outer) {
      numSamples = 0;
      combinedValue = outer.function.zero();
    }

    public void add(BucketingFunction outer, long value) {
      combinedValue = outer.function.f(combinedValue, value);
      numSamples++;
    }

    public boolean remove() {
      numSamples--;
      Preconditions.checkState(numSamples >= 0);
      return numSamples == 0;
    }

    public long get() {
      return combinedValue;
    }
  }

  /**
   * How large a time interval to fit within each bucket.
   */
  private final long bucketWidthMs;

  /**
   * How many buckets are considered 'significant'?
   */
  private final int numSignificantBuckets;

  /**
   * How many samples are considered 'significant'?
   */
  private final int numSignificantSamples;

  /**
   * Function for combining sample values.
   */
  private final SimpleFunction function;

  /**
   * Active buckets.
   */
  private final Map<Long, Bucket> buckets;

  public BucketingFunction(
      long bucketWidthMs,
      int numSignificantBuckets,
      int numSignificantSamples,
      SimpleFunction function) {
    this.bucketWidthMs = bucketWidthMs;
    this.numSignificantBuckets = numSignificantBuckets;
    this.numSignificantSamples = numSignificantSamples;
    this.function = function;
    this.buckets = new HashMap<>();
  }

  /**
   * Which bucket key corresponds to {@code timeMsSinceEpoch}.
   */
  private long key(long timeMsSinceEpoch) {
    return timeMsSinceEpoch - (timeMsSinceEpoch % bucketWidthMs);
  }

  /**
   * Add one sample of {@code value} (to bucket) at {@code timeMsSinceEpoch}.
   */
  public void add(long timeMsSinceEpoch, long value) {
    long key = key(timeMsSinceEpoch);
    Bucket bucket = buckets.get(key);
    if (bucket == null) {
      bucket = new Bucket(this);
      buckets.put(key, bucket);
    }
    bucket.add(this, value);
  }

  /**
   * Remove one sample (from bucket) at {@code timeMsSinceEpoch}.
   */
  public void remove(long timeMsSinceEpoch) {
    long key = key(timeMsSinceEpoch);
    Bucket bucket = buckets.get(key);
    if (bucket == null) {
      return;
    }
    if (bucket.remove()) {
      buckets.remove(key);
    }
  }

  /**
   * Return the (bucketized) combined value of all samples.
   */
  public long get() {
    long result = function.zero();
    for (Bucket bucket : buckets.values()) {
      result = function.f(result, bucket.get());
    }
    return result;
  }

  /**
   * Is the current result 'significant'? Ie is it drawn from enough buckets
   * or from enough samples?
   */
  public boolean isSignificant() {
    if (buckets.size() >= numSignificantBuckets) {
      return true;
    }
    int totalSamples = 0;
    for (Bucket bucket : buckets.values()) {
      totalSamples += bucket.numSamples;
    }
    return totalSamples >= numSignificantSamples;
  }
}
