package com.google.cloud.dataflow.sdk.io;

/**
 * Some simple functions (commutative and associative with a zero)
 * usable with {@link MovingFunction} and {@link BucketingFunction}.
 */
public enum SimpleFunction {
  MIN {
    @Override
    public long f(long l, long r) {
      return Math.min(l, r);
    }

    @Override
    public long zero() {
      return Long.MAX_VALUE;
    }
  },
  MAX {
    @Override
    public long f(long l, long r) {
      return Math.max(l, r);
    }

    @Override
    public long zero() {
      return Long.MIN_VALUE;
    }
  },
  SUM {
    @Override
    public long f(long l, long r) {
      return l + r;
    }

    @Override
    public long zero() {
      return 0;
    }
  };

  public abstract long f(long l, long r);

  public abstract long zero();
}

