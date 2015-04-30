/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.AND;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.OR;

import com.google.common.reflect.TypeToken;

import java.util.Objects;

import javax.annotation.Nullable;

/**
 * A Counter enables the aggregation of a stream of values over time.  The
 * cumulative aggregate value is updated as new values are added, or it can be
 * reset to a new value.  Multiple kinds of aggregation are supported depending
 * on the type of the counter.
 *
 * <p>Counters compare using value equality of their name, kind, and
 * cumulative value.  Equal counters should have equal toString()s.
 *
 * @param <T> the type of values aggregated by this counter
 */
public abstract class Counter<T> {
  /**
   * Possible kinds of counter aggregation.
   */
  public static enum AggregationKind {

    /**
     * Computes the sum of all added values.
     * Applicable to {@link Integer}, {@link Long}, and {@link Double} values.
     */
    SUM,

    /**
     * Computes the maximum value of all added values.
     * Applicable to {@link Integer}, {@link Long}, and {@link Double} values.
     */
    MAX,

    /**
     * Computes the minimum value of all added values.
     * Applicable to {@link Integer}, {@link Long}, and {@link Double} values.
     */
    MIN,

    /**
     * Computes the arithmetic mean of all added values.  Applicable to
     * {@link Integer}, {@link Long}, and {@link Double} values.
     */
    MEAN,

    /**
     * Computes boolean AND over all added values.
     * Applicable only to {@link Boolean} values.
     */
    AND,

    /**
     * Computes boolean OR over all added values. Applicable only to
     * {@link Boolean} values.
     */
    OR
    // TODO: consider adding VECTOR_SUM, HISTOGRAM, KV_SET, PRODUCT, TOP.
  }

  /**
   * Constructs a new {@link Counter} that aggregates {@link Integer}, values
   * according to the desired aggregation kind. The supported aggregation kinds
   * are {@link AggregationKind#SUM}, {@link AggregationKind#MIN},
   * {@link AggregationKind#MAX}, and {@link AggregationKind#MEAN}.
   * This is a convenience wrapper over a
   * {@link Counter} implementation that aggregates {@link Long} values. This is
   * useful when the application handles (boxed) {@link Integer} values that
   * are not readily convertible to the (boxed) {@link Long} values otherwise
   * expected by the {@link Counter} implementation aggregating {@link Long}
   * values.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  public static Counter<Integer> ints(String name, AggregationKind kind) {
    return new IntegerCounter(name, kind);
  }

  /**
   * Constructs a new {@link Counter} that aggregates {@link Long} values
   * according to the desired aggregation kind. The supported aggregation kinds
   * are {@link AggregationKind#SUM}, {@link AggregationKind#MIN},
   * {@link AggregationKind#MAX}, and {@link AggregationKind#MEAN}.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  public static Counter<Long> longs(String name, AggregationKind kind) {
    return new LongCounter(name, kind);
  }

  /**
   * Constructs a new {@link Counter} that aggregates {@link Double} values
   * according to the desired aggregation kind. The supported aggregation kinds
   * are {@link AggregationKind#SUM}, {@link AggregationKind#MIN},
   * {@link AggregationKind#MAX}, and {@link AggregationKind#MEAN}.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  public static Counter<Double> doubles(String name, AggregationKind kind) {
    return new DoubleCounter(name, kind);
  }

  /**
   * Constructs a new {@link Counter} that aggregates {@link Boolean} values
   * according to the desired aggregation kind. The only supported aggregation
   * kinds are {@link AggregationKind#AND} and {@link AggregationKind#OR}.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  public static Counter<Boolean> booleans(String name, AggregationKind kind) {
    return new BooleanCounter(name, kind);
  }

  /**
   * Constructs a new {@link Counter} that aggregates {@link String} values
   * according to the desired aggregation kind. The only supported aggregation
   * kind is {@link AggregationKind#MIN} and {@link AggregationKind#MAX}.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  private static Counter<String> strings(String name, AggregationKind kind) {
    return new StringCounter(name, kind);
  }


  //////////////////////////////////////////////////////////////////////////////

  /**
   * Adds a new value to the aggregation stream. Returns this (to allow method
   * chaining).
   */
  public abstract Counter<T> addValue(T value);

  /**
   * Resets the aggregation stream to this new value. This aggregator must not
   * be a MEAN aggregator. Returns this (to allow method chaining).
   */
  public abstract Counter<T> resetToValue(T value);

  /**
   * Resets the aggregation stream to this new value. Returns this (to allow
   * method chaining). The value of elementCount must be non-negative, and this
   * aggregator must be a MEAN aggregator.
   */
  public abstract Counter<T> resetMeanToValue(long elementCount, T value);

  /**
   * Resets the counter's delta value to have no values accumulated and returns
   * the value of the delta prior to the reset.
   *
   * @return the aggregate delta at the time this method is called
   */
  public abstract T getAndResetDelta();

  /**
   * Resets the counter's delta value to have no values accumulated and returns
   * the value of the delta prior to the reset, for a MEAN counter.
   *
   * @return the mean delta t the time this method is called
   */
  public abstract CounterMean<T> getAndResetMeanDelta();

  /**
   * Returns the counter's name.
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the counter's aggregation kind.
   */
  public AggregationKind getKind() {
    return kind;
  }

  /**
   * Returns the counter's type.
   */
  public Class<?> getType() {
    return new TypeToken<T>(getClass()) {
      private static final long serialVersionUID = 0;
    }.getRawType();
  }

  /**
   * Returns the aggregated value, or the sum for MEAN aggregation, either
   * total or, if delta, since the last update extraction or resetDelta.
   */
  public abstract T getAggregate();

  /**
   * The mean value of a {@code Counter}, represented as an aggregate value and
   * a count.
   *
   * @param <T> the type of the aggregate
   */
  public static interface CounterMean<T> {
    /**
     * Gets the aggregate value of this {@code CounterMean}.
     */
    T getAggregate();

    /**
     * Gets the count of this {@code CounterMean}.
     */
    long getCount();
  }

  /**
   * Returns the mean in the form of a CounterMean, or null if this is not a
   * MEAN counter.
   */
  @Nullable
  public abstract CounterMean<T> getMean();

  /**
   * Returns a string representation of the Counter. Useful for debugging logs.
   * Example return value: "ElementCount:SUM(15)".
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getName());
    sb.append(":");
    sb.append(getKind());
    sb.append("(");
    switch (kind) {
      case SUM:
      case MAX:
      case MIN:
      case AND:
      case OR:
        sb.append(aggregate);
        break;
      case MEAN:
        sb.append(aggregate);
        sb.append("/");
        sb.append(count);
        break;
      default:
        throw illegalArgumentException();
    }
    sb.append(")");

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof Counter) {
      Counter<?> that = (Counter<?>) o;
      return this.name.equals(that.name)
          && this.kind == that.kind
          && this.getClass().equals(that.getClass())
          && this.count == that.count
          && Objects.equals(this.aggregate, that.aggregate);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, kind, aggregate, count);
  }

  /**
   * Returns whether this Counter is compatible with that Counter.  If
   * so, they can be merged into a single Counter.
   */
  public boolean isCompatibleWith(Counter<?> that) {
    return this.name.equals(that.name)
        && this.kind == that.kind
        && this.getClass().equals(that.getClass());
  }


  //////////////////////////////////////////////////////////////////////////////

  /** The name of this counter. */
  protected final String name;

  /** The kind of aggregation function to apply to this counter. */
  protected final AggregationKind kind;

  /** The total cumulative aggregation value. Holds sum for MEAN aggregation. */
  protected T aggregate;

  /** The cumulative aggregation value since the last update extraction. */
  protected T deltaAggregate;

  /** The total number of aggregated values. Useful for MEAN aggregation. */
  protected long count;

  /** The number of aggregated values since the last update extraction. */
  protected long deltaCount;

  protected Counter(String name, AggregationKind kind) {
    this.name = name;
    this.kind = kind;
    this.count = 0;
    this.deltaCount = 0;
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Implements a {@link Counter} for {@link Long} values.
   */
  private static class LongCounter extends Counter<Long> {

    /** Initializes a new {@link Counter} for {@link Long} values. */
    private LongCounter(String name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case SUM:
        case MEAN:
          aggregate = deltaAggregate = 0L;
          break;
        case MAX:
          aggregate = deltaAggregate = Long.MIN_VALUE;
          break;
        case MIN:
          aggregate = deltaAggregate = Long.MAX_VALUE;
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public synchronized LongCounter addValue(Long value) {
      switch (kind) {
        case SUM:
          aggregate += value;
          deltaAggregate += value;
          break;
        case MEAN:
          aggregate += value;
          deltaAggregate += value;
          count++;
          deltaCount++;
          break;
        case MAX:
          aggregate = Math.max(aggregate, value);
          deltaAggregate = Math.max(deltaAggregate, value);
          break;
        case MIN:
          aggregate = Math.min(aggregate, value);
          deltaAggregate = Math.min(deltaAggregate, value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized Long getAggregate() {
      return aggregate;
    }

    @Override
    public synchronized Long getAndResetDelta() {
      long oldDelta = deltaAggregate;
      switch (kind) {
        case SUM:
          deltaAggregate = 0L;
          break;
        case MEAN:
          deltaAggregate = 0L;
          deltaCount = 0;
          break;
        case MAX:
          deltaAggregate = Long.MIN_VALUE;
          break;
        case MIN:
          deltaAggregate = Long.MAX_VALUE;
          break;
        default:
          throw illegalArgumentException();
      }
      return oldDelta;
    }

    @Override
    public synchronized Counter<Long> resetToValue(Long value) {
      if (kind == MEAN) {
        throw illegalArgumentException();
      }
      aggregate = value;
      deltaAggregate = value;
      return this;
    }

    @Override
    public synchronized Counter<Long> resetMeanToValue(long elementCount,
        Long value) {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      aggregate = value;
      deltaAggregate = value;
      count = elementCount;
      deltaCount = elementCount;
      return this;
    }

    @Override
    public synchronized CounterMean<Long> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      CounterMean<Long> mean = new LongCounterMean(deltaAggregate, deltaCount);
      deltaAggregate = 0L;
      deltaCount = 0L;
      return mean;
    }

    @Override
    @Nullable
    public synchronized CounterMean<Long> getMean() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return new LongCounterMean(aggregate, count);
    }

    private static class LongCounterMean implements CounterMean<Long> {
      private final long aggregate;
      private final long count;

      public LongCounterMean(long aggregate, long count) {
        this.aggregate = aggregate;
        this.count = count;
      }

      @Override
      public Long getAggregate() {
        return aggregate;
      }

      @Override
      public long getCount() {
        return count;
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link Double} values.
   */
  private static class DoubleCounter extends Counter<Double> {

    /** Initializes a new {@link Counter} for {@link Double} values. */
    private DoubleCounter(String name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case SUM:
        case MEAN:
          aggregate = deltaAggregate = 0.0;
          break;
        case MAX:
          aggregate = deltaAggregate = Double.NEGATIVE_INFINITY;
          break;
        case MIN:
          aggregate = deltaAggregate = Double.POSITIVE_INFINITY;
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public synchronized DoubleCounter addValue(Double value) {
      switch (kind) {
        case SUM:
          aggregate += value;
          deltaAggregate += value;
          break;
        case MEAN:
          aggregate += value;
          deltaAggregate += value;
          count++;
          deltaCount++;
          break;
        case MAX:
          aggregate = Math.max(aggregate, value);
          deltaAggregate = Math.max(deltaAggregate, value);
          break;
        case MIN:
          aggregate = Math.min(aggregate, value);
          deltaAggregate = Math.min(deltaAggregate, value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized Double getAndResetDelta() {
      double oldDelta = deltaAggregate;
      switch (kind) {
        case SUM:
          deltaAggregate = 0.0;
          break;
        case MEAN:
          deltaAggregate = 0.0;
          deltaCount = 0;
          break;
        case MAX:
          deltaAggregate = Double.NEGATIVE_INFINITY;
          break;
        case MIN:
          deltaAggregate = Double.POSITIVE_INFINITY;
          break;
        default:
          throw illegalArgumentException();
      }
      return oldDelta;
    }

    @Override
    public synchronized Counter<Double> resetToValue(Double value) {
      if (kind == MEAN) {
        throw illegalArgumentException();
      }
      aggregate = value;
      deltaAggregate = value;
      return this;
    }

    @Override
    public synchronized Counter<Double> resetMeanToValue(long elementCount,
        Double value) {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      if (elementCount < 0) {
        throw new IllegalArgumentException("elementCount must be non-negative");
      }
      aggregate = value;
      deltaAggregate = value;
      count = elementCount;
      deltaCount = elementCount;
      return this;
    }

    @Override
    public synchronized CounterMean<Double> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      CounterMean<Double> mean =
          new DoubleCounterMean(deltaAggregate, deltaCount);
      deltaAggregate = 0.0;
      deltaCount = 0L;
      return mean;
    }

    @Override
    public synchronized Double getAggregate() {
      return aggregate;
    }

    @Override
    @Nullable
    public synchronized CounterMean<Double> getMean() {
      return new DoubleCounterMean(aggregate, count);
    }

    private static class DoubleCounterMean implements CounterMean<Double> {
      private final double aggregate;
      private final long count;

      public DoubleCounterMean(double aggregate, long count) {
        this.aggregate = aggregate;
        this.count = count;
      }

      @Override
      public Double getAggregate() {
        return aggregate;
      }

      @Override
      public long getCount() {
        return count;
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link Boolean} values.
   */
  private static class BooleanCounter extends Counter<Boolean> {

    /** Initializes a new {@link Counter} for {@link Boolean} values. */
    private BooleanCounter(String name, AggregationKind kind) {
      super(name, kind);
      if (kind.equals(AND)) {
        aggregate = deltaAggregate = true;
      } else if (kind.equals(OR)) {
        aggregate = deltaAggregate = false;
      } else {
        throw illegalArgumentException();
      }
    }

    @Override
    public synchronized BooleanCounter addValue(Boolean value) {
      if (kind.equals(AND)) {
        aggregate &= value;
        deltaAggregate &= value;
      } else { // kind.equals(OR))
        aggregate |= value;
        deltaAggregate |= value;
      }
      return this;
    }

    @Override
    public synchronized Boolean getAndResetDelta() {
      boolean delta = deltaAggregate;
      switch (kind) {
        case AND:
          deltaAggregate = true;
          break;
        case OR:
          deltaAggregate = false;
          break;
        default:
          throw illegalArgumentException();
      }
      return delta;
    }

    @Override
    public synchronized Counter<Boolean> resetToValue(Boolean value) {
      aggregate = value;
      deltaAggregate = value;
      return this;
    }

    @Override
    public Counter<Boolean> resetMeanToValue(long elementCount, Boolean value) {
      throw illegalArgumentException();
    }

    @Override
    public com.google.cloud.dataflow.sdk.util.common.Counter.CounterMean<
        Boolean> getAndResetMeanDelta() {
      throw illegalArgumentException();
    }

    @Override
    public synchronized Boolean getAggregate() {
      return aggregate;
    }

    @Override
    @Nullable
    public CounterMean<Boolean> getMean() {
      throw illegalArgumentException();
    }
  }

  /**
   * Implements a {@link Counter} for {@link String} values.
   */
  private static class StringCounter extends Counter<String> {

    /** Initializes a new {@link Counter} for {@link String} values. */
    private StringCounter(String name, AggregationKind kind) {
      super(name, kind);
      // TODO: Support MIN, MAX of Strings.
      throw illegalArgumentException();
    }

    @Override
    public synchronized StringCounter addValue(String value) {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<String> resetToValue(String value) {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<String> resetMeanToValue(long elementCount, String value) {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public String getAndResetDelta() {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public CounterMean<String> getAndResetMeanDelta() {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public String getAggregate() {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    @Nullable
    public CounterMean<String> getMean() {
      switch (kind) {
        default:
          throw illegalArgumentException();
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link Integer} values.
   */
  private static class IntegerCounter extends Counter<Integer> {

    /** Initializes a new {@link Counter} for {@link Integer} values. */
    private IntegerCounter(String name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case SUM:
        case MEAN:
          aggregate = deltaAggregate = 0;
          break;
        case MAX:
          aggregate = deltaAggregate = Integer.MIN_VALUE;
          break;
        case MIN:
          aggregate = deltaAggregate = Integer.MAX_VALUE;
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public synchronized IntegerCounter addValue(Integer value) {
      switch (kind) {
        case SUM:
          aggregate += value;
          deltaAggregate += value;
          break;
        case MEAN:
          aggregate += value;
          deltaAggregate += value;
          count++;
          deltaCount++;
          break;
        case MAX:
          aggregate = Math.max(aggregate, value);
          deltaAggregate = Math.max(deltaAggregate, value);
          break;
        case MIN:
          aggregate = Math.min(aggregate, value);
          deltaAggregate = Math.min(deltaAggregate, value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized Integer getAndResetDelta() {
      int delta = deltaAggregate;
      switch (kind) {
        case SUM:
          deltaAggregate = 0;
          break;
        case MEAN:
          deltaAggregate = 0;
          deltaCount = 0;
          break;
        case MAX:
          deltaAggregate = Integer.MIN_VALUE;
          break;
        case MIN:
          deltaAggregate = Integer.MAX_VALUE;
          break;
        default:
          throw illegalArgumentException();
      }
      return delta;
    }

    @Override
    public synchronized Counter<Integer> resetToValue(Integer value) {
      if (kind == MEAN) {
        throw illegalArgumentException();
      }
      aggregate = value;
      deltaAggregate = value;
      return this;
    }

    @Override
    public Counter<Integer> resetMeanToValue(long elementCount, Integer value) {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      if (elementCount < 0) {
        throw new IllegalArgumentException("elementCount must be non-negative");
      }
      aggregate = value;
      deltaAggregate = value;
      count = value;
      deltaCount = value;
      return this;
    }

    @Override
    public synchronized CounterMean<Integer> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      CounterMean<Integer> mean =
          new IntegerCounterMean(deltaAggregate, deltaCount);
      deltaAggregate = 0;
      deltaCount = 0L;
      return mean;
    }

    @Override
    public synchronized Integer getAggregate() {
      return aggregate;
    }

    @Override
    @Nullable
    public synchronized CounterMean<Integer> getMean() {
      return new IntegerCounterMean(aggregate, count);
    }

    private static class IntegerCounterMean implements CounterMean<Integer> {
      private final int aggregate;
      private final long count;

      public IntegerCounterMean(int aggregate, long count) {
        this.aggregate = aggregate;
        this.count = count;
      }

      @Override
      public Integer getAggregate() {
        return aggregate;
      }

      @Override
      public long getCount() {
        return count;
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Constructs an {@link IllegalArgumentException} explaining that this
   * {@link Counter}'s aggregation kind is not supported by its value type.
   */
  protected IllegalArgumentException illegalArgumentException() {
    return new IllegalArgumentException("Cannot compute " + kind
        + " aggregation over " + getType().getSimpleName() + " values.");
  }
}
