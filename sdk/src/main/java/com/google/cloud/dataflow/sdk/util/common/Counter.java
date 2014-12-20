/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SET;

import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

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
     * Computes the set of all added values.  Applicable to {@link Integer},
     * {@link Long}, {@link Double}, and {@link String} values.
     */
    SET,

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
   * {@link AggregationKind#MAX}, {@link AggregationKind#MEAN}, and
   * {@link AggregationKind#SET}. This is a convenience wrapper over a
   * {@link Counter} implementation that aggregates {@link Long} values. This is
   * useful when the application handles (boxed) {@link Integer} values which
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
   * {@link AggregationKind#MAX}, {@link AggregationKind#MEAN}, and
   * {@link AggregationKind#SET}.
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
   * {@link AggregationKind#MAX}, {@link AggregationKind#MEAN}, and
   * {@link AggregationKind#SET}.
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
   * kind is {@link AggregationKind#SET}.
   *
   * @param name the name of the new counter
   * @param kind the new counter's aggregation kind
   * @return the newly constructed Counter
   * @throws IllegalArgumentException if the aggregation kind is not supported
   */
  public static Counter<String> strings(String name, AggregationKind kind) {
    return new StringCounter(name, kind);
  }


  //////////////////////////////////////////////////////////////////////////////

  /**
   * Adds a new value to the aggregation stream. Returns this (to allow method
   * chaining).
   */
  public abstract Counter<T> addValue(T value);

  /**
   * Resets the aggregation stream to this new value. Returns this (to allow
   * method chaining).
   */
  public Counter<T> resetToValue(T value) {
    return resetToValue(-1, value);
  }

  /**
   * Resets the aggregation stream to this new value. Returns this (to allow
   * method chaining). The value of elementCount must be -1 for non-MEAN
   * aggregations. The value of elementCount must be non-negative for MEAN
   * aggregation.
   */
  public synchronized Counter<T> resetToValue(long elementCount, T value) {
    aggregate = value;
    deltaAggregate = value;

    if (kind.equals(MEAN)) {
      if (elementCount < 0) {
        throw new AssertionError(
            "elementCount must be non-negative for MEAN aggregation");
      }
      count = elementCount;
      deltaCount = elementCount;
    } else {
      if (elementCount != -1) {
        throw new AssertionError(
            "elementCount must be -1 for non-MEAN aggregations");
      }
      count = 0;
      deltaCount = 0;
    }

    if (kind.equals(SET)) {
      set.clear();
      set.add(value);
      deltaSet = new HashSet<>();
      deltaSet.add(value);
    }
    return this;
  }

  /** Resets the counter's delta value to have no values accumulated. */
  public abstract void resetDelta();

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
   * total or, if delta, since the last update extraction or resetDelta,
   * if not a SET aggregation.
   */
  public T getAggregate(boolean delta) {
    return delta ? deltaAggregate : aggregate;
  }

  /**
   * Returns the number of aggregated values, either total or, if
   * delta, since the last update extraction or resetDelta, if a MEAN
   * aggregation.
   */
  public long getCount(boolean delta) {
    return delta ? deltaCount : count;
  }

  /**
   * Returns the set of all aggregated values, either total or, if
   * delta, since the last update extraction or resetDelta, if a SET
   * aggregation.
   */
  public Set<T> getSet(boolean delta) {
    return delta ? deltaSet : set;
  }

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
      case SET:
        sb.append(set);
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
          && Objects.equals(this.aggregate, that.aggregate)
          && Objects.equals(this.set, that.set);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, kind, aggregate, count, set);
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

  /** Holds the set of all aggregated values. Used only for SET aggregation. */
  protected Set<T> set;

  /** Holds the set of aggregated values since the last update extraction. */
  protected Set<T> deltaSet;

  protected Counter(String name, AggregationKind kind) {
    this.name = name;
    this.kind = kind;
    this.count = 0;
    this.deltaCount = 0;
    if (kind.equals(SET)) {
      set = new HashSet<>();
      deltaSet = new HashSet<>();
    }
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
        case SET:
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
        case SET:
          set.add(value);
          deltaSet.add(value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized void resetDelta() {
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
        case SET:
          deltaSet = new HashSet<>();
          break;
        default:
          throw illegalArgumentException();
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
          aggregate = deltaAggregate = Double.MIN_VALUE;
          break;
        case MIN:
          aggregate = deltaAggregate = Double.MAX_VALUE;
          break;
        case SET:
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
        case SET:
          set.add(value);
          deltaSet.add(value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized void resetDelta() {
      switch (kind) {
        case SUM:
          deltaAggregate = 0.0;
          break;
        case MEAN:
          deltaAggregate = 0.0;
          deltaCount = 0;
          break;
        case MAX:
          deltaAggregate = Double.MIN_VALUE;
          break;
        case MIN:
          deltaAggregate = Double.MAX_VALUE;
          break;
        case SET:
          deltaSet = new HashSet<>();
          break;
        default:
          throw illegalArgumentException();
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
    public synchronized void resetDelta() {
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
    }
  }

  /**
   * Implements a {@link Counter} for {@link String} values.
   */
  private static class StringCounter extends Counter<String> {

    /** Initializes a new {@link Counter} for {@link String} values. */
    private StringCounter(String name, AggregationKind kind) {
      super(name, kind);
      if (!kind.equals(SET)) {
        throw illegalArgumentException();
      }
    }

    @Override
    public synchronized StringCounter addValue(String value) {
      set.add(value);
      deltaSet.add(value);
      return this;
    }

    @Override
    public synchronized void resetDelta() {
      switch (kind) {
        case SET:
          deltaSet = new HashSet<>();
          break;
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
        case SET:
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
        case SET:
          set.add(value);
          deltaSet.add(value);
          break;
        default:
          throw illegalArgumentException();
      }
      return this;
    }

    @Override
    public synchronized void resetDelta() {
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
        case SET:
          deltaSet = new HashSet<>();
          break;
        default:
          throw illegalArgumentException();
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


  //////////////////////////////////////////////////////////////////////////////

  // For testing.
  synchronized T getTotalAggregate() { return aggregate; }
  synchronized T getDeltaAggregate() { return deltaAggregate; }
  synchronized long getTotalCount() { return count; }
  synchronized long getDeltaCount() { return deltaCount; }
  synchronized Set<T> getTotalSet() { return set; }
  synchronized Set<T> getDeltaSet() { return deltaSet; }
}
