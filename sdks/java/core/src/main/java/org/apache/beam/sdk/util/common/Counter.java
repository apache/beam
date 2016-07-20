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
package org.apache.beam.sdk.util.common;

import static org.apache.beam.sdk.util.common.Counter.AggregationKind.AND;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.MEAN;
import static org.apache.beam.sdk.util.common.Counter.AggregationKind.OR;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>After all possible mutations have completed, the reader should check
 * {@link #isDirty} for every counter, otherwise updates may be lost.
 *
 * <p>A counter may become dirty without a corresponding update to the value.
 * This generally will occur when the calls to {@code addValue()}, {@code committing()},
 * and {@code committed()} are interleaved such that the value is updated
 * between the calls to committing and the read of the value.
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
  public static Counter<Integer> ints(CounterName name, AggregationKind kind) {
    return new IntegerCounter(name, kind);
  }

  /**
   * Constructs a new {@code Counter<Integer>} with an unstructured name.
   *
   * @deprecated use {@link #ints(CounterName, AggregationKind)}.
   */
  @Deprecated
  public static Counter<Integer> ints(String name, AggregationKind kind) {
    return new IntegerCounter(CounterName.named(name), kind);
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
  public static Counter<Long> longs(CounterName name, AggregationKind kind) {
    return new LongCounter(name, kind);
  }

  /**
   * Constructs a new {@code Counter<Long>} with an unstructured name.
   *
   * @deprecated use {@link #longs(CounterName, AggregationKind)}.
   */
  @Deprecated
  public static Counter<Long> longs(String name, AggregationKind kind) {
    return new LongCounter(CounterName.named(name), kind);
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
  public static Counter<Double> doubles(CounterName name, AggregationKind kind) {
    return new DoubleCounter(name, kind);
  }

  /**
   * Constructs a new {@code Counter<Double>} with an unstructured name.
   *
   * @deprecated use {@link #doubles(CounterName, AggregationKind)}.
   */
  @Deprecated
  public static Counter<Double> doubles(String name, AggregationKind kind) {
    return new DoubleCounter(CounterName.named(name), kind);
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
  public static Counter<Boolean> booleans(CounterName name, AggregationKind kind) {
    return new BooleanCounter(name, kind);
  }

  /**
   * Constructs a new {@code Counter<Boolean>} with an unstructured name.
   *
   * @deprecated use {@link #booleans(CounterName, AggregationKind)}.
   */
  @Deprecated
  public static Counter<Boolean> booleans(String name, AggregationKind kind) {
    return new BooleanCounter(CounterName.named(name), kind);
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
   * Returns the counter's flat name.
   */
  public String getFlatName() {
    return name.getFlatName();
  }

  /**
   * Returns the counter's name.
   *
   * @deprecated use {@link #getFlatName}.
   */
  @Deprecated
  public String getName() {
    return name.getFlatName();
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
    return new TypeDescriptor<T>(getClass()) {}.getRawType();
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
   * Represents whether counters' values have been committed to the backend.
   *
   * <p>Runners can use this information to optimize counters updates.
   * For example, if counters are committed, runners may choose to skip the updates.
   *
   * <p>Counters' state transition table:
   * {@code
   * Action\Current State         COMMITTED        DIRTY        COMMITTING
   * addValue()                   DIRTY            DIRTY        DIRTY
   * committing()                 None             COMMITTING   None
   * committed()                  None             None         COMMITTED
   * }
   */
  @VisibleForTesting
  enum CommitState {
    /**
     * There are no local updates that haven't been committed to the backend.
     */
    COMMITTED,
    /**
     * There are local updates that haven't been committed to the backend.
     */
    DIRTY,
    /**
     * Local updates are committing to the backend, but are still pending.
     */
    COMMITTING,
  }

  /**
   * Returns if the counter contains non-committed aggregate.
   */
  public boolean isDirty() {
    return commitState.get() != CommitState.COMMITTED;
  }

  /**
   * Changes the counter from {@code CommitState.DIRTY} to {@code CommitState.COMMITTING}.
   *
   * @return true if successful. False return indicates that the commit state
   * was not in {@code CommitState.DIRTY}.
   */
  public boolean committing() {
    return commitState.compareAndSet(CommitState.DIRTY, CommitState.COMMITTING);
  }

  /**
   * Changes the counter from {@code CommitState.COMMITTING} to {@code CommitState.COMMITTED}.
   *
   * @return true if successful.
   *
   * <p>False return indicates that the counter was updated while the committing is pending.
   * That counter update might or might not has been committed. The {@code commitState} has to
   * stay in {@code CommitState.DIRTY}.
   */
  public boolean committed() {
    return commitState.compareAndSet(CommitState.COMMITTING, CommitState.COMMITTED);
  }

  /**
   * Sets the counter to {@code CommitState.DIRTY}.
   *
   * <p>Must be called at the end of {@link #addValue}, {@link #resetToValue},
   * {@link #resetMeanToValue}, and {@link #merge}.
   */
  protected void setDirty() {
    commitState.set(CommitState.DIRTY);
  }

  /**
   * Returns a string representation of the Counter. Useful for debugging logs.
   * Example return value: "ElementCount:SUM(15)".
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getFlatName());
    sb.append(":");
    sb.append(getKind());
    sb.append("(");
    switch (kind) {
      case SUM:
      case MAX:
      case MIN:
      case AND:
      case OR:
        sb.append(getAggregate());
        break;
      case MEAN:
        sb.append(getMean());
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
      if (this.name.equals(that.name) && this.kind == that.kind
          && this.getClass().equals(that.getClass())) {
        if (kind == MEAN) {
          CounterMean<T> thisMean = this.getMean();
          CounterMean<?> thatMean = that.getMean();
          return thisMean == thatMean
              || (Objects.equals(thisMean.getAggregate(), thatMean.getAggregate())
                     && thisMean.getCount() == thatMean.getCount());
        } else {
          return Objects.equals(this.getAggregate(), that.getAggregate());
        }
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (kind == MEAN) {
      CounterMean<T> mean = getMean();
      return Objects.hash(getClass(), name, kind, mean.getAggregate(), mean.getCount());
    } else {
      return Objects.hash(getClass(), name, kind, getAggregate());
    }
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

  /**
   * Merges this counter with the provided counter, returning this counter with the combined value
   * of both counters. This may reset the delta of this counter.
   *
   * @throws IllegalArgumentException if the provided Counter is not compatible with this Counter
   */
  public abstract Counter<T> merge(Counter<T> that);

  //////////////////////////////////////////////////////////////////////////////

  /** The name and metadata of this counter. */
  protected final CounterName name;

  /** The kind of aggregation function to apply to this counter. */
  protected final AggregationKind kind;

  /** The commit state of this counter. */
  protected final AtomicReference<CommitState> commitState;

  protected Counter(CounterName name, AggregationKind kind) {
    this.name = name;
    this.kind = kind;
    this.commitState = new AtomicReference<>(CommitState.COMMITTED);
  }

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Implements a {@link Counter} for {@link Long} values.
   */
  private static class LongCounter extends Counter<Long> {
    private final AtomicLong aggregate;
    private final AtomicLong deltaAggregate;
    private final AtomicReference<LongCounterMean> mean;
    private final AtomicReference<LongCounterMean> deltaMean;

    /** Initializes a new {@link Counter} for {@link Long} values. */
    private LongCounter(CounterName name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case MEAN:
          mean = new AtomicReference<>();
          deltaMean = new AtomicReference<>();
          getAndResetMeanDelta();
          mean.set(deltaMean.get());
          aggregate = deltaAggregate = null;
          break;
        case SUM:
        case MAX:
        case MIN:
          aggregate = new AtomicLong();
          deltaAggregate = new AtomicLong();
          getAndResetDelta();
          aggregate.set(deltaAggregate.get());
          mean = deltaMean = null;
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public LongCounter addValue(Long value) {
      try {
        switch (kind) {
          case SUM:
            aggregate.addAndGet(value);
            deltaAggregate.addAndGet(value);
            break;
          case MEAN:
            addToMeanAndSet(value, mean);
            addToMeanAndSet(value, deltaMean);
            break;
          case MAX:
            maxAndSet(value, aggregate);
            maxAndSet(value, deltaAggregate);
            break;
          case MIN:
            minAndSet(value, aggregate);
            minAndSet(value, deltaAggregate);
            break;
          default:
            throw illegalArgumentException();
        }
        return this;
      } finally {
        setDirty();
      }
    }

    private void minAndSet(Long value, AtomicLong target) {
      long current;
      long update;
      do {
        current = target.get();
        update = Math.min(value, current);
      } while (update < current && !target.compareAndSet(current, update));
    }

    private void maxAndSet(Long value, AtomicLong target) {
      long current;
      long update;
      do {
        current = target.get();
        update = Math.max(value, current);
      } while (update > current && !target.compareAndSet(current, update));
    }

    private void addToMeanAndSet(Long value, AtomicReference<LongCounterMean> target) {
      LongCounterMean current;
      LongCounterMean update;
      do {
        current = target.get();
        update = new LongCounterMean(current.getAggregate() + value, current.getCount() + 1L);
      } while (!target.compareAndSet(current, update));
    }

    @Override
    public Long getAggregate() {
      if (kind != MEAN) {
        return aggregate.get();
      } else {
        return getMean().getAggregate();
      }
    }

    @Override
    public Long getAndResetDelta() {
      switch (kind) {
        case SUM:
          return deltaAggregate.getAndSet(0L);
        case MAX:
          return deltaAggregate.getAndSet(Long.MIN_VALUE);
        case MIN:
          return deltaAggregate.getAndSet(Long.MAX_VALUE);
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<Long> resetToValue(Long value) {
      try {
        if (kind == MEAN) {
          throw illegalArgumentException();
        }
        aggregate.set(value);
        deltaAggregate.set(value);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public Counter<Long> resetMeanToValue(long elementCount, Long value) {
      try {
        if (kind != MEAN) {
          throw illegalArgumentException();
        }
        if (elementCount < 0) {
          throw new IllegalArgumentException("elementCount must be non-negative");
        }
        LongCounterMean counterMean = new LongCounterMean(value, elementCount);
        mean.set(counterMean);
        deltaMean.set(counterMean);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public CounterMean<Long> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return deltaMean.getAndSet(new LongCounterMean(0L, 0L));
    }

    @Override
    @Nullable
    public CounterMean<Long> getMean() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return mean.get();
    }

    @Override
    public Counter<Long> merge(Counter<Long> that) {
      try {
        checkArgument(
            this.isCompatibleWith(that), "Counters %s and %s are incompatible", this, that);
        switch (kind) {
          case SUM:
          case MIN:
          case MAX:
            return addValue(that.getAggregate());
          case MEAN:
            CounterMean<Long> thisCounterMean = this.getMean();
            CounterMean<Long> thatCounterMean = that.getMean();
            return resetMeanToValue(
                thisCounterMean.getCount() + thatCounterMean.getCount(),
                thisCounterMean.getAggregate() + thatCounterMean.getAggregate());
          default:
            throw illegalArgumentException();
        }
      } finally {
        setDirty();
      }
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

      @Override
      public String toString() {
        return aggregate + "/" + count;
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link Double} values.
   */
  private static class DoubleCounter extends Counter<Double> {
    AtomicDouble aggregate;
    AtomicDouble deltaAggregate;
    AtomicReference<DoubleCounterMean> mean;
    AtomicReference<DoubleCounterMean> deltaMean;

    /** Initializes a new {@link Counter} for {@link Double} values. */
    private DoubleCounter(CounterName name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case MEAN:
          aggregate = deltaAggregate = null;
          mean = new AtomicReference<>();
          deltaMean = new AtomicReference<>();
          getAndResetMeanDelta();
          mean.set(deltaMean.get());
          break;
        case SUM:
        case MAX:
        case MIN:
          mean = deltaMean = null;
          aggregate = new AtomicDouble();
          deltaAggregate = new AtomicDouble();
          getAndResetDelta();
          aggregate.set(deltaAggregate.get());
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public DoubleCounter addValue(Double value) {
      try {
        switch (kind) {
          case SUM:
            aggregate.addAndGet(value);
            deltaAggregate.addAndGet(value);
            break;
          case MEAN:
            addToMeanAndSet(value, mean);
            addToMeanAndSet(value, deltaMean);
            break;
          case MAX:
            maxAndSet(value, aggregate);
            maxAndSet(value, deltaAggregate);
            break;
          case MIN:
            minAndSet(value, aggregate);
            minAndSet(value, deltaAggregate);
            break;
          default:
            throw illegalArgumentException();
        }
        return this;
      } finally {
        setDirty();
      }
    }

    private void addToMeanAndSet(Double value, AtomicReference<DoubleCounterMean> target) {
      DoubleCounterMean current;
      DoubleCounterMean update;
      do {
        current = target.get();
        update = new DoubleCounterMean(current.getAggregate() + value, current.getCount() + 1);
      } while (!target.compareAndSet(current, update));
    }

    private void maxAndSet(Double value, AtomicDouble target) {
      double current;
      double update;
      do {
        current = target.get();
        update = Math.max(current, value);
      } while (update > current && !target.compareAndSet(current, update));
    }

    private void minAndSet(Double value, AtomicDouble target) {
      double current;
      double update;
      do {
        current = target.get();
        update = Math.min(current, value);
      } while (update < current && !target.compareAndSet(current, update));
    }

    @Override
    public Double getAndResetDelta() {
      switch (kind) {
        case SUM:
          return deltaAggregate.getAndSet(0.0);
        case MAX:
          return deltaAggregate.getAndSet(Double.NEGATIVE_INFINITY);
        case MIN:
          return deltaAggregate.getAndSet(Double.POSITIVE_INFINITY);
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<Double> resetToValue(Double value) {
      try {
        if (kind == MEAN) {
          throw illegalArgumentException();
        }
        aggregate.set(value);
        deltaAggregate.set(value);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public Counter<Double> resetMeanToValue(long elementCount, Double value) {
      try {
        if (kind != MEAN) {
          throw illegalArgumentException();
        }
        if (elementCount < 0) {
          throw new IllegalArgumentException("elementCount must be non-negative");
        }
        DoubleCounterMean counterMean = new DoubleCounterMean(value, elementCount);
        mean.set(counterMean);
        deltaMean.set(counterMean);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public CounterMean<Double> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return deltaMean.getAndSet(new DoubleCounterMean(0.0, 0L));
    }

    @Override
    public Double getAggregate() {
      if (kind != MEAN) {
        return aggregate.get();
      } else {
        return getMean().getAggregate();
      }
    }

    @Override
    @Nullable
    public CounterMean<Double> getMean() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return mean.get();
    }

    @Override
    public Counter<Double> merge(Counter<Double> that) {
      try {
        checkArgument(
            this.isCompatibleWith(that), "Counters %s and %s are incompatible", this, that);
        switch (kind) {
          case SUM:
          case MIN:
          case MAX:
            return addValue(that.getAggregate());
          case MEAN:
            CounterMean<Double> thisCounterMean = this.getMean();
            CounterMean<Double> thatCounterMean = that.getMean();
            return resetMeanToValue(
                thisCounterMean.getCount() + thatCounterMean.getCount(),
                thisCounterMean.getAggregate() + thatCounterMean.getAggregate());
          default:
            throw illegalArgumentException();
        }
      } finally {
        setDirty();
      }
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

      @Override
      public String toString() {
        return aggregate + "/" + count;
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link Boolean} values.
   */
  private static class BooleanCounter extends Counter<Boolean> {
    private final AtomicBoolean aggregate;
    private final AtomicBoolean deltaAggregate;

    /** Initializes a new {@link Counter} for {@link Boolean} values. */
    private BooleanCounter(CounterName name, AggregationKind kind) {
      super(name, kind);
      aggregate = new AtomicBoolean();
      deltaAggregate = new AtomicBoolean();
      getAndResetDelta();
      aggregate.set(deltaAggregate.get());
    }

    @Override
    public BooleanCounter addValue(Boolean value) {
      try {
        if (kind.equals(AND) && !value) {
          aggregate.set(value);
          deltaAggregate.set(value);
        } else if (kind.equals(OR) && value) {
          aggregate.set(value);
          deltaAggregate.set(value);
        }
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public Boolean getAndResetDelta() {
      switch (kind) {
        case AND:
          return deltaAggregate.getAndSet(true);
        case OR:
          return deltaAggregate.getAndSet(false);
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<Boolean> resetToValue(Boolean value) {
      try {
        aggregate.set(value);
        deltaAggregate.set(value);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public Counter<Boolean> resetMeanToValue(long elementCount, Boolean value) {
      throw illegalArgumentException();
    }

    @Override
    public CounterMean<Boolean> getAndResetMeanDelta() {
      throw illegalArgumentException();
    }

    @Override
    public Boolean getAggregate() {
      return aggregate.get();
    }

    @Override
    @Nullable
    public CounterMean<Boolean> getMean() {
      throw illegalArgumentException();
    }

    @Override
    public Counter<Boolean> merge(Counter<Boolean> that) {
      try {
        checkArgument(
            this.isCompatibleWith(that), "Counters %s and %s are incompatible", this, that);
        return addValue(that.getAggregate());
      } finally {
        setDirty();
      }
    }
  }

  /**
   * Implements a {@link Counter} for {@link String} values.
   */
  private static class StringCounter extends Counter<String> {
    /** Initializes a new {@link Counter} for {@link String} values. */
    private StringCounter(CounterName name, AggregationKind kind) {
      super(name, kind);
      // TODO: Support MIN, MAX of Strings.
      throw illegalArgumentException();
    }

    @Override
    public StringCounter addValue(String value) {
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

    @Override
    public Counter<String> merge(Counter<String> that) {
      checkArgument(this.isCompatibleWith(that), "Counters %s and %s are incompatible", this, that);
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
    private final AtomicInteger aggregate;
    private final AtomicInteger deltaAggregate;
    private final AtomicReference<IntegerCounterMean> mean;
    private final AtomicReference<IntegerCounterMean> deltaMean;

    /** Initializes a new {@link Counter} for {@link Integer} values. */
    private IntegerCounter(CounterName name, AggregationKind kind) {
      super(name, kind);
      switch (kind) {
        case MEAN:
          aggregate = deltaAggregate = null;
          mean = new AtomicReference<>();
          deltaMean = new AtomicReference<>();
          getAndResetMeanDelta();
          mean.set(deltaMean.get());
          break;
        case SUM:
        case MAX:
        case MIN:
          mean = deltaMean = null;
          aggregate = new AtomicInteger();
          deltaAggregate = new AtomicInteger();
          getAndResetDelta();
          aggregate.set(deltaAggregate.get());
          break;
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public IntegerCounter addValue(Integer value) {
      try {
        switch (kind) {
          case SUM:
            aggregate.getAndAdd(value);
            deltaAggregate.getAndAdd(value);
            break;
          case MEAN:
            addToMeanAndSet(value, mean);
            addToMeanAndSet(value, deltaMean);
            break;
          case MAX:
            maxAndSet(value, aggregate);
            maxAndSet(value, deltaAggregate);
            break;
          case MIN:
            minAndSet(value, aggregate);
            minAndSet(value, deltaAggregate);
            break;
          default:
            throw illegalArgumentException();
        }
        return this;
      } finally {
        setDirty();
      }
    }

    private void addToMeanAndSet(int value, AtomicReference<IntegerCounterMean> target) {
      IntegerCounterMean current;
      IntegerCounterMean update;
      do {
        current = target.get();
        update = new IntegerCounterMean(current.getAggregate() + value, current.getCount() + 1);
      } while (!target.compareAndSet(current, update));
    }

    private void maxAndSet(int value, AtomicInteger target) {
      int current;
      int update;
      do {
        current = target.get();
        update = Math.max(value, current);
      } while (update > current && !target.compareAndSet(current, update));
    }

    private void minAndSet(int value, AtomicInteger target) {
      int current;
      int update;
      do {
        current = target.get();
        update = Math.min(value, current);
      } while (update < current && !target.compareAndSet(current, update));
    }

    @Override
    public Integer getAndResetDelta() {
      switch (kind) {
        case SUM:
          return deltaAggregate.getAndSet(0);
        case MAX:
          return deltaAggregate.getAndSet(Integer.MIN_VALUE);
        case MIN:
          return deltaAggregate.getAndSet(Integer.MAX_VALUE);
        default:
          throw illegalArgumentException();
      }
    }

    @Override
    public Counter<Integer> resetToValue(Integer value) {
      try {
        if (kind == MEAN) {
          throw illegalArgumentException();
        }
        aggregate.set(value);
        deltaAggregate.set(value);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public Counter<Integer> resetMeanToValue(long elementCount, Integer value) {
      try {
        if (kind != MEAN) {
          throw illegalArgumentException();
        }
        if (elementCount < 0) {
          throw new IllegalArgumentException("elementCount must be non-negative");
        }
        IntegerCounterMean counterMean = new IntegerCounterMean(value, elementCount);
        mean.set(counterMean);
        deltaMean.set(counterMean);
        return this;
      } finally {
        setDirty();
      }
    }

    @Override
    public CounterMean<Integer> getAndResetMeanDelta() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return deltaMean.getAndSet(new IntegerCounterMean(0, 0L));
    }

    @Override
    public Integer getAggregate() {
      if (kind != MEAN) {
        return aggregate.get();
      } else {
        return getMean().getAggregate();
      }
    }

    @Override
    @Nullable
    public CounterMean<Integer> getMean() {
      if (kind != MEAN) {
        throw illegalArgumentException();
      }
      return mean.get();
    }

    @Override
    public Counter<Integer> merge(Counter<Integer> that) {
      try {
        checkArgument(
            this.isCompatibleWith(that), "Counters %s and %s are incompatible", this, that);
        switch (kind) {
          case SUM:
          case MIN:
          case MAX:
            return addValue(that.getAggregate());
          case MEAN:
            CounterMean<Integer> thisCounterMean = this.getMean();
            CounterMean<Integer> thatCounterMean = that.getMean();
            return resetMeanToValue(
                thisCounterMean.getCount() + thatCounterMean.getCount(),
                thisCounterMean.getAggregate() + thatCounterMean.getAggregate());
          default:
            throw illegalArgumentException();
        }
      } finally {
        setDirty();
      }
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

      @Override
      public String toString() {
        return aggregate + "/" + count;
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
