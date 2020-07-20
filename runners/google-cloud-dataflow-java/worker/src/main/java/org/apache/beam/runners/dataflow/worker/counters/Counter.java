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
package org.apache.beam.runners.dataflow.worker.counters;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterDistribution;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A Counter enables the aggregation of a stream of values over time. The cumulative aggregate value
 * is updated as new values are added. Multiple kinds of aggregation are supported depending on the
 * type of the counter.
 *
 * <p>After all possible mutations have completed, the reader should check {@link #isDirty} for
 * every counter, otherwise updates may be lost.
 *
 * <p>A counter may become dirty without a corresponding update to the value. This generally will
 * occur when the calls to {@code addValue()}, {@code committing()}, and {@code committed()} are
 * interleaved such that the value is updated between the calls to committing and the read of the
 * value.
 *
 * @param <InputT> the type of values aggregated by this counter.
 * @param <AccumT> the type of aggregator stored by this counter.
 */
public class Counter<InputT, AccumT> {

  /**
   * An instance of {@link AtomicCounterValue} specifies how a counter is aggregated and stored
   * internally. It should ensure that all operations are atomic when used in multiple threads.
   */
  public interface AtomicCounterValue<InputT, AccumT> {
    void addValue(InputT value);

    AccumT getAggregate();

    AccumT getAndReset();

    <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor);
  }

  /**
   * An instance of {@link CounterUpdateExtractor} specifies how a counter can be turned into an
   * appropriate update proto.
   *
   * <p>This class is essentially a visitor for counter values.
   */
  public interface CounterUpdateExtractor<UpdateT> {
    @Nullable
    UpdateT longSum(CounterName name, boolean delta, Long value);

    @Nullable
    UpdateT longMin(CounterName name, boolean delta, Long value);

    @Nullable
    UpdateT longMax(CounterName name, boolean delta, Long value);

    @Nullable
    UpdateT longMean(CounterName name, boolean delta, CounterMean<Long> value);

    @Nullable
    UpdateT intSum(CounterName name, boolean delta, Integer value);

    @Nullable
    UpdateT intMin(CounterName name, boolean delta, Integer value);

    @Nullable
    UpdateT intMax(CounterName name, boolean delta, Integer value);

    @Nullable
    UpdateT intMean(CounterName name, boolean delta, CounterMean<Integer> value);

    @Nullable
    UpdateT doubleSum(CounterName name, boolean delta, Double value);

    @Nullable
    UpdateT doubleMin(CounterName name, boolean delta, Double value);

    @Nullable
    UpdateT doubleMax(CounterName name, boolean delta, Double value);

    @Nullable
    UpdateT doubleMean(CounterName name, boolean delta, CounterMean<Double> value);

    @Nullable
    UpdateT boolOr(CounterName name, boolean delta, Boolean value);

    @Nullable
    UpdateT boolAnd(CounterName name, boolean delta, Boolean value);

    @Nullable
    UpdateT distribution(CounterName name, boolean delta, CounterDistribution value);
  }

  //////////////////////////////////////////////////////////////////////////////

  /** The name and metadata of this counter. */
  private final CounterName name;

  /** The commit state of this counter. */
  @VisibleForTesting final AtomicReference<CommitState> commitState;

  /** The implementation of the counter aggregation. */
  private final AtomicCounterValue<InputT, AccumT> internal;

  public Counter(CounterName name, AtomicCounterValue<InputT, AccumT> internal) {
    this.name = name;
    this.commitState = new AtomicReference<>(CommitState.COMMITTED);
    this.internal = internal;

    // Make sure the counter is properly initialized.
    internal.getAndReset();
  }

  /** Adds a new value to the aggregation stream. Returns this (to allow method chaining). */
  public Counter<InputT, AccumT> addValue(InputT value) {
    try {
      internal.addValue(value);
    } finally {
      setDirty();
    }
    return this;
  }

  /** Atomically resets the counter's value and returns the previous aggregate. */
  public AccumT getAndReset() {
    return internal.getAndReset();
  }

  /** Returns the counter's name. */
  public CounterName getName() {
    return name;
  }

  public <UpdateT> UpdateT extractUpdate(
      boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
    return internal.extractUpdate(name, delta, updateExtractor);
  }

  /** Returns the counter's flat name. This is only suitable for hashing purposes */
  public String getFlatName() {
    return name.getFlatName();
  }

  /** Returns the aggregated value. */
  public AccumT getAggregate() {
    return internal.getAggregate();
  }

  /**
   * Represents whether counters' values have been committed to the backend.
   *
   * <p>Runners can use this information to optimize counters updates. For example, if counters are
   * committed, runners may choose to skip the updates.
   *
   * <p>Counter state transition table: {@code Action\Current State COMMITTED DIRTY COMMITTING
   * addValue() DIRTY DIRTY DIRTY committing() None COMMITTING None committed() None None COMMITTED
   * }
   */
  @VisibleForTesting
  enum CommitState {
    /** There are no local updates that haven't been committed to the backend. */
    COMMITTED,
    /** There are local updates that haven't been committed to the backend. */
    DIRTY,
    /** Local updates are committing to the backend, but are still pending. */
    COMMITTING,
  }

  /** Returns if the counter contains non-committed aggregate. */
  public boolean isDirty() {
    return commitState.get() != CommitState.COMMITTED;
  }

  /**
   * Changes the counter from {@code CommitState.DIRTY} to {@code CommitState.COMMITTING}.
   *
   * @return true if successful. False return indicates that the commit state was not in {@code
   *     CommitState.DIRTY}.
   */
  public boolean committing() {
    return commitState.compareAndSet(CommitState.DIRTY, CommitState.COMMITTING);
  }

  /**
   * Changes the counter from {@code CommitState.COMMITTING} to {@code CommitState.COMMITTED}.
   *
   * @return true if successful.
   *     <p>False return indicates that the counter was updated while the committing is pending.
   *     That counter update might or might not has been committed. The {@code commitState} has to
   *     stay in {@code CommitState.DIRTY}.
   */
  public boolean committed() {
    return commitState.compareAndSet(CommitState.COMMITTING, CommitState.COMMITTED);
  }

  /**
   * Sets the counter to {@code CommitState.DIRTY}.
   *
   * <p>Must be called at the end of any methods that add values to the counter (specifically,
   * {@link #addValue}. This does not need to be called by {@link #getAndReset} because it doesn't
   * add any new values to the counter (just returns the current values).
   */
  protected void setDirty() {
    commitState.set(CommitState.DIRTY);
  }

  /**
   * Returns a string representation of the Counter. Useful for debugging logs. Example return
   * value: "ElementCount:SUM(15)".
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getFlatName());
    sb.append(":");
    sb.append(internal.getClass().getSimpleName());
    sb.append("(").append(getAggregate()).append(")");

    return sb.toString();
  }

  public String toPrettyString() {
    return name.getPrettyName();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    } else if (o instanceof Counter) {
      Counter<?, ?> that = (Counter<?, ?>) o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(internal.getClass(), that.internal.getClass());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(internal.getClass(), name);
  }
}
