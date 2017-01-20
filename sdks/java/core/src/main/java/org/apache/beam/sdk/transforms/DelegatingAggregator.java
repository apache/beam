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
package org.apache.beam.sdk.transforms;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * An {@link Aggregator} that delegates calls to {@link #addValue} to another aggregator.
 *
 * <p>This {@link Aggregator} is designed to be constructed without a delegate, at pipeline
 * construction time, and serialized within a {@link DoFn}. The delegate aggregator to which it
 * submits values must be provided by the runner at execution time.
 *
 * @param <AggInputT> the type of input element
 * @param <AggOutputT> the type of output element
 */
public class DelegatingAggregator<AggInputT, AggOutputT>
    implements Aggregator<AggInputT, AggOutputT>, Serializable {
  private static final AtomicInteger ID_GEN = new AtomicInteger();
  private final int id;

  private final String name;

  private final CombineFn<AggInputT, ?, AggOutputT> combineFn;

  private Aggregator<AggInputT, ?> delegate;

  public DelegatingAggregator(String name,
      CombineFn<? super AggInputT, ?, AggOutputT> combiner) {
    this.id = ID_GEN.getAndIncrement();
    this.name = checkNotNull(name, "name cannot be null");
    // Safe contravariant cast
    @SuppressWarnings("unchecked")
    CombineFn<AggInputT, ?, AggOutputT> specificCombiner =
        (CombineFn<AggInputT, ?, AggOutputT>) checkNotNull(combiner, "combineFn cannot be null");
    this.combineFn = specificCombiner;
  }

  @Override
  public void addValue(AggInputT value) {
    if (delegate == null) {
      throw new IllegalStateException(
          String.format(
              "addValue cannot be called on Aggregator outside of the execution of a %s.",
              DoFn.class.getSimpleName()));
    } else {
      delegate.addValue(value);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public CombineFn<AggInputT, ?, AggOutputT> getCombineFn() {
    return combineFn;
  }

  /**
   * Sets the current delegate of the Aggregator.
   *
   * @param delegate the delegate to set in this aggregator
   */
  public void setDelegate(Aggregator<AggInputT, ?> delegate) {
    this.delegate = delegate;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("name", name)
        .add("combineFn", combineFn)
        .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name, combineFn.getClass());
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * <p>{@code DelegatingAggregator} instances are equal if they have the same name, their
   * CombineFns are the same class, and they have identical IDs.
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof DelegatingAggregator) {
      DelegatingAggregator<?, ?> that = (DelegatingAggregator<?, ?>) o;
      return Objects.equals(this.id, that.id)
          && Objects.equals(this.name, that.name)
          && Objects.equals(this.combineFn.getClass(), that.combineFn.getClass());
    }
    return false;
  }
}
