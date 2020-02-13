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
package org.apache.beam.runners.spark.aggregators;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/**
 * This class wraps a map of named aggregators. Spark expects that all accumulators be declared
 * before a job is launched. Beam allows aggregators to be used and incremented on the fly. We
 * create a map of named aggregators and instantiate in the the spark context before the job is
 * launched. We can then add aggregators on the fly in Spark.
 */
public class NamedAggregators implements Serializable {
  /** Map from aggregator name to current state. */
  private final Map<String, State<?, ?, ?>> mNamedAggregators = new TreeMap<>();

  /** Constructs a new NamedAggregators instance. */
  public NamedAggregators() {}

  /**
   * @param name Name of aggregator to retrieve.
   * @param typeClass Type class to cast the value to.
   * @param <T> Type to be returned.
   * @return the value of the aggregator associated with the specified name, or <code>null</code> if
   *     the specified aggregator could not be found.
   */
  public <T> T getValue(String name, Class<T> typeClass) {
    final State<?, ?, ?> state = mNamedAggregators.get(name);
    return state != null ? typeClass.cast(state.render()) : null;
  }

  /** @return a map of all the aggregator names and their <b>rendered </b>values */
  public Map<String, ?> renderAll() {
    return ImmutableMap.copyOf(Maps.transformValues(mNamedAggregators, State::render));
  }

  /**
   * Merges another NamedAggregators instance with this instance.
   *
   * @param other The other instance of named aggregators ot merge.
   * @return This instance of Named aggregators with associated states updated to reflect the other
   *     instance's aggregators.
   */
  public NamedAggregators merge(NamedAggregators other) {
    for (Map.Entry<String, State<?, ?, ?>> e : other.mNamedAggregators.entrySet()) {
      String key = e.getKey();
      State<?, ?, ?> otherValue = e.getValue();
      mNamedAggregators.merge(key, otherValue, NamedAggregators::merge);
    }
    return this;
  }

  /**
   * Helper method to merge States whose generic types aren't provably the same, so require some
   * casting.
   */
  @SuppressWarnings("unchecked")
  private static <InputT, InterT, OutputT> State<InputT, InterT, OutputT> merge(
      State<?, ?, ?> s1, State<?, ?, ?> s2) {
    return ((State<InputT, InterT, OutputT>) s1).merge((State<InputT, InterT, OutputT>) s2);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, State<?, ?, ?>> e : mNamedAggregators.entrySet()) {
      sb.append(e.getKey()).append(": ").append(e.getValue().render()).append(" ");
    }
    return sb.toString();
  }

  /**
   * @param <InputT> Input data type
   * @param <InterT> Intermediate data type (useful for averages)
   * @param <OutputT> Output data type
   */
  public interface State<InputT, InterT, OutputT> extends Serializable {

    /** @param element new element to update state */
    void update(InputT element);

    State<InputT, InterT, OutputT> merge(State<InputT, InterT, OutputT> other);

    InterT current();

    OutputT render();

    Combine.CombineFn<InputT, InterT, OutputT> getCombineFn();
  }
}
