/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark.aggregators;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class wraps a map of named aggregators. Spark expects that all accumulators be declared
 * before a job is launched. Dataflow allows aggregators to be used and incremented on the fly.
 * We create a map of named aggregators and instanyiate in the the spark context before the job
 * is launched. We can then add aggregators on the fly in Spark.
 */
public class NamedAggregators implements Serializable {
  /** Map from aggregator name to current state. */
  private final Map<String, State<?, ?, ?>> mNamedAggregators = new TreeMap<>();
  /** Constructs a new NamedAggregators instance. */
  public NamedAggregators() {
  }

  /**
   * Constructs a new named aggrgators instance that contains a mapping from the specified
   * `named` to the associated initial state.
   *
   * @param name Name of aggregator.
   * @param state Associated State.
   */
  public NamedAggregators(String name, State<?, ?, ?> state) {
    this.mNamedAggregators.put(name, state);
  }

  /**
   * Returns the value of the aggregator associated with the specified name.
   *
   * @param name Name of aggregator to retrieve.
   * @param typeClass Type class to cast the value to.
   * @param <T> Type to be returned.
   * @return
   */
  public <T> T getValue(String name, Class<T> typeClass) {
    return typeClass.cast(mNamedAggregators.get(name).render());
  }

  /**
   * Merges another NamedAggregators instance with this instance.
   *
   * @param other The other instance of named aggragtors ot merge.
   * @return This instance of Named aggragtors with associated states updated to reflect the
   *        other instance's aggregators.
   */
  public NamedAggregators merge(NamedAggregators other) {
    for (Map.Entry<String, State<?, ?, ?>> e : other.mNamedAggregators.entrySet()) {
      String key = e.getKey();
      State<?, ?, ?> otherValue = e.getValue();
      State<?, ?, ?> value = mNamedAggregators.get(key);
      if (value == null) {
        mNamedAggregators.put(key, otherValue);
      } else {
        mNamedAggregators.put(key, merge(value, otherValue));
      }
    }
    return this;
  }

  /**
   * Helper method to merge States whose generic types aren't provably the same,
   * so require some casting.
   */
  @SuppressWarnings("unchecked")
  private static <A, B, C> State<A, B, C> merge(State<?, ?, ?> s1, State<?, ?, ?> s2) {
    return ((State<A, B, C>) s1).merge((State<A, B, C>) s2);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, State<?, ?, ?>> e : mNamedAggregators.entrySet()) {
      sb.append(e.getKey()).append(": ").append(e.getValue().render());
    }
    return sb.toString();
  }

  /**
   * @param <In>    Input data type
   * @param <Inter> Intermediate data type (useful for averages)
   * @param <Out>   Output datatype
   */
  public interface State<In, Inter, Out> extends Serializable {
    /**
     * @param element
     */
    void update(In element);

    State<In, Inter, Out> merge(State<In, Inter, Out> other);

    Inter current();

    Out render();
  }

  /**
   * => combineFunction in data flow.
   *
   * @param <In>
   * @param <Inter>
   * @param <Out>
   */
  public static class CombineFunctionState<In, Inter, Out> implements State<In, Inter, Out> {

    private Combine.CombineFn<In, Inter, Out> combineFn;
    private Inter state;

    public CombineFunctionState(Combine.CombineFn<In, Inter, Out> combineFn) {
      this.combineFn = combineFn;
      this.state = combineFn.createAccumulator();
    }

    @Override
    public void update(In element) {
      combineFn.addInput(state, element);
    }

    @Override
    public State<In, Inter, Out> merge(State<In, Inter, Out> other) {
      this.state = combineFn.mergeAccumulators(ImmutableList.of(current(), other.current()));
      return this;
    }

    @Override
    public Inter current() {
      return state;
    }

    @Override
    public Out render() {
      return combineFn.extractOutput(state);
    }
  }

  /**
   * states correspond to dataflow objects. this one => serializable function
   *
   * @param <In>
   * @param <Out>
   */
  public static class SerFunctionState<In, Out> implements State<In, Out, Out> {

    private final SerializableFunction<Iterable<In>, Out> sfunc;
    private Out state;

    public SerFunctionState(SerializableFunction<Iterable<In>, Out> sfunc) {
      this.sfunc = sfunc;
      this.state = sfunc.apply(ImmutableList.<In>of());
    }

    @Override
    public void update(In element) {
      this.state = sfunc.apply(ImmutableList.of(element, (In) state));
    }

    @Override
    public State merge(State<In, Out, Out> other) {
      // Add exception catching and logging here.
      this.state = sfunc.apply(ImmutableList.of((In) state, (In) other.current()));
      return this;
    }

    @Override
    public Out current() {
      return state;
    }

    @Override
    public Out render() {
      return state;
    }
  }

}
