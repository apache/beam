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

import com.cloudera.dataflow.spark.SparkRuntimeContext;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class wraps a map of named aggregators. Spark expects that all accumulators be declared
 * before a job is launched. Dataflow allows aggregators to be used and incremented on the fly.
 * We create a map of named aggregators and instantiate in the the spark context before the job
 * is launched. We can then add aggregators on the fly in Spark.
 */
public class NamedAggregators implements Serializable {
  /**
   * Map from aggregator name to current state.
   */
  private final Map<String, State<?, ?, ?>> mNamedAggregators = new TreeMap<>();

  /**
   * Constructs a new NamedAggregators instance.
   */
  public NamedAggregators() {
  }

  /**
   * Constructs a new named aggregators instance that contains a mapping from the specified
   * `named` to the associated initial state.
   *
   * @param name  Name of aggregator.
   * @param state Associated State.
   */
  public NamedAggregators(String name, State<?, ?, ?> state) {
    this.mNamedAggregators.put(name, state);
  }

  /**
   * @param name      Name of aggregator to retrieve.
   * @param typeClass Type class to cast the value to.
   * @param <T>       Type to be returned.
   * @return the value of the aggregator associated with the specified name
   */
  public <T> T getValue(String name, Class<T> typeClass) {
    return typeClass.cast(mNamedAggregators.get(name).render());
  }

  /**
   * Merges another NamedAggregators instance with this instance.
   *
   * @param other The other instance of named aggregators ot merge.
   * @return This instance of Named aggregators with associated states updated to reflect the
   * other instance's aggregators.
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
   * @param <Out>   Output data type
   */
  public interface State<In, Inter, Out> extends Serializable {
    /**
     * @param element new element to update state
     */
    void update(In element);

    State<In, Inter, Out> merge(State<In, Inter, Out> other);

    Inter current();

    Out render();
  }

  /**
   * =&gt; combineFunction in data flow.
   */
  public static class CombineFunctionState<In, Inter, Out> implements State<In, Inter, Out> {

    private Combine.CombineFn<In, Inter, Out> combineFn;
    private Coder<In> inCoder;
    private SparkRuntimeContext ctxt;
    private transient Inter state;

    public CombineFunctionState(
        Combine.CombineFn<In, Inter, Out> combineFn,
        Coder<In> inCoder,
        SparkRuntimeContext ctxt) {
      this.combineFn = combineFn;
      this.inCoder = inCoder;
      this.ctxt = ctxt;
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

    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.writeObject(ctxt);
      oos.writeObject(combineFn);
      oos.writeObject(inCoder);
      combineFn.getAccumulatorCoder(ctxt.getCoderRegistry(), inCoder).encode(state, oos, Coder.Context.NESTED);
    }

    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      ctxt = (SparkRuntimeContext) ois.readObject();
      combineFn = (Combine.CombineFn) ois.readObject();
      inCoder = (Coder) ois.readObject();
      state = combineFn.getAccumulatorCoder(ctxt.getCoderRegistry(), inCoder).decode(ois, Coder.Context.NESTED);
    }
  }

  /**
   * states correspond to dataflow objects. this one =&gt; serializable function
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
      @SuppressWarnings("unchecked")
      In thisState = (In) state;
      this.state = sfunc.apply(ImmutableList.of(element, thisState));
    }

    @Override
    public State<In, Out, Out> merge(State<In, Out, Out> other) {
      // Add exception catching and logging here.
      @SuppressWarnings("unchecked")
      In thisState = (In) state;
      @SuppressWarnings("unchecked")
      In otherCurrent = (In) other.current();
      this.state = sfunc.apply(ImmutableList.of(thisState, otherCurrent));
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
