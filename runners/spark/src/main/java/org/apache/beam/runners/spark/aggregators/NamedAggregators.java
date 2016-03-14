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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.common.collect.ImmutableList;

import org.apache.beam.runners.spark.translation.SparkRuntimeContext;

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
   * @param <IN>    Input data type
   * @param <INTER> Intermediate data type (useful for averages)
   * @param <OUT>   Output data type
   */
  public interface State<IN, INTER, OUT> extends Serializable {
    /**
     * @param element new element to update state
     */
    void update(IN element);

    State<IN, INTER, OUT> merge(State<IN, INTER, OUT> other);

    INTER current();

    OUT render();

    Combine.CombineFn<IN, INTER, OUT> getCombineFn();
  }

  /**
   * =&gt; combineFunction in data flow.
   */
  public static class CombineFunctionState<IN, INTER, OUT> implements State<IN, INTER, OUT> {

    private Combine.CombineFn<IN, INTER, OUT> combineFn;
    private Coder<IN> inCoder;
    private SparkRuntimeContext ctxt;
    private transient INTER state;

    public CombineFunctionState(
        Combine.CombineFn<IN, INTER, OUT> combineFn,
        Coder<IN> inCoder,
        SparkRuntimeContext ctxt) {
      this.combineFn = combineFn;
      this.inCoder = inCoder;
      this.ctxt = ctxt;
      this.state = combineFn.createAccumulator();
    }

    @Override
    public void update(IN element) {
      combineFn.addInput(state, element);
    }

    @Override
    public State<IN, INTER, OUT> merge(State<IN, INTER, OUT> other) {
      this.state = combineFn.mergeAccumulators(ImmutableList.of(current(), other.current()));
      return this;
    }

    @Override
    public INTER current() {
      return state;
    }

    @Override
    public OUT render() {
      return combineFn.extractOutput(state);
    }

    @Override
    public Combine.CombineFn<IN, INTER, OUT> getCombineFn() {
      return combineFn;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
      oos.writeObject(ctxt);
      oos.writeObject(combineFn);
      oos.writeObject(inCoder);
      try {
        combineFn.getAccumulatorCoder(ctxt.getCoderRegistry(), inCoder)
            .encode(state, oos, Coder.Context.NESTED);
      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException("Could not determine coder for accumulator", e);
      }
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
      ctxt = (SparkRuntimeContext) ois.readObject();
      combineFn = (Combine.CombineFn<IN, INTER, OUT>) ois.readObject();
      inCoder = (Coder<IN>) ois.readObject();
      try {
        state = combineFn.getAccumulatorCoder(ctxt.getCoderRegistry(), inCoder)
            .decode(ois, Coder.Context.NESTED);
      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException("Could not determine coder for accumulator", e);
      }
    }
  }

}
