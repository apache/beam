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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * This class wraps a map of named aggregators. Spark expects that all accumulators be declared
 * before a job is launched. Beam allows aggregators to be used and incremented on the fly.
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
   * @return the value of the aggregator associated with the specified name,
   * or <code>null</code> if the specified aggregator could not be found.
   */
  public <T> T getValue(String name, Class<T> typeClass) {
    final State<?, ?, ?> state = mNamedAggregators.get(name);
    return state != null ? typeClass.cast(state.render()) : null;
  }

  /**
   * @return a map of all the aggregator names and their <b>rendered </b>values
   */
  public Map<String, ?> renderAll() {
    return
        ImmutableMap.copyOf(
            Maps.transformValues(mNamedAggregators,
                new Function<State<?, ?, ?>, Object>() {

                  @Override
                  public Object apply(State<?, ?, ?> state) {
                    return state.render();
                  }
                }));
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
  private static <InputT, InterT, OutputT> State<InputT, InterT, OutputT> merge(
      State<?, ?, ?> s1,
      State<?, ?, ?> s2) {
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
   * @param <InputT>    Input data type
   * @param <InterT> Intermediate data type (useful for averages)
   * @param <OutputT>   Output data type
   */
  public interface State<InputT, InterT, OutputT> extends Serializable {

    /**
     * @param element new element to update state
     */
    void update(InputT element);

    State<InputT, InterT, OutputT> merge(State<InputT, InterT, OutputT> other);

    InterT current();

    OutputT render();

    Combine.CombineFn<InputT, InterT, OutputT> getCombineFn();
  }

  /**
   * @param <InputT> Input data type
   * @param <InterT> Intermediate data type (useful for averages)
   * @param <OutputT> Output data type
   */
  public static class CombineFunctionState<InputT, InterT, OutputT>
      implements State<InputT, InterT, OutputT> {

    private Combine.CombineFn<InputT, InterT, OutputT> combineFn;
    private Coder<InputT> inCoder;
    private SparkRuntimeContext ctxt;
    private transient InterT state;

    public CombineFunctionState(
        Combine.CombineFn<InputT, InterT, OutputT> combineFn,
        Coder<InputT> inCoder,
        SparkRuntimeContext ctxt) {
      this.combineFn = combineFn;
      this.inCoder = inCoder;
      this.ctxt = ctxt;
      this.state = combineFn.createAccumulator();
    }

    @Override
    public void update(InputT element) {
      combineFn.addInput(state, element);
    }

    @Override
    public State<InputT, InterT, OutputT> merge(State<InputT, InterT, OutputT> other) {
      this.state = combineFn.mergeAccumulators(ImmutableList.of(current(), other.current()));
      return this;
    }

    @Override
    public InterT current() {
      return state;
    }

    @Override
    public OutputT render() {
      return combineFn.extractOutput(state);
    }

    @Override
    public Combine.CombineFn<InputT, InterT, OutputT> getCombineFn() {
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
      combineFn = (Combine.CombineFn<InputT, InterT, OutputT>) ois.readObject();
      inCoder = (Coder<InputT>) ois.readObject();
      try {
        state = combineFn.getAccumulatorCoder(ctxt.getCoderRegistry(), inCoder)
            .decode(ois, Coder.Context.NESTED);
      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException("Could not determine coder for accumulator", e);
      }
    }
  }

}
