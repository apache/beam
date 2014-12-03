/**
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
package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

public class Agg implements Serializable {

  public interface State<VI, VA, VO> extends Serializable {
    void update(VI vi);
    State<VI, VA, VO> merge(State<VI, VA, VO> other);
    VA current();
    VO render();
  }

  private final Map<String, State> states = new TreeMap<>();

  public Agg() {
  }

  public Agg(String named, State state) {
    this.states.put(named, state);
  }

  public Agg merge(Agg other) {
    for (Map.Entry<String, State> e : other.states.entrySet()) {
      State cur = states.get(e.getKey());
      if (cur == null) {
        states.put(e.getKey(), e.getValue());
      } else {
        states.put(e.getKey(), cur.merge(e.getValue()));
      }
    }
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, State> e : states.entrySet()) {
      sb.append(e.getKey()).append(": ").append(e.getValue().render());
    }
    return sb.toString();
  }

  public static class CombineState<VI, VA, VO> implements State<VI, VA, VO> {

    private Combine.CombineFn<VI, VA, VO> combineFn;
    private VA state;

    public CombineState(Combine.CombineFn<VI, VA, VO> combineFn) {
      this.combineFn = combineFn;
      this.state = combineFn.createAccumulator();
    }

    @Override
    public void update(VI vi) {
      combineFn.addInput(state, vi);
    }

    @Override
    public State<VI, VA, VO> merge(State<VI, VA, VO> other) {
      this.state = combineFn.mergeAccumulators(ImmutableList.of(current(), other.current()));
      return this;
    }

    @Override
    public VA current() {
      return state;
    }

    @Override
    public VO render() {
      return combineFn.extractOutput(state);
    }
  }

  public static class SerState<VI, VO> implements State<VI, VO, VO> {

    private SerializableFunction<Iterable<VI>, VO> sfunc;
    private VO state;

    public SerState(SerializableFunction<Iterable<VI>, VO> sfunc) {
      this.sfunc = sfunc;
    }

    @Override
    public void update(VI vi) {
      this.state = sfunc.apply(ImmutableList.of(vi, (VI) state));
    }

    @Override
    public State merge(State<VI, VO, VO> other) {
      this.state = sfunc.apply(ImmutableList.of((VI) state, (VI) other.current()));
      return this;
    }

    @Override
    public VO current() {
      return state;
    }

    @Override
    public VO render() {
      return state;
    }
  }

}
