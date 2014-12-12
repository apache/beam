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

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.common.collect.ImmutableList;

/**
 * What is an Agg?
 * wrapper around a map of named aggregators.
 * This allows us to add a named aggregator on the fly.
 * we create an accumulable instance of aggs every time in the spark context.
 * When the dataflow
 */
public class NamedAggregators implements Serializable {
    /**
     * Why is this final if you later add states to it?
     */
    private final Map<String, State> mNamedAggregators = new TreeMap<>();

    public NamedAggregators() {
    }

    /**
     * is "named" the label for a state?
     *
     * @param named
     * @param state
     */
    public NamedAggregators(String named, State state) {
        this.mNamedAggregators.put(named, state);
    }

    /**
     * @param named
     * @param typeClass
     * @param <T>
     * @return
     */
    public <T> T getValue(String named, Class<T> typeClass) {
        return typeClass.cast(mNamedAggregators.get(named).render());
    }

    public NamedAggregators merge(NamedAggregators other) {
        for (Map.Entry<String, State> e : other.mNamedAggregators.entrySet()) {
            State cur = mNamedAggregators.get(e.getKey());
            if (cur == null) {
                mNamedAggregators.put(e.getKey(), e.getValue());
            } else {
                mNamedAggregators.put(e.getKey(), cur.merge(e.getValue()));
            }
        }
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, State> e : mNamedAggregators.entrySet()) {
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
     * states correspond to dataflow objects. this one => seriazable function
     *
     * @param <In>
     * @param <Out>
     */
    public static class SerFunctionState<In, Out> implements State<In, Out, Out> {

        private SerializableFunction<Iterable<In>, Out> sfunc;
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
