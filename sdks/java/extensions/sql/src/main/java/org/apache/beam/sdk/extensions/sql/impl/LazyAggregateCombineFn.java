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
package org.apache.beam.sdk.extensions.sql.impl;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link org.apache.beam.sdk.transforms.Combine.CombineFn} that wraps an {@link AggregateFn}. The
 * {@link AggregateFn} is lazily instantiated so it doesn't have to be serialized/deserialized.
 */
public class LazyAggregateCombineFn<InputT, AccumT, OutputT>
    extends Combine.CombineFn<InputT, AccumT, OutputT> {
  private final List<String> functionPath;
  private final String jarPath;
  private transient @Nullable AggregateFn<InputT, AccumT, OutputT> aggregateFn = null;

  public LazyAggregateCombineFn(List<String> functionPath, String jarPath) {
    this.functionPath = functionPath;
    this.jarPath = jarPath;
  }

  @VisibleForTesting
  LazyAggregateCombineFn(AggregateFn aggregateFn) {
    this.functionPath = ImmutableList.of();
    this.jarPath = "";
    this.aggregateFn = aggregateFn;
  }

  private AggregateFn<InputT, AccumT, OutputT> getAggregateFn() {
    if (aggregateFn == null) {
      JavaUdfLoader loader = new JavaUdfLoader();
      aggregateFn = loader.loadAggregateFunction(functionPath, jarPath);
    }
    return aggregateFn;
  }

  @Override
  public AccumT createAccumulator() {
    return getAggregateFn().createAccumulator();
  }

  @Override
  public AccumT addInput(AccumT mutableAccumulator, InputT input) {
    return getAggregateFn().addInput(mutableAccumulator, input);
  }

  @Override
  public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
    AccumT first = accumulators.iterator().next();
    Iterable<AccumT> rest = new SkipFirstElementIterable<>(accumulators);
    return getAggregateFn().mergeAccumulators(first, rest);
  }

  @Override
  public OutputT extractOutput(AccumT accumulator) {
    return getAggregateFn().extractOutput(accumulator);
  }

  @Override
  public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
      throws CannotProvideCoderException {
    // Infer coder based on underlying AggregateFn instance.
    return registry.getCoder(
        getAggregateFn().getClass(),
        AggregateFn.class,
        ImmutableMap.<Type, Coder<?>>of(getInputTVariable(), inputCoder),
        getAccumTVariable());
  }

  @Override
  public TypeVariable<?> getAccumTVariable() {
    return AggregateFn.class.getTypeParameters()[1];
  }

  public UdafImpl getUdafImpl() {
    return new LazyUdafImpl<>(this);
  }

  @Override
  public String toString() {
    return String.format(
        "%s %s from jar %s",
        LazyAggregateCombineFn.class.getSimpleName(), String.join(".", functionPath), jarPath);
  }

  /** Wrapper {@link Iterable} which always skips its first element. */
  private static class SkipFirstElementIterable<T> implements Iterable<T> {
    private final Iterable<T> all;

    SkipFirstElementIterable(Iterable<T> all) {
      this.all = all;
    }

    @Override
    public Iterator<T> iterator() {
      Iterator<T> it = all.iterator();
      it.next();
      return it;
    }
  }

  /** {@link UdafImpl} that defers type inference to the underlying {@link AggregateFn}. */
  private static class LazyUdafImpl<InputT, AccumT, OutputT> extends UdafImpl {
    private final LazyAggregateCombineFn<InputT, AccumT, OutputT> lazyFn;

    public LazyUdafImpl(LazyAggregateCombineFn lazyFn) {
      super(lazyFn);
      this.lazyFn = lazyFn;
    }

    private Type[] getTypeArguments() {
      Class clazz = lazyFn.getAggregateFn().getClass();
      while (clazz != null) {
        for (Type genericInterface : clazz.getGenericInterfaces()) {
          if (genericInterface instanceof ParameterizedType) {
            ParameterizedType parameterizedType = ((ParameterizedType) genericInterface);
            if (parameterizedType.getRawType().equals(AggregateFn.class)) {
              return parameterizedType.getActualTypeArguments();
            }
          }
        }
        clazz = clazz.getSuperclass();
      }
      throw new IllegalStateException(
          String.format(
              "Cannot get type arguments for %s: must implement parameterized %s",
              lazyFn, AggregateFn.class.getSimpleName()));
    }

    @Override
    protected Type getInputType() {
      return getTypeArguments()[0];
    }

    @Override
    protected Type getOutputType() {
      return getTypeArguments()[2];
    }
  }
}
