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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Combine.KeyedCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.PerKeyCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.CombineWithContext.KeyedCombineFnWithContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.values.TupleTag;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Static utility methods that create combine function instances.
 */
public class CombineFns {

  /**
   * Returns a {@link ComposeKeyedCombineFnBuilder} to construct a composed
   * {@link PerKeyCombineFn}.
   *
   * <p>The same {@link TupleTag} cannot be used in a composition multiple times.
   *
   * <p>Example:
   * <pre>{ @code
   * PCollection<KV<K, Integer>> latencies = ...;
   *
   * TupleTag<Integer> maxLatencyTag = new TupleTag<Integer>();
   * TupleTag<Double> meanLatencyTag = new TupleTag<Double>();
   *
   * SimpleFunction<Integer, Integer> identityFn =
   *     new SimpleFunction<Integer, Integer>() {
   *       @Override
   *       public Integer apply(Integer input) {
   *           return input;
   *       }};
   * PCollection<KV<K, CoCombineResult>> maxAndMean = latencies.apply(
   *     Combine.perKey(
   *         CombineFns.composeKeyed()
   *            .with(identityFn, new MaxIntegerFn(), maxLatencyTag)
   *            .with(identityFn, new MeanFn<Integer>(), meanLatencyTag)));
   *
   * PCollection<T> finalResultCollection = maxAndMean
   *     .apply(ParDo.of(
   *         new DoFn<KV<K, CoCombineResult>, T>() {
   *           @Override
   *           public void processElement(ProcessContext c) throws Exception {
   *             KV<K, CoCombineResult> e = c.element();
   *             Integer maxLatency = e.getValue().get(maxLatencyTag);
   *             Double meanLatency = e.getValue().get(meanLatencyTag);
   *             .... Do Something ....
   *             c.output(...some T...);
   *           }
   *         }));
   * } </pre>
   */
  public static ComposeKeyedCombineFnBuilder composeKeyed() {
    return new ComposeKeyedCombineFnBuilder();
  }

  /**
   * Returns a {@link ComposeCombineFnBuilder} to construct a composed
   * {@link GlobalCombineFn}.
   *
   * <p>The same {@link TupleTag} cannot be used in a composition multiple times.
   *
   * <p>Example:
   * <pre>{ @code
   * PCollection<Integer> globalLatencies = ...;
   *
   * TupleTag<Integer> maxLatencyTag = new TupleTag<Integer>();
   * TupleTag<Double> meanLatencyTag = new TupleTag<Double>();
   *
   * SimpleFunction<Integer, Integer> identityFn =
   *     new SimpleFunction<Integer, Integer>() {
   *       @Override
   *       public Integer apply(Integer input) {
   *           return input;
   *       }};
   * PCollection<CoCombineResult> maxAndMean = globalLatencies.apply(
   *     Combine.globally(
   *         CombineFns.compose()
   *            .with(identityFn, new MaxIntegerFn(), maxLatencyTag)
   *            .with(identityFn, new MeanFn<Integer>(), meanLatencyTag)));
   *
   * PCollection<T> finalResultCollection = maxAndMean
   *     .apply(ParDo.of(
   *         new DoFn<CoCombineResult, T>() {
   *           @Override
   *           public void processElement(ProcessContext c) throws Exception {
   *             CoCombineResult e = c.element();
   *             Integer maxLatency = e.get(maxLatencyTag);
   *             Double meanLatency = e.get(meanLatencyTag);
   *             .... Do Something ....
   *             c.output(...some T...);
   *           }
   *         }));
   * } </pre>
   */
  public static ComposeCombineFnBuilder compose() {
    return new ComposeCombineFnBuilder();
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A builder class to construct a composed {@link PerKeyCombineFn}.
   */
  public static class ComposeKeyedCombineFnBuilder {
    /**
     * Returns a {@link ComposedKeyedCombineFn} that can take additional
     * {@link PerKeyCombineFn PerKeyCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedKeyedCombineFn} extracts inputs from {@code DataT} with
     * the {@code extractInputFn} and combines them with the {@code keyedCombineFn},
     * and then it outputs each combined value with a {@link TupleTag} to a
     * {@link CoCombineResult}.
     */
    public <K, DataT, InputT, OutputT> ComposedKeyedCombineFn<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        KeyedCombineFn<K, InputT, ?, OutputT> keyedCombineFn,
        TupleTag<OutputT> outputTag) {
      return new ComposedKeyedCombineFn<DataT, K>()
          .with(extractInputFn, keyedCombineFn, outputTag);
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} that can take additional
     * {@link PerKeyCombineFn PerKeyCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedKeyedCombineFnWithContext} extracts inputs from {@code DataT} with
     * the {@code extractInputFn} and combines them with the {@code keyedCombineFnWithContext},
     * and then it outputs each combined value with a {@link TupleTag} to a
     * {@link CoCombineResult}.
     */
    public <K, DataT, InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        KeyedCombineFnWithContext<K, InputT, ?, OutputT> keyedCombineFnWithContext,
        TupleTag<OutputT> outputTag) {
      return new ComposedKeyedCombineFnWithContext<DataT, K>()
          .with(extractInputFn, keyedCombineFnWithContext, outputTag);
    }

    /**
     * Returns a {@link ComposedKeyedCombineFn} that can take additional
     * {@link PerKeyCombineFn PerKeyCombineFns} and apply them as a single combine function.
     */
    public <K, DataT, InputT, OutputT> ComposedKeyedCombineFn<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      return with(extractInputFn, combineFn.<K>asKeyedFn(), outputTag);
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} that can take additional
     * {@link PerKeyCombineFn PerKeyCombineFns} and apply them as a single combine function.
     */
    public <K, DataT, InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFnWithContext<InputT, ?, OutputT> combineFnWithContext,
        TupleTag<OutputT> outputTag) {
      return with(extractInputFn, combineFnWithContext.<K>asKeyedFn(), outputTag);
    }
  }

  /**
   * A builder class to construct a composed {@link GlobalCombineFn}.
   */
  public static class ComposeCombineFnBuilder {
    /**
     * Returns a {@link ComposedCombineFn} that can take additional
     * {@link GlobalCombineFn GlobalCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedCombineFn} extracts inputs from {@code DataT} with
     * the {@code extractInputFn} and combines them with the {@code combineFn},
     * and then it outputs each combined value with a {@link TupleTag} to a
     * {@link CoCombineResult}.
     */
    public <DataT, InputT, OutputT> ComposedCombineFn<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFn<DataT>()
          .with(extractInputFn, combineFn, outputTag);
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} that can take additional
     * {@link GlobalCombineFn GlobalCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedCombineFnWithContext} extracts inputs from {@code DataT} with
     * the {@code extractInputFn} and combines them with the {@code combineFnWithContext},
     * and then it outputs each combined value with a {@link TupleTag} to a
     * {@link CoCombineResult}.
     */
    public <DataT, InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFnWithContext<InputT, ?, OutputT> combineFnWithContext,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFnWithContext<DataT>()
          .with(extractInputFn, combineFnWithContext, outputTag);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A tuple of outputs produced by a composed combine functions.
   *
   * <p>See {@link #compose()} or {@link #composeKeyed()}) for details.
   */
  public static class CoCombineResult implements Serializable {

    private enum NullValue {
      INSTANCE;
    }

    private final Map<TupleTag<?>, Object> valuesMap;

    /**
     * The constructor of {@link CoCombineResult}.
     *
     * <p>Null values should have been filtered out from the {@code valuesMap}.
     * {@link TupleTag TupleTags} that associate with null values doesn't exist in the key set of
     * {@code valuesMap}.
     *
     * @throws NullPointerException if any key or value in {@code valuesMap} is null
     */
    CoCombineResult(Map<TupleTag<?>, Object> valuesMap) {
      ImmutableMap.Builder<TupleTag<?>, Object> builder = ImmutableMap.builder();
      for (Entry<TupleTag<?>, Object> entry : valuesMap.entrySet()) {
        if (entry.getValue() != null) {
          builder.put(entry);
        } else {
          builder.put(entry.getKey(), NullValue.INSTANCE);
        }
      }
      this.valuesMap = builder.build();
    }

    /**
     * Returns the value represented by the given {@link TupleTag}.
     *
     * <p>It is an error to request a non-exist tuple tag from the {@link CoCombineResult}.
     */
    @SuppressWarnings("unchecked")
    public <V> V get(TupleTag<V> tag) {
      checkArgument(
          valuesMap.keySet().contains(tag), "TupleTag " + tag + " is not in the CoCombineResult");
      Object value = valuesMap.get(tag);
      if (value == NullValue.INSTANCE) {
        return null;
      } else {
        return (V) value;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A composed {@link CombineFn} that applies multiple {@link CombineFn CombineFns}.
   *
   * <p>For each {@link CombineFn} it extracts inputs from {@code DataT} with
   * the {@code extractInputFn} and combines them,
   * and then it outputs each combined value with a {@link TupleTag} to a
   * {@link CoCombineResult}.
   */
  public static class ComposedCombineFn<DataT> extends CombineFn<DataT, Object[], CoCombineResult> {

    private final List<CombineFn<Object, Object, Object>> combineFns;
    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedCombineFn() {
      this.extractInputFns = ImmutableList.of();
      this.combineFns = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedCombineFn(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        ImmutableList<CombineFn<?, ?, ?>> combineFns,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns = (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;

      @SuppressWarnings({"unchecked", "rawtypes"})
      List<CombineFn<Object, Object, Object>> castedCombineFns = (List) combineFns;
      this.combineFns = castedCombineFns;

      this.outputTags = outputTags;
      this.combineFnCount = this.combineFns.size();
    }

    /**
     * Returns a {@link ComposedCombineFn} with an additional {@link CombineFn}.
     */
    public <InputT, OutputT> ComposedCombineFn<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedCombineFn<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<CombineFn<?, ?, ?>>builder()
              .addAll(combineFns)
              .add(combineFn)
              .build(),
          ImmutableList.<TupleTag<?>>builder()
              .addAll(outputTags)
              .add(outputTag)
              .build());
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} with an additional
     * {@link CombineFnWithContext}.
     */
    public <InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFnWithContext<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      List<CombineFnWithContext<Object, Object, Object>> fnsWithContext = Lists.newArrayList();
      for (CombineFn<Object, Object, Object> fn : combineFns) {
        fnsWithContext.add(CombineFnUtil.toFnWithContext(fn));
      }
      return new ComposedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(fnsWithContext)
              .add(combineFn)
              .build(),
          ImmutableList.<TupleTag<?>>builder()
              .addAll(outputTags)
              .add(outputTag)
              .build());
    }

    @Override
    public Object[] createAccumulator() {
      Object[] accumsArray = new Object[combineFnCount];
      for (int i = 0; i < combineFnCount; ++i) {
        accumsArray[i] = combineFns.get(i).createAccumulator();
      }
      return accumsArray;
    }

    @Override
    public Object[] addInput(Object[] accumulator, DataT value) {
      for (int i = 0; i < combineFnCount; ++i) {
        Object input = extractInputFns.get(i).apply(value);
        accumulator[i] = combineFns.get(i).addInput(accumulator[i], input);
      }
      return accumulator;
    }

    @Override
    public Object[] mergeAccumulators(Iterable<Object[]> accumulators) {
      Iterator<Object[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        // Reuses the first accumulator, and overwrites its values.
        // It is safe because {@code accum[i]} only depends on
        // the i-th component of each accumulator.
        Object[] accum = iter.next();
        for (int i = 0; i < combineFnCount; ++i) {
          accum[i] = combineFns.get(i).mergeAccumulators(new ProjectionIterable(accumulators, i));
        }
        return accum;
      }
    }

    @Override
    public CoCombineResult extractOutput(Object[] accumulator) {
      Map<TupleTag<?>, Object> valuesMap = Maps.newHashMap();
      for (int i = 0; i < combineFnCount; ++i) {
        valuesMap.put(
            outputTags.get(i),
            combineFns.get(i).extractOutput(accumulator[i]));
      }
      return new CoCombineResult(valuesMap);
    }

    @Override
    public Object[] compact(Object[] accumulator) {
      for (int i = 0; i < combineFnCount; ++i) {
        accumulator[i] = combineFns.get(i).compact(accumulator[i]);
      }
      return accumulator;
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(CoderRegistry registry, Coder<DataT> dataCoder)
        throws CannotProvideCoderException {
      List<Coder<Object>> coders = Lists.newArrayList();
      for (int i = 0; i < combineFnCount; ++i) {
        Coder<Object> inputCoder =
            registry.getDefaultOutputCoder(extractInputFns.get(i), dataCoder);
        coders.add(combineFns.get(i).getAccumulatorCoder(registry, inputCoder));
      }
      return new ComposedAccumulatorCoder(coders);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      CombineFns.populateDisplayData(builder, combineFns);
    }
  }

  /**
   * A composed {@link CombineFnWithContext} that applies multiple
   * {@link CombineFnWithContext CombineFnWithContexts}.
   *
   * <p>For each {@link CombineFnWithContext} it extracts inputs from {@code DataT} with
   * the {@code extractInputFn} and combines them,
   * and then it outputs each combined value with a {@link TupleTag} to a
   * {@link CoCombineResult}.
   */
  public static class ComposedCombineFnWithContext<DataT>
      extends CombineFnWithContext<DataT, Object[], CoCombineResult> {

    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<CombineFnWithContext<Object, Object, Object>> combineFnWithContexts;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedCombineFnWithContext() {
      this.extractInputFns = ImmutableList.of();
      this.combineFnWithContexts = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedCombineFnWithContext(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        ImmutableList<CombineFnWithContext<?, ?, ?>> combineFnWithContexts,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns =
          (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;

      @SuppressWarnings({"rawtypes", "unchecked"})
      List<CombineFnWithContext<Object, Object, Object>> castedCombineFnWithContexts =
          (List) combineFnWithContexts;
      this.combineFnWithContexts = castedCombineFnWithContexts;

      this.outputTags = outputTags;
      this.combineFnCount = this.combineFnWithContexts.size();
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} with an additional {@link GlobalCombineFn}.
     */
    public <InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        GlobalCombineFn<InputT, ?, OutputT> globalCombineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(combineFnWithContexts)
              .add(CombineFnUtil.toFnWithContext(globalCombineFn))
              .build(),
          ImmutableList.<TupleTag<?>>builder()
              .addAll(outputTags)
              .add(outputTag)
              .build());
    }

    @Override
    public Object[] createAccumulator(Context c) {
      Object[] accumsArray = new Object[combineFnCount];
      for (int i = 0; i < combineFnCount; ++i) {
        accumsArray[i] = combineFnWithContexts.get(i).createAccumulator(c);
      }
      return accumsArray;
    }

    @Override
    public Object[] addInput(Object[] accumulator, DataT value, Context c) {
      for (int i = 0; i < combineFnCount; ++i) {
        Object input = extractInputFns.get(i).apply(value);
        accumulator[i] = combineFnWithContexts.get(i).addInput(accumulator[i], input, c);
      }
      return accumulator;
    }

    @Override
    public Object[] mergeAccumulators(Iterable<Object[]> accumulators, Context c) {
      Iterator<Object[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator(c);
      } else {
        // Reuses the first accumulator, and overwrites its values.
        // It is safe because {@code accum[i]} only depends on
        // the i-th component of each accumulator.
        Object[] accum = iter.next();
        for (int i = 0; i < combineFnCount; ++i) {
          accum[i] = combineFnWithContexts.get(i).mergeAccumulators(
              new ProjectionIterable(accumulators, i), c);
        }
        return accum;
      }
    }

    @Override
    public CoCombineResult extractOutput(Object[] accumulator, Context c) {
      Map<TupleTag<?>, Object> valuesMap = Maps.newHashMap();
      for (int i = 0; i < combineFnCount; ++i) {
        valuesMap.put(
            outputTags.get(i),
            combineFnWithContexts.get(i).extractOutput(accumulator[i], c));
      }
      return new CoCombineResult(valuesMap);
    }

    @Override
    public Object[] compact(Object[] accumulator, Context c) {
      for (int i = 0; i < combineFnCount; ++i) {
        accumulator[i] = combineFnWithContexts.get(i).compact(accumulator[i], c);
      }
      return accumulator;
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(CoderRegistry registry, Coder<DataT> dataCoder)
        throws CannotProvideCoderException {
      List<Coder<Object>> coders = Lists.newArrayList();
      for (int i = 0; i < combineFnCount; ++i) {
        Coder<Object> inputCoder =
            registry.getDefaultOutputCoder(extractInputFns.get(i), dataCoder);
        coders.add(combineFnWithContexts.get(i).getAccumulatorCoder(registry, inputCoder));
      }
      return new ComposedAccumulatorCoder(coders);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      CombineFns.populateDisplayData(builder, combineFnWithContexts);
    }
  }

  /**
   * A composed {@link KeyedCombineFn} that applies multiple {@link KeyedCombineFn KeyedCombineFns}.
   *
   * <p>For each {@link KeyedCombineFn} it extracts inputs from {@code DataT} with
   * the {@code extractInputFn} and combines them,
   * and then it outputs each combined value with a {@link TupleTag} to a
   * {@link CoCombineResult}.
   */
  public static class ComposedKeyedCombineFn<DataT, K>
      extends KeyedCombineFn<K, DataT, Object[], CoCombineResult> {

    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<KeyedCombineFn<K, Object, Object, Object>> keyedCombineFns;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedKeyedCombineFn() {
      this.extractInputFns = ImmutableList.of();
      this.keyedCombineFns = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedKeyedCombineFn(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        ImmutableList<KeyedCombineFn<K, ?, ?, ?>> keyedCombineFns,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns = (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;

      @SuppressWarnings({"unchecked", "rawtypes"})
      List<KeyedCombineFn<K, Object, Object, Object>> castedKeyedCombineFns =
          (List) keyedCombineFns;
      this.keyedCombineFns = castedKeyedCombineFns;
      this.outputTags = outputTags;
      this.combineFnCount = this.keyedCombineFns.size();
    }

    /**
     * Returns a {@link ComposedKeyedCombineFn} with an additional {@link KeyedCombineFn}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFn<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        KeyedCombineFn<K, InputT, ?, OutputT> keyedCombineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedKeyedCombineFn<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
          .addAll(extractInputFns)
          .add(extractInputFn)
          .build(),
      ImmutableList.<KeyedCombineFn<K, ?, ?, ?>>builder()
          .addAll(keyedCombineFns)
          .add(keyedCombineFn)
          .build(),
      ImmutableList.<TupleTag<?>>builder()
          .addAll(outputTags)
          .add(outputTag)
          .build());
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} with an additional
     * {@link KeyedCombineFnWithContext}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        KeyedCombineFnWithContext<K, InputT, ?, OutputT> keyedCombineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      List<KeyedCombineFnWithContext<K, Object, Object, Object>> fnsWithContext =
          Lists.newArrayList();
      for (KeyedCombineFn<K, Object, Object, Object> fn : keyedCombineFns) {
        fnsWithContext.add(CombineFnUtil.toFnWithContext(fn));
      }
      return new ComposedKeyedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
          .addAll(extractInputFns)
          .add(extractInputFn)
          .build(),
      ImmutableList.<KeyedCombineFnWithContext<K, ?, ?, ?>>builder()
          .addAll(fnsWithContext)
          .add(keyedCombineFn)
          .build(),
      ImmutableList.<TupleTag<?>>builder()
          .addAll(outputTags)
          .add(outputTag)
          .build());
    }

    /**
     * Returns a {@link ComposedKeyedCombineFn} with an additional {@link CombineFn}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFn<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFn<InputT, ?, OutputT> keyedCombineFn,
        TupleTag<OutputT> outputTag) {
      return with(extractInputFn, keyedCombineFn.<K>asKeyedFn(), outputTag);
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} with an additional
     * {@link CombineFnWithContext}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFnWithContext<InputT, ?, OutputT> keyedCombineFn,
        TupleTag<OutputT> outputTag) {
      return with(extractInputFn, keyedCombineFn.<K>asKeyedFn(), outputTag);
    }

    @Override
    public Object[] createAccumulator(K key) {
      Object[] accumsArray = new Object[combineFnCount];
      for (int i = 0; i < combineFnCount; ++i) {
        accumsArray[i] = keyedCombineFns.get(i).createAccumulator(key);
      }
      return accumsArray;
    }

    @Override
    public Object[] addInput(K key, Object[] accumulator, DataT value) {
      for (int i = 0; i < combineFnCount; ++i) {
        Object input = extractInputFns.get(i).apply(value);
        accumulator[i] = keyedCombineFns.get(i).addInput(key, accumulator[i], input);
      }
      return accumulator;
    }

    @Override
    public Object[] mergeAccumulators(K key, final Iterable<Object[]> accumulators) {
      Iterator<Object[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator(key);
      } else {
        // Reuses the first accumulator, and overwrites its values.
        // It is safe because {@code accum[i]} only depends on
        // the i-th component of each accumulator.
        Object[] accum = iter.next();
        for (int i = 0; i < combineFnCount; ++i) {
          accum[i] = keyedCombineFns.get(i).mergeAccumulators(
              key, new ProjectionIterable(accumulators, i));
        }
        return accum;
      }
    }

    @Override
    public CoCombineResult extractOutput(K key, Object[] accumulator) {
      Map<TupleTag<?>, Object> valuesMap = Maps.newHashMap();
      for (int i = 0; i < combineFnCount; ++i) {
        valuesMap.put(
            outputTags.get(i),
            keyedCombineFns.get(i).extractOutput(key, accumulator[i]));
      }
      return new CoCombineResult(valuesMap);
    }

    @Override
    public Object[] compact(K key, Object[] accumulator) {
      for (int i = 0; i < combineFnCount; ++i) {
        accumulator[i] = keyedCombineFns.get(i).compact(key, accumulator[i]);
      }
      return accumulator;
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<DataT> dataCoder)
        throws CannotProvideCoderException {
      List<Coder<Object>> coders = Lists.newArrayList();
      for (int i = 0; i < combineFnCount; ++i) {
        Coder<Object> inputCoder =
            registry.getDefaultOutputCoder(extractInputFns.get(i), dataCoder);
        coders.add(keyedCombineFns.get(i).getAccumulatorCoder(registry, keyCoder, inputCoder));
      }
      return new ComposedAccumulatorCoder(coders);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      CombineFns.populateDisplayData(builder, keyedCombineFns);
    }
  }

  /**
   * A composed {@link KeyedCombineFnWithContext} that applies multiple
   * {@link KeyedCombineFnWithContext KeyedCombineFnWithContexts}.
   *
   * <p>For each {@link KeyedCombineFnWithContext} it extracts inputs from {@code DataT} with
   * the {@code extractInputFn} and combines them,
   * and then it outputs each combined value with a {@link TupleTag} to a
   * {@link CoCombineResult}.
   */
  public static class ComposedKeyedCombineFnWithContext<DataT, K>
      extends KeyedCombineFnWithContext<K, DataT, Object[], CoCombineResult> {

    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<KeyedCombineFnWithContext<K, Object, Object, Object>> keyedCombineFns;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedKeyedCombineFnWithContext() {
      this.extractInputFns = ImmutableList.of();
      this.keyedCombineFns = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedKeyedCombineFnWithContext(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        ImmutableList<KeyedCombineFnWithContext<K, ?, ?, ?>> keyedCombineFns,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns =
          (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;

      @SuppressWarnings({"unchecked", "rawtypes"})
      List<KeyedCombineFnWithContext<K, Object, Object, Object>> castedKeyedCombineFns =
          (List) keyedCombineFns;
      this.keyedCombineFns = castedKeyedCombineFns;
      this.outputTags = outputTags;
      this.combineFnCount = this.keyedCombineFns.size();
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} with an additional
     * {@link PerKeyCombineFn}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        PerKeyCombineFn<K, InputT, ?, OutputT> perKeyCombineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedKeyedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<KeyedCombineFnWithContext<K, ?, ?, ?>>builder()
              .addAll(keyedCombineFns)
              .add(CombineFnUtil.toFnWithContext(perKeyCombineFn))
              .build(),
          ImmutableList.<TupleTag<?>>builder()
              .addAll(outputTags)
              .add(outputTag)
              .build());
    }

    /**
     * Returns a {@link ComposedKeyedCombineFnWithContext} with an additional
     * {@link GlobalCombineFn}.
     */
    public <InputT, OutputT> ComposedKeyedCombineFnWithContext<DataT, K> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        GlobalCombineFn<InputT, ?, OutputT> perKeyCombineFn,
        TupleTag<OutputT> outputTag) {
      return with(extractInputFn, perKeyCombineFn.<K>asKeyedFn(), outputTag);
    }

    @Override
    public Object[] createAccumulator(K key, Context c) {
      Object[] accumsArray = new Object[combineFnCount];
      for (int i = 0; i < combineFnCount; ++i) {
        accumsArray[i] = keyedCombineFns.get(i).createAccumulator(key, c);
      }
      return accumsArray;
    }

    @Override
    public Object[] addInput(K key, Object[] accumulator, DataT value, Context c) {
      for (int i = 0; i < combineFnCount; ++i) {
        Object input = extractInputFns.get(i).apply(value);
        accumulator[i] = keyedCombineFns.get(i).addInput(key, accumulator[i], input, c);
      }
      return accumulator;
    }

    @Override
    public Object[] mergeAccumulators(K key, Iterable<Object[]> accumulators, Context c) {
      Iterator<Object[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator(key, c);
      } else {
        // Reuses the first accumulator, and overwrites its values.
        // It is safe because {@code accum[i]} only depends on
        // the i-th component of each accumulator.
        Object[] accum = iter.next();
        for (int i = 0; i < combineFnCount; ++i) {
          accum[i] = keyedCombineFns.get(i).mergeAccumulators(
              key, new ProjectionIterable(accumulators, i), c);
        }
        return accum;
      }
    }

    @Override
    public CoCombineResult extractOutput(K key, Object[] accumulator, Context c) {
      Map<TupleTag<?>, Object> valuesMap = Maps.newHashMap();
      for (int i = 0; i < combineFnCount; ++i) {
        valuesMap.put(
            outputTags.get(i),
            keyedCombineFns.get(i).extractOutput(key, accumulator[i], c));
      }
      return new CoCombineResult(valuesMap);
    }

    @Override
    public Object[] compact(K key, Object[] accumulator, Context c) {
      for (int i = 0; i < combineFnCount; ++i) {
        accumulator[i] = keyedCombineFns.get(i).compact(key, accumulator[i], c);
      }
      return accumulator;
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<DataT> dataCoder)
        throws CannotProvideCoderException {
      List<Coder<Object>> coders = Lists.newArrayList();
      for (int i = 0; i < combineFnCount; ++i) {
        Coder<Object> inputCoder =
            registry.getDefaultOutputCoder(extractInputFns.get(i), dataCoder);
        coders.add(keyedCombineFns.get(i).getAccumulatorCoder(
            registry, keyCoder, inputCoder));
      }
      return new ComposedAccumulatorCoder(coders);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      CombineFns.populateDisplayData(builder, keyedCombineFns);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  private static class ProjectionIterable implements Iterable<Object> {
    private final Iterable<Object[]> iterable;
    private final int column;

    private ProjectionIterable(Iterable<Object[]> iterable, int column) {
      this.iterable = iterable;
      this.column = column;
    }

    @Override
    public Iterator<Object> iterator() {
      final Iterator<Object[]> iter = iterable.iterator();
      return new Iterator<Object>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public Object next() {
          return iter.next()[column];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
      };
    }
  }

  private static class ComposedAccumulatorCoder extends StandardCoder<Object[]> {
    private List<Coder<Object>> coders;
    private int codersCount;

    public ComposedAccumulatorCoder(List<Coder<Object>> coders) {
      this.coders = ImmutableList.copyOf(coders);
      this.codersCount  = coders.size();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @JsonCreator
    public static ComposedAccumulatorCoder of(
        @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
        List<Coder<?>> components) {
      return new ComposedAccumulatorCoder((List) components);
    }

    @Override
    public void encode(Object[] value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      checkArgument(value.length == codersCount);
      Context nestedContext = context.nested();
      for (int i = 0; i < codersCount; ++i) {
        coders.get(i).encode(value[i], outStream, nestedContext);
      }
    }

    @Override
    public Object[] decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Object[] ret = new Object[codersCount];
      Context nestedContext = context.nested();
      for (int i = 0; i < codersCount; ++i) {
        ret[i] = coders.get(i).decode(inStream, nestedContext);
      }
      return ret;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return coders;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      for (int i = 0; i < codersCount; ++i) {
        coders.get(i).verifyDeterministic();
      }
    }
  }

  private static <OutputT> void checkUniqueness(
      List<TupleTag<?>> registeredTags, TupleTag<OutputT> outputTag) {
    checkArgument(
        !registeredTags.contains(outputTag),
        "Cannot compose with tuple tag %s because it is already present in the composition.",
        outputTag);
  }

  /**
   * Populate display data for the {@code combineFns} that make up a composed combine transform.
   *
   * <p>The same combineFn class may be used multiple times, in which case we must take special care
   * to register display data with unique namespaces.
   */
  private static void populateDisplayData(
      DisplayData.Builder builder, List<? extends HasDisplayData> combineFns) {

    // NB: ArrayListMultimap necessary to maintain ordering of combineFns of the same type.
    Multimap<Class<?>, HasDisplayData> combineFnMap = ArrayListMultimap.create();

    for (int i = 0; i < combineFns.size(); i++) {
      HasDisplayData combineFn = combineFns.get(i);
      builder.add(DisplayData.item("combineFn" + (i + 1), combineFn.getClass())
        .withLabel("Combine Function"));
      combineFnMap.put(combineFn.getClass(), combineFn);
    }

    for (Map.Entry<Class<?>, Collection<HasDisplayData>> combineFnEntries :
        combineFnMap.asMap().entrySet()) {

      Collection<HasDisplayData> classCombineFns = combineFnEntries.getValue();
      if (classCombineFns.size() == 1) {
        // Only one combineFn of this type, include it directly.
        builder.include(Iterables.getOnlyElement(classCombineFns));

      } else {
        // Multiple combineFns of same type, add a namespace suffix so display data is
        // unique and ordered.
        String baseNamespace = combineFnEntries.getKey().getName();
        for (int i = 0; i < combineFns.size(); i++) {
          HasDisplayData combineFn = combineFns.get(i);
          String namespace = String.format("%s#%d", baseNamespace, i + 1);
          builder.include(combineFn, namespace);
        }
      }
    }
  }
}
