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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Static utility methods that create combine function instances. */
public class CombineFns {

  /**
   * Returns a {@link ComposeCombineFnBuilder} to construct a composed {@link GlobalCombineFn}.
   *
   * <p>The same {@link TupleTag} cannot be used in a composition multiple times.
   *
   * <p>Example:
   *
   * <pre>{@code  PCollection<Integer> globalLatencies = ...;
   *
   *  TupleTag<Integer> maxLatencyTag = new TupleTag<Integer>();
   *  TupleTag<Double> meanLatencyTag = new TupleTag<Double>();}
   *
   * {@code SimpleFunction<Integer, Integer> identityFn =
   *     new SimpleFunction<Integer, Integer>() }{
   *      {@code @Override
   *       public Integer apply(Integer input) {
   *           return input;
   *       }}};
   *
   * {@code PCollection<CoCombineResult> maxAndMean = globalLatencies.apply(
   *     Combine.globally(
   *         CombineFns.compose()
   *            .with(identityFn, new MaxIntegerFn(), maxLatencyTag)
   *            .with(identityFn, new MeanFn<Integer>(), meanLatencyTag)))};
   *
   * {@code PCollection<T> finalResultCollection = maxAndMean
   *     .apply(ParDo.of(
   *         new DoFn<CoCombineResult, T>() }{
   *            {@code @ProcessElement
   *             public void processElement(}
   *                  {@code @Element CoCombineResult e, OutputReceiver<T> r) throws Exception {
   *                 Integer maxLatency = e.get(maxLatencyTag);
   *                 Double meanLatency = e.get(meanLatencyTag);
   *                 .... Do Something ....
   *                 r.output(...some T...);
   *              }
   *         }}));
   * </pre>
   */
  public static ComposeCombineFnBuilder compose() {
    return new ComposeCombineFnBuilder();
  }

  /////////////////////////////////////////////////////////////////////////////

  /** A builder class to construct a composed {@link GlobalCombineFn}. */
  public static class ComposeCombineFnBuilder {
    /**
     * Returns a {@link ComposedCombineFn} that can take additional {@link GlobalCombineFn
     * GlobalCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedCombineFn} extracts inputs from {@code DataT} with the {@code
     * extractInputFn} and combines them with the {@code combineFn}, and then it outputs each
     * combined value with a {@link TupleTag} to a {@link CoCombineResult}.
     */
    public <DataT, InputT, OutputT> ComposedCombineFn<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFn<DataT>().with(extractInputFn, combineFn, outputTag);
    }

    /** Like {@link #with(SimpleFunction, CombineFn, TupleTag)} but with an explicit input coder. */
    public <DataT, InputT, OutputT> ComposedCombineFn<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        Coder combineInputCoder,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFn<DataT>()
          .with(extractInputFn, combineInputCoder, combineFn, outputTag);
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} that can take additional {@link
     * GlobalCombineFn GlobalCombineFns} and apply them as a single combine function.
     *
     * <p>The {@link ComposedCombineFnWithContext} extracts inputs from {@code DataT} with the
     * {@code extractInputFn} and combines them with the {@code combineFnWithContext}, and then it
     * outputs each combined value with a {@link TupleTag} to a {@link CoCombineResult}.
     */
    public <DataT, InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        CombineFnWithContext<InputT, ?, OutputT> combineFnWithContext,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFnWithContext<DataT>()
          .with(extractInputFn, combineFnWithContext, outputTag);
    }

    /** Like {@link #with(SimpleFunction, CombineFnWithContext, TupleTag)} but with input coder. */
    public <DataT, InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        Coder combineInputCoder,
        CombineFnWithContext<InputT, ?, OutputT> combineFnWithContext,
        TupleTag<OutputT> outputTag) {
      return new ComposedCombineFnWithContext<DataT>()
          .with(extractInputFn, combineInputCoder, combineFnWithContext, outputTag);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A tuple of outputs produced by a composed combine functions.
   *
   * <p>See {@link #compose()} for details.
   */
  public static class CoCombineResult implements Serializable {

    private enum NullValue {
      INSTANCE
    }

    private final Map<TupleTag<?>, Object> valuesMap;

    /**
     * The constructor of {@link CoCombineResult}.
     *
     * <p>Null values should have been filtered out from the {@code valuesMap}. {@link TupleTag
     * TupleTags} that associate with null values doesn't exist in the key set of {@code valuesMap}.
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
    public @Nullable <V> V get(TupleTag<V> tag) {
      checkArgument(
          valuesMap.keySet().contains(tag), "TupleTag " + tag + " is not in the CoCombineResult");
      Object value = valuesMap.get(tag);
      if (value == NullValue.INSTANCE) {
        return null;
      } else {
        return (V) value;
      }
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CoCombineResult that = (CoCombineResult) o;
      return Objects.equal(valuesMap, that.valuesMap);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(valuesMap);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A composed {@link CombineFn} that applies multiple {@link CombineFn CombineFns}.
   *
   * <p>For each {@link CombineFn} it extracts inputs from {@code DataT} with the {@code
   * extractInputFn} and combines them, and then it outputs each combined value with a {@link
   * TupleTag} to a {@link CoCombineResult}.
   */
  public static class ComposedCombineFn<DataT> extends CombineFn<DataT, Object[], CoCombineResult> {

    private final List<CombineFn<Object, Object, Object>> combineFns;
    private final List<Optional<Coder>> combineInputCoders;
    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedCombineFn() {
      this.extractInputFns = ImmutableList.of();
      this.combineInputCoders = ImmutableList.of();
      this.combineFns = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedCombineFn(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        List<Optional<Coder>> combineInputCoders,
        ImmutableList<CombineFn<?, ?, ?>> combineFns,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns = (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;
      this.combineInputCoders = combineInputCoders;

      @SuppressWarnings({"unchecked", "rawtypes"})
      List<CombineFn<Object, Object, Object>> castedCombineFns = (List) combineFns;
      this.combineFns = castedCombineFns;

      this.outputTags = outputTags;
      this.combineFnCount = this.combineFns.size();
    }

    /** Returns a {@link ComposedCombineFn} with an additional {@link CombineFn}. */
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
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.absent())
              .build(),
          ImmutableList.<CombineFn<?, ?, ?>>builder().addAll(combineFns).add(combineFn).build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} with an additional {@link
     * CombineFnWithContext}.
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
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.absent())
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(fnsWithContext)
              .add(combineFn)
              .build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
    }

    /** Returns a {@link ComposedCombineFn} with an additional {@link CombineFn}. */
    public <InputT, OutputT> ComposedCombineFn<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        Coder combineInputCoder,
        CombineFn<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedCombineFn<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.of(combineInputCoder))
              .build(),
          ImmutableList.<CombineFn<?, ?, ?>>builder().addAll(combineFns).add(combineFn).build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} with an additional {@link
     * CombineFnWithContext}.
     */
    public <InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        Coder combineInputCoder,
        CombineFnWithContext<InputT, ?, OutputT> combineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      List<CombineFnWithContext<Object, Object, Object>> fnsWithContext =
          combineFns.stream().map(CombineFnUtil::toFnWithContext).collect(Collectors.toList());

      return new ComposedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.of(combineInputCoder))
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(fnsWithContext)
              .add(combineFn)
              .build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
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
        valuesMap.put(outputTags.get(i), combineFns.get(i).extractOutput(accumulator[i]));
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
            combineInputCoders.get(i).isPresent()
                ? combineInputCoders.get(i).get()
                : registry.getOutputCoder(extractInputFns.get(i), dataCoder);
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
   * A composed {@link CombineFnWithContext} that applies multiple {@link CombineFnWithContext
   * CombineFnWithContexts}.
   *
   * <p>For each {@link CombineFnWithContext} it extracts inputs from {@code DataT} with the {@code
   * extractInputFn} and combines them, and then it outputs each combined value with a {@link
   * TupleTag} to a {@link CoCombineResult}.
   */
  public static class ComposedCombineFnWithContext<DataT>
      extends CombineFnWithContext<DataT, Object[], CoCombineResult> {

    private final List<SerializableFunction<DataT, Object>> extractInputFns;
    private final List<Optional<Coder>> combineInputCoders;
    private final List<CombineFnWithContext<Object, Object, Object>> combineFnWithContexts;
    private final List<TupleTag<?>> outputTags;
    private final int combineFnCount;

    private ComposedCombineFnWithContext() {
      this.extractInputFns = ImmutableList.of();
      this.combineInputCoders = ImmutableList.of();
      this.combineFnWithContexts = ImmutableList.of();
      this.outputTags = ImmutableList.of();
      this.combineFnCount = 0;
    }

    private ComposedCombineFnWithContext(
        ImmutableList<SerializableFunction<DataT, ?>> extractInputFns,
        ImmutableList<Optional<Coder>> combineInputCoders,
        ImmutableList<CombineFnWithContext<?, ?, ?>> combineFnWithContexts,
        ImmutableList<TupleTag<?>> outputTags) {
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SerializableFunction<DataT, Object>> castedExtractInputFns = (List) extractInputFns;
      this.extractInputFns = castedExtractInputFns;
      this.combineInputCoders = combineInputCoders;

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
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.absent())
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(combineFnWithContexts)
              .add(CombineFnUtil.toFnWithContext(globalCombineFn))
              .build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
    }

    /**
     * Returns a {@link ComposedCombineFnWithContext} with an additional {@link GlobalCombineFn}.
     */
    public <InputT, OutputT> ComposedCombineFnWithContext<DataT> with(
        SimpleFunction<DataT, InputT> extractInputFn,
        Coder<InputT> combineInputCoder,
        GlobalCombineFn<InputT, ?, OutputT> globalCombineFn,
        TupleTag<OutputT> outputTag) {
      checkUniqueness(outputTags, outputTag);
      return new ComposedCombineFnWithContext<>(
          ImmutableList.<SerializableFunction<DataT, ?>>builder()
              .addAll(extractInputFns)
              .add(extractInputFn)
              .build(),
          ImmutableList.<Optional<Coder>>builder()
              .addAll(combineInputCoders)
              .add(Optional.of(combineInputCoder))
              .build(),
          ImmutableList.<CombineFnWithContext<?, ?, ?>>builder()
              .addAll(combineFnWithContexts)
              .add(CombineFnUtil.toFnWithContext(globalCombineFn))
              .build(),
          ImmutableList.<TupleTag<?>>builder().addAll(outputTags).add(outputTag).build());
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
          accum[i] =
              combineFnWithContexts
                  .get(i)
                  .mergeAccumulators(new ProjectionIterable(accumulators, i), c);
        }
        return accum;
      }
    }

    @Override
    public CoCombineResult extractOutput(Object[] accumulator, Context c) {
      Map<TupleTag<?>, Object> valuesMap = Maps.newHashMap();
      for (int i = 0; i < combineFnCount; ++i) {
        valuesMap.put(
            outputTags.get(i), combineFnWithContexts.get(i).extractOutput(accumulator[i], c));
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
            combineInputCoders.get(i).isPresent()
                ? combineInputCoders.get(i).get()
                : registry.getOutputCoder(extractInputFns.get(i), dataCoder);
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

  private static class ComposedAccumulatorCoder extends StructuredCoder<Object[]> {
    private List<Coder<Object>> coders;
    private int codersCount;

    public ComposedAccumulatorCoder(List<Coder<Object>> coders) {
      this.coders = ImmutableList.copyOf(coders);
      this.codersCount = coders.size();
    }

    @Override
    public void encode(Object[] value, OutputStream outStream) throws CoderException, IOException {
      encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(Object[] value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      checkArgument(value.length == codersCount);
      if (value.length == 0) {
        return;
      }
      int lastIndex = codersCount - 1;
      for (int i = 0; i < lastIndex; ++i) {
        coders.get(i).encode(value[i], outStream);
      }
      coders.get(lastIndex).encode(value[lastIndex], outStream, context);
    }

    @Override
    public Object[] decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public Object[] decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      Object[] ret = new Object[codersCount];
      if (codersCount == 0) {
        return ret;
      }
      int lastIndex = codersCount - 1;
      for (int i = 0; i < lastIndex; ++i) {
        ret[i] = coders.get(i).decode(inStream);
      }
      ret[lastIndex] = coders.get(lastIndex).decode(inStream, context);
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
    for (int i = 0; i < combineFns.size(); i++) {
      HasDisplayData combineFn = combineFns.get(i);
      String token = "combineFn" + (i + 1);
      builder.add(DisplayData.item(token, combineFn.getClass()).withLabel("Combine Function"));
      builder.include(token, combineFn);
    }
  }
}
