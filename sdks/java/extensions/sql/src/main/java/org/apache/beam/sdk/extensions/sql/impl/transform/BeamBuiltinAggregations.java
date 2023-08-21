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
package org.apache.beam.sdk.extensions.sql.impl.transform;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.CountIf;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.CovarianceFn;
import org.apache.beam.sdk.extensions.sql.impl.transform.agg.VarianceFn;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG/VAR_POP/VAR_SAMP. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamBuiltinAggregations {

  public static final Map<String, Function<Schema.FieldType, CombineFn<?, ?, ?>>>
      BUILTIN_AGGREGATOR_FACTORIES =
          ImmutableMap.<String, Function<Schema.FieldType, CombineFn<?, ?, ?>>>builder()
              .put("ANY_VALUE", typeName -> Sample.anyValueCombineFn())
              // Drop null elements for these aggregations.
              .put("COUNT", typeName -> new DropNullFnWithDefault(Count.combineFn()))
              .put("MAX", typeName -> new DropNullFn(BeamBuiltinAggregations.createMax(typeName)))
              .put("MIN", typeName -> new DropNullFn(BeamBuiltinAggregations.createMin(typeName)))
              .put("SUM", typeName -> new DropNullFn(BeamBuiltinAggregations.createSum(typeName)))
              .put(
                  "$SUM0",
                  typeName ->
                      new DropNullFnWithDefault(BeamBuiltinAggregations.createSum0(typeName)))
              .put("AVG", typeName -> new DropNullFn(BeamBuiltinAggregations.createAvg(typeName)))
              .put(
                  "BIT_OR",
                  typeName -> new DropNullFn(BeamBuiltinAggregations.createBitOr(typeName)))
              .put(
                  "BIT_XOR",
                  typeName -> new DropNullFn(BeamBuiltinAggregations.createBitXOr(typeName)))
              // JIRA link:https://issues.apache.org/jira/browse/BEAM-10379
              .put(
                  "BIT_AND",
                  typeName -> new DropNullFn(BeamBuiltinAggregations.createBitAnd(typeName)))
              .put("VAR_POP", t -> VarianceFn.newPopulation(t.getTypeName()))
              .put("VAR_SAMP", t -> VarianceFn.newSample(t.getTypeName()))
              .put("COVAR_POP", t -> CovarianceFn.newPopulation(t.getTypeName()))
              .put("COVAR_SAMP", t -> CovarianceFn.newSample(t.getTypeName()))
              .put("COUNTIF", typeName -> CountIf.combineFn())
              .build();

  private static MathContext mc = new MathContext(10, RoundingMode.HALF_UP);

  public static CombineFn<?, ?, ?> create(String functionName, Schema.FieldType fieldType) {

    Function<Schema.FieldType, CombineFn<?, ?, ?>> aggregatorFactory =
        BUILTIN_AGGREGATOR_FACTORIES.get(functionName);

    if (aggregatorFactory != null) {
      return aggregatorFactory.apply(fieldType);
    }

    throw new UnsupportedOperationException(
        String.format("Aggregator [%s] is not supported", functionName));
  }

  /** {@link CombineFn} for MAX based on {@link Max} and {@link Combine.BinaryCombineFn}. */
  static CombineFn createMax(FieldType fieldType) {
    if (CalciteUtils.isDateTimeType(fieldType)) {
      return new CustMax<>();
    }
    switch (fieldType.getTypeName()) {
      case BOOLEAN:
      case INT16:
      case BYTE:
      case FLOAT:
      case DATETIME:
      case DECIMAL:
      case STRING:
        return new CustMax<>();
      case INT32:
        return Max.ofIntegers();
      case INT64:
        return Max.ofLongs();
      case DOUBLE:
        return Max.ofDoubles();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in MAX", fieldType));
    }
  }

  /** {@link CombineFn} for MIN based on {@link Min} and {@link Combine.BinaryCombineFn}. */
  static CombineFn createMin(Schema.FieldType fieldType) {
    if (CalciteUtils.isDateTimeType(fieldType)) {
      return new CustMin();
    }
    switch (fieldType.getTypeName()) {
      case BOOLEAN:
      case BYTE:
      case INT16:
      case FLOAT:
      case DATETIME:
      case DECIMAL:
      case STRING:
        return new CustMin();
      case INT32:
        return Min.ofIntegers();
      case INT64:
        return Min.ofLongs();
      case DOUBLE:
        return Min.ofDoubles();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in MIN", fieldType));
    }
  }

  /** {@link CombineFn} for Sum based on {@link Sum} and {@link Combine.BinaryCombineFn}. */
  static CombineFn createSum(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
        return Sum.ofIntegers();
      case INT16:
        return new ShortSum();
      case BYTE:
        return new ByteSum();
      case INT64:
        return new LongSum();
      case FLOAT:
        return new FloatSum();
      case DOUBLE:
        return Sum.ofDoubles();
      case DECIMAL:
        return new BigDecimalSum();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in SUM", fieldType));
    }
  }

  /**
   * {@link CombineFn} for Sum0 where sum of null returns 0 based on {@link Sum} and {@link
   * Combine.BinaryCombineFn}.
   */
  static CombineFn createSum0(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
        return new IntegerSum0();
      case INT16:
        return new ShortSum0();
      case BYTE:
        return new ByteSum0();
      case INT64:
        return new LongSum0();
      case FLOAT:
        return new FloatSum0();
      case DOUBLE:
        return new DoubleSum0();
      case DECIMAL:
        return new BigDecimalSum0();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in SUM0", fieldType));
    }
  }

  /** {@link CombineFn} for AVG. */
  static CombineFn createAvg(Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case INT32:
        return new IntegerAvg();
      case INT16:
        return new ShortAvg();
      case BYTE:
        return new ByteAvg();
      case INT64:
        return new LongAvg();
      case FLOAT:
        return new FloatAvg();
      case DOUBLE:
        return new DoubleAvg();
      case DECIMAL:
        return new BigDecimalAvg();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not supported in AVG", fieldType));
    }
  }

  static CombineFn createBitOr(Schema.FieldType fieldType) {
    if (fieldType.getTypeName() == TypeName.INT64) {
      return new BitOr();
    }
    throw new UnsupportedOperationException(
        String.format("[%s] is not supported in BIT_OR", fieldType));
  }

  static CombineFn createBitAnd(Schema.FieldType fieldType) {
    if (fieldType.getTypeName() == TypeName.INT64) {
      return new BitAnd();
    }
    throw new UnsupportedOperationException(
        String.format("[%s] is not supported in BIT_AND", fieldType));
  }

  public static CombineFn createBitXOr(Schema.FieldType fieldType) {
    if (fieldType.getTypeName() == TypeName.INT64) {
      return new BitXOr();
    }
    throw new UnsupportedOperationException(
        String.format("[%s] is not supported in BIT_XOR", fieldType));
  }

  static class CustMax<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    @Override
    public T apply(T left, T right) {
      return (right == null || right.compareTo(left) < 0) ? left : right;
    }
  }

  static class CustMin<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    @Override
    public T apply(T left, T right) {
      return (left == null || left.compareTo(right) < 0) ? left : right;
    }
  }

  static class IntegerSum extends Combine.BinaryCombineFn<Integer> {
    @Override
    public Integer apply(Integer left, Integer right) {
      return (int) (left + right);
    }
  }

  static class ShortSum extends Combine.BinaryCombineFn<Short> {
    @Override
    public Short apply(Short left, Short right) {
      return (short) (left + right);
    }
  }

  static class ByteSum extends Combine.BinaryCombineFn<Byte> {
    @Override
    public Byte apply(Byte left, Byte right) {
      return (byte) (left + right);
    }
  }

  static class FloatSum extends Combine.BinaryCombineFn<Float> {
    @Override
    public Float apply(Float left, Float right) {
      return left + right;
    }
  }

  static class DoubleSum extends Combine.BinaryCombineFn<Double> {
    @Override
    public Double apply(Double left, Double right) {
      return (double) left + right;
    }
  }

  static class LongSum extends Combine.BinaryCombineFn<Long> {
    @Override
    public Long apply(Long left, Long right) {
      return Math.addExact(left, right);
    }
  }

  static class BigDecimalSum extends Combine.BinaryCombineFn<BigDecimal> {
    @Override
    public BigDecimal apply(BigDecimal left, BigDecimal right) {
      return left.add(right);
    }
  }

  static class IntegerSum0 extends IntegerSum {
    @Override
    public @Nullable Integer identity() {
      return 0;
    }
  }

  static class ShortSum0 extends ShortSum {
    @Override
    public @Nullable Short identity() {
      return 0;
    }
  }

  static class ByteSum0 extends ByteSum {
    @Override
    public @Nullable Byte identity() {
      return 0;
    }
  }

  static class FloatSum0 extends FloatSum {
    @Override
    public @Nullable Float identity() {
      return 0F;
    }
  }

  static class DoubleSum0 extends DoubleSum {
    @Override
    public @Nullable Double identity() {
      return 0D;
    }
  }

  static class LongSum0 extends LongSum {
    @Override
    public @Nullable Long identity() {
      return 0L;
    }
  }

  static class BigDecimalSum0 extends BigDecimalSum {
    @Override
    public @Nullable BigDecimal identity() {
      return BigDecimal.ZERO;
    }
  }

  private static class DropNullFn<InputT, AccumT, OutputT>
      extends CombineFn<InputT, AccumT, OutputT> {
    protected final CombineFn<InputT, AccumT, OutputT> combineFn;

    DropNullFn(CombineFn<InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public AccumT createAccumulator() {
      return null;
    }

    @Override
    public AccumT addInput(AccumT accumulator, InputT input) {
      if (input == null) {
        return accumulator;
      }

      if (accumulator == null) {
        accumulator = combineFn.createAccumulator();
      }
      return combineFn.addInput(accumulator, input);
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      // filter out nulls
      accumulators = Iterables.filter(accumulators, Predicates.notNull());

      // handle only nulls
      if (!accumulators.iterator().hasNext()) {
        return null;
      }

      return combineFn.mergeAccumulators(accumulators);
    }

    @Override
    public OutputT extractOutput(AccumT accumulator) {
      if (accumulator == null) {
        return null;
      }
      return combineFn.extractOutput(accumulator);
    }

    @Override
    public Coder<AccumT> getAccumulatorCoder(CoderRegistry registry, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      Coder<AccumT> coder = combineFn.getAccumulatorCoder(registry, inputCoder);
      if (coder instanceof NullableCoder) {
        return coder;
      }
      return NullableCoder.of(coder);
    }
  }

  private static class DropNullFnWithDefault<InputT, AccumT, OutputT>
      extends DropNullFn<InputT, AccumT, OutputT> {

    DropNullFnWithDefault(CombineFn<InputT, AccumT, OutputT> combineFn) {
      super(combineFn);
    }

    @Override
    public AccumT createAccumulator() {
      return combineFn.createAccumulator();
    }
  }

  /** {@link CombineFn} for <em>AVG</em> on {@link Number} types. */
  abstract static class Avg<T extends Number> extends CombineFn<T, KV<Integer, BigDecimal>, T> {
    @Override
    public KV<Integer, BigDecimal> createAccumulator() {
      return KV.of(0, BigDecimal.ZERO);
    }

    @Override
    public KV<Integer, BigDecimal> addInput(KV<Integer, BigDecimal> accumulator, T input) {
      return KV.of(accumulator.getKey() + 1, accumulator.getValue().add(toBigDecimal(input)));
    }

    @Override
    public KV<Integer, BigDecimal> mergeAccumulators(
        Iterable<KV<Integer, BigDecimal>> accumulators) {
      int size = 0;
      BigDecimal acc = BigDecimal.ZERO;
      for (KV<Integer, BigDecimal> ele : accumulators) {
        size += ele.getKey();
        acc = acc.add(ele.getValue());
      }
      return KV.of(size, acc);
    }

    @Override
    public Coder<KV<Integer, BigDecimal>> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> inputCoder) {
      return KvCoder.of(BigEndianIntegerCoder.of(), BigDecimalCoder.of());
    }

    protected BigDecimal prepareOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getValue().divide(new BigDecimal(accumulator.getKey()), mc);
    }

    @Override
    public abstract T extractOutput(KV<Integer, BigDecimal> accumulator);

    public abstract BigDecimal toBigDecimal(T record);
  }

  static class IntegerAvg extends Avg<Integer> {
    @Override
    public @Nullable Integer extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).intValue();
    }

    @Override
    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class LongAvg extends Avg<Long> {
    @Override
    public @Nullable Long extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).longValue();
    }

    @Override
    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class ShortAvg extends Avg<Short> {
    @Override
    public @Nullable Short extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).shortValue();
    }

    @Override
    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteAvg extends Avg<Byte> {
    @Override
    public @Nullable Byte extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).byteValue();
    }

    @Override
    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class FloatAvg extends Avg<Float> {
    @Override
    public @Nullable Float extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).floatValue();
    }

    @Override
    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleAvg extends Avg<Double> {
    @Override
    public @Nullable Double extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalAvg extends Avg<BigDecimal> {
    @Override
    public @Nullable BigDecimal extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator);
    }

    @Override
    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }

  static class BitOr<T extends Number> extends CombineFn<T, BitOr.Accum, Long> {
    static class Accum implements Serializable {
      /** True if no inputs have been seen yet. */
      boolean isEmpty = true;
      /** The bitwise-or of the inputs seen so far. */
      long bitOr = 0L;
    }

    @Override
    public BitOr.Accum createAccumulator() {
      return new BitOr.Accum();
    }

    @Override
    public BitOr.Accum addInput(BitOr.Accum accum, T input) {
      accum.isEmpty = false;
      accum.bitOr |= input.longValue();
      return accum;
    }

    @Override
    public BitOr.Accum mergeAccumulators(Iterable<BitOr.Accum> accums) {
      BitOr.Accum merged = createAccumulator();
      for (BitOr.Accum accum : accums) {
        if (accum.isEmpty) {
          continue;
        }
        merged.isEmpty = false;
        merged.bitOr |= accum.bitOr;
      }
      return merged;
    }

    @Override
    public Long extractOutput(BitOr.Accum accum) {
      if (accum.isEmpty) {
        return null;
      }
      return accum.bitOr;
    }
  }

  /**
   * Bitwise AND function implementation.
   *
   * <p>Note: null values are ignored when mixed with non-null values.
   * (https://issues.apache.org/jira/browse/BEAM-10379)
   */
  static class BitAnd<T extends Number> extends CombineFn<T, BitAnd.Accum, Long> {
    static class Accum implements Serializable {
      /** True if no inputs have been seen yet. */
      boolean isEmpty = true;
      /** The bitwise-and of the inputs seen so far. */
      long bitAnd = -1L;
    }

    @Override
    public BitAnd.Accum createAccumulator() {
      return new BitAnd.Accum();
    }

    @Override
    public BitAnd.Accum addInput(BitAnd.Accum accum, T input) {
      accum.isEmpty = false;
      accum.bitAnd &= input.longValue();
      return accum;
    }

    @Override
    public BitAnd.Accum mergeAccumulators(Iterable<BitAnd.Accum> accums) {
      BitAnd.Accum merged = createAccumulator();
      for (BitAnd.Accum accum : accums) {
        if (accum.isEmpty) {
          continue;
        }
        merged.isEmpty = false;
        merged.bitAnd &= accum.bitAnd;
      }
      return merged;
    }

    @Override
    public Long extractOutput(BitAnd.Accum accum) {
      if (accum.isEmpty) {
        return null;
      }
      return accum.bitAnd;
    }
  }

  public static class BitXOr<T extends Number> extends CombineFn<T, BitXOr.Accum, Long> {

    static class Accum implements Serializable {
      /** True if no inputs have been seen yet. */
      boolean isEmpty = true;
      /** The bitwise-and of the inputs seen so far. */
      long bitXOr = 0L;
    }

    @Override
    public BitXOr.Accum createAccumulator() {
      return new BitXOr.Accum();
    }

    @Override
    public BitXOr.Accum addInput(Accum mutableAccumulator, T input) {
      if (input != null) {
        mutableAccumulator.isEmpty = false;
        mutableAccumulator.bitXOr ^= input.longValue();
      }
      return mutableAccumulator;
    }

    @Override
    public BitXOr.Accum mergeAccumulators(Iterable<BitXOr.Accum> accumulators) {
      BitXOr.Accum merged = createAccumulator();
      for (BitXOr.Accum accum : accumulators) {
        if (accum.isEmpty) {
          continue;
        }
        merged.isEmpty = false;
        merged.bitXOr ^= accum.bitXOr;
      }
      return merged;
    }

    @Override
    public Long extractOutput(BitXOr.Accum accumulator) {
      if (accumulator.isEmpty) {
        return null;
      }
      return accumulator.bitXOr;
    }
  }
}
