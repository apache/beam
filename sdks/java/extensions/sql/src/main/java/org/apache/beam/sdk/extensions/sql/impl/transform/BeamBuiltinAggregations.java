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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
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
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG/VAR_POP/VAR_SAMP. */
public class BeamBuiltinAggregations {

  public static final Map<String, Function<Schema.FieldType, CombineFn<?, ?, ?>>>
      BUILTIN_AGGREGATOR_FACTORIES =
          ImmutableMap.<String, Function<Schema.FieldType, CombineFn<?, ?, ?>>>builder()
              .put("ANY_VALUE", typeName -> Sample.anyValueCombineFn())
              .put("COUNT", typeName -> Count.combineFn())
              .put("MAX", BeamBuiltinAggregations::createMax)
              .put("MIN", BeamBuiltinAggregations::createMin)
              .put("SUM", BeamBuiltinAggregations::createSum)
              .put("$SUM0", BeamBuiltinAggregations::createSum)
              .put("AVG", BeamBuiltinAggregations::createAvg)
              .put("BIT_OR", BeamBuiltinAggregations::createBitOr)
              // JIRA link:https://issues.apache.org/jira/browse/BEAM-10379
              // .put("BIT_AND", BeamBuiltinAggregations::createBitAnd)
              .put("VAR_POP", t -> VarianceFn.newPopulation(t.getTypeName()))
              .put("VAR_SAMP", t -> VarianceFn.newSample(t.getTypeName()))
              .put("COVAR_POP", t -> CovarianceFn.newPopulation(t.getTypeName()))
              .put("COVAR_SAMP", t -> CovarianceFn.newSample(t.getTypeName()))
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
        return Sum.ofLongs();
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

  //  static CombineFn createBitAnd(Schema.FieldType fieldType) {
  //    if (fieldType.getTypeName() == TypeName.INT64) {
  //      return new BitAnd();
  //    }
  //    throw new UnsupportedOperationException(
  //        String.format("[%s] is not supported in BIT_AND", fieldType));
  //  }

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

  static class BigDecimalSum extends Combine.BinaryCombineFn<BigDecimal> {
    @Override
    public BigDecimal apply(BigDecimal left, BigDecimal right) {
      return left.add(right);
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

  static class BitOr<T extends Number> extends CombineFn<T, Long, Long> {
    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long accum, T input) {
      return accum | input.longValue();
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accums) {
      Long merged = createAccumulator();
      for (Long accum : accums) {
        merged = merged | accum;
      }
      return merged;
    }

    @Override
    public Long extractOutput(Long accum) {
      return accum;
    }
  }

  /**
   * NULL values don't work correctly. (https://issues.apache.org/jira/browse/BEAM-10379)
   *
   * <p>Comment the following implementation for BitAnd class for now.
   */
  //  static class BitAnd<T extends Number> extends CombineFn<T, Long, Long> {
  //    // Indicate if input only contains null value.
  //    private boolean isEmpty = true;
  //
  //    @Override
  //    public Long createAccumulator() {
  //      return -1L;
  //    }
  //
  //    @Override
  //    public Long addInput(Long accum, T input) {
  //      if (input != null) {
  //        this.isEmpty = false;
  //        return accum & input.longValue();
  //      } else {
  //        return null;
  //      }
  //    }
  //
  //    @Override
  //    public Long mergeAccumulators(Iterable<Long> accums) {
  //      Long merged = createAccumulator();
  //      for (Long accum : accums) {
  //        merged = merged & accum;
  //      }
  //      return merged;
  //    }
  //
  //    @Override
  //    public Long extractOutput(Long accum) {
  //      if (this.isEmpty) {
  //        return null;
  //      }
  //      return accum;
  //    }
  //  }

  //  static class BitAnd<T extends Number> extends CombineFn<T, BitAnd.Accum, Long> {
  //    public static class Accum {
  //      long val = -1L;
  //      boolean isEmpty = true;
  //      boolean seenNull = false;
  //    }
  //
  //    @Override
  //    public Accum createAccumulator() {
  //      return new Accum();
  //    }
  //
  //    @Override
  //    public Accum addInput(Accum accum, T input) {
  //      if (input == null) {
  //        accum.seenNull = true;
  //      } else {
  //        accum.isEmpty = false;
  //        accum.val = accum.val & input.longValue();
  //      }
  //      return accum;
  //    }
  //
  //    @Override
  //    public Accum mergeAccumulators(Iterable<Accum> accums) {
  //      Accum merged = createAccumulator();
  //      for (Accum accum : accums) {
  //        if (accum.isEmpty) {
  //          merged.isEmpty = true;
  //          break;
  //        }
  //        if (accum.seenNull) {
  //          merged.seenNull = true;
  //          break;
  //        }
  //        merged.val = merged.val & accum.val;
  //      }
  //      return merged;
  //    }
  //
  //    @Override
  //    @Nullable
  //    public Long extractOutput(Accum accum) {
  //      if (accum.isEmpty || accum.seenNull) {
  //        return null;
  //      }
  //      return accum.val;
  //    }
  //  }
}
