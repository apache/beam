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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Built-in Analytic Functions for the aggregation analytics functionality. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamBuiltinAnalyticFunctions {
  public static final Map<String, Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>>>
      BUILTIN_ANALYTIC_FACTORIES =
          ImmutableMap.<String, Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>>>builder()
              // Aggregate Analytic Functions
              .putAll(BeamBuiltinAggregations.BUILTIN_AGGREGATOR_FACTORIES)
              // Navigation Functions
              .put("FIRST_VALUE", typeName -> navigationFirstValue())
              .put("LAST_VALUE", typeName -> navigationLastValue())
              // Numbering Functions
              .put("ROW_NUMBER", typeName -> numberingRowNumber())
              .put("DENSE_RANK", typeName -> numberingDenseRank())
              .put("RANK", typeName -> numberingRank())
              .put("PERCENT_RANK", typeName -> numberingPercentRank())
              .put("CUME_DIST", typeName -> numberingCumeDist())
              .build();

  public static Combine.CombineFn<?, ?, ?> create(String functionName, Schema.FieldType fieldType) {
    Function<Schema.FieldType, Combine.CombineFn<?, ?, ?>> aggregatorFactory =
        BUILTIN_ANALYTIC_FACTORIES.get(functionName);
    if (aggregatorFactory != null) {
      return aggregatorFactory.apply(fieldType);
    }
    throw new UnsupportedOperationException(
        String.format("Analytics Function [%s] is not supported", functionName));
  }

  // Navigation functions
  public static <T> Combine.CombineFn<T, ?, T> navigationFirstValue() {
    return new FirstValueCombineFn();
  }

  public static <T> Combine.CombineFn<T, ?, T> navigationLastValue() {
    return new LastValueCombineFn();
  }

  /**
   * FIRST_VALUE that optionally skips nulls. With {@code ignoreNulls=true} it returns the first
   * non-null value in the frame (matching Spark's {@code first(col, ignoreNulls=true)}, as emitted
   * by pandas ffill / bfill); otherwise it behaves like {@link #navigationFirstValue()}.
   */
  public static <T> Combine.CombineFn<T, ?, T> navigationFirstValue(boolean ignoreNulls) {
    return ignoreNulls ? new FirstValueIgnoreNullsCombineFn() : new FirstValueCombineFn();
  }

  /**
   * LAST_VALUE that optionally skips nulls. With {@code ignoreNulls=true} it returns the last
   * non-null value in the frame (matching Spark's {@code last(col, ignoreNulls=true)}); otherwise
   * it behaves like {@link #navigationLastValue()}.
   */
  public static <T> Combine.CombineFn<T, ?, T> navigationLastValue(boolean ignoreNulls) {
    return ignoreNulls ? new LastValueIgnoreNullsCombineFn() : new LastValueCombineFn();
  }

  /**
   * Accumulator for FIRST_VALUE / LAST_VALUE that, unlike {@link Optional}, can represent a {@code
   * null} value distinct from "no row seen yet". Plain {@code Optional.of(input)} throws an NPE
   * when a row's value is null, but FIRST_VALUE / LAST_VALUE (without IGNORE NULLS) must faithfully
   * return the first / last row's value even when that value is null.
   */
  private static class ValueHolder<T> {
    private boolean seen;
    private T value;

    void set(T input) {
      this.seen = true;
      this.value = input;
    }
  }

  private static class FirstValueCombineFn<T> extends Combine.CombineFn<T, ValueHolder<T>, T> {
    private FirstValueCombineFn() {}

    @Override
    public ValueHolder<T> createAccumulator() {
      return new ValueHolder<>();
    }

    @Override
    public ValueHolder<T> addInput(ValueHolder<T> accumulator, T input) {
      if (!accumulator.seen) {
        accumulator.set(input);
      }
      return accumulator;
    }

    @Override
    public ValueHolder<T> mergeAccumulators(Iterable<ValueHolder<T>> accumulators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public T extractOutput(ValueHolder<T> accumulator) {
      return accumulator.seen ? accumulator.value : null;
    }
  }

  private static class LastValueCombineFn<T> extends Combine.CombineFn<T, ValueHolder<T>, T> {
    private LastValueCombineFn() {}

    @Override
    public ValueHolder<T> createAccumulator() {
      return new ValueHolder<>();
    }

    @Override
    public ValueHolder<T> addInput(ValueHolder<T> accumulator, T input) {
      accumulator.set(input);
      return accumulator;
    }

    @Override
    public ValueHolder<T> mergeAccumulators(Iterable<ValueHolder<T>> accumulators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public T extractOutput(ValueHolder<T> accumulator) {
      return accumulator.seen ? accumulator.value : null;
    }
  }

  /** FIRST_VALUE(... IGNORE NULLS): the first non-null value in the frame. */
  private static class FirstValueIgnoreNullsCombineFn<T>
      extends Combine.CombineFn<T, Optional<T>, T> {
    private FirstValueIgnoreNullsCombineFn() {}

    @Override
    public Optional<T> createAccumulator() {
      return Optional.empty();
    }

    @Override
    public Optional<T> addInput(Optional<T> accumulator, T input) {
      if (!accumulator.isPresent() && input != null) {
        return Optional.of(input);
      }
      return accumulator;
    }

    @Override
    public Optional<T> mergeAccumulators(Iterable<Optional<T>> accumulators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public T extractOutput(Optional<T> accumulator) {
      return accumulator.isPresent() ? accumulator.get() : null;
    }
  }

  /** LAST_VALUE(... IGNORE NULLS): the last non-null value in the frame. */
  private static class LastValueIgnoreNullsCombineFn<T>
      extends Combine.CombineFn<T, Optional<T>, T> {
    private LastValueIgnoreNullsCombineFn() {}

    @Override
    public Optional<T> createAccumulator() {
      return Optional.empty();
    }

    @Override
    public Optional<T> addInput(Optional<T> accumulator, T input) {
      if (input != null) {
        return Optional.of(input);
      }
      return accumulator;
    }

    @Override
    public Optional<T> mergeAccumulators(Iterable<Optional<T>> accumulators) {
      throw new UnsupportedOperationException();
    }

    @Override
    public T extractOutput(Optional<T> accumulator) {
      return accumulator.isPresent() ? accumulator.get() : null;
    }
  }

  // Numbering functions
  public static <T> Combine.CombineFn<T, ?, T> numberingRowNumber() {
    return new RowNumberCombineFn();
  }

  public static <T> Combine.CombineFn<T, ?, T> numberingDenseRank() {
    return new DenseRankCombineFn();
  }

  public static <T> Combine.CombineFn<T, ?, T> numberingRank() {
    return new RankCombineFn();
  }

  public static <T> Combine.CombineFn<T, ?, T> numberingPercentRank() {
    return new PercentRankCombineFn();
  }

  public static <T> Combine.CombineFn<T, ?, T> numberingCumeDist() {
    return new CumeDistCombineFn();
  }

  public abstract static class PositionAwareCombineFn<InputT, AccumT, OutputT>
      extends Combine.CombineFn<InputT, AccumT, OutputT> {
    public abstract AccumT addInput(
        AccumT accumulator,
        InputT input,
        Long cursorWindow,
        Long cursorPartition,
        Long countPartition);

    @Override
    public AccumT addInput(AccumT mutableAccumulator, InputT input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      throw new UnsupportedOperationException();
    }
  }

  private static class RowNumberCombineFn<T>
      extends PositionAwareCombineFn<BigDecimal, Optional<Long>, Long> {

    @Override
    public Optional<Long> addInput(
        Optional<Long> accumulator,
        BigDecimal input,
        Long cursorPosition,
        Long cursorPartition,
        Long countPartition) {
      return Optional.of(cursorPartition);
    }

    @Override
    public Optional<Long> createAccumulator() {
      return Optional.empty();
    }

    @Override
    public Long extractOutput(Optional<Long> accumulator) {
      // 1-based result
      return accumulator.isPresent() ? accumulator.get() + 1 : null;
    }
  }

  private static class DenseRankCombineFn<T>
      extends PositionAwareCombineFn<BigDecimal, KV<BigDecimal, Long>, Long> {

    @Override
    public KV<BigDecimal, Long> addInput(
        KV<BigDecimal, Long> accumulator,
        BigDecimal input,
        Long cursorPosition,
        Long cursorPartition,
        Long countPartition) {
      KV<BigDecimal, Long> r = null;
      if (accumulator == null) {
        r = KV.of(input, 0L);
      } else {
        if (accumulator.getKey().compareTo(input) == 0) {
          r = KV.of(input, accumulator.getValue());
        } else {
          r = KV.of(input, accumulator.getValue() + 1);
        }
      }
      return r;
    }

    @Override
    public KV<BigDecimal, Long> createAccumulator() {
      return null;
    }

    @Override
    public Long extractOutput(KV<BigDecimal, Long> accumulator) {
      // 1-based result
      return accumulator != null ? accumulator.getValue() + 1 : null;
    }
  }

  private static class RankCombineFn<T>
      extends PositionAwareCombineFn<BigDecimal, KV<BigDecimal, Long>, Long> {

    @Override
    public KV<BigDecimal, Long> addInput(
        KV<BigDecimal, Long> accumulator,
        BigDecimal input,
        Long cursorPosition,
        Long cursorPartition,
        Long countPartition) {
      KV<BigDecimal, Long> r = null;
      if (accumulator == null) {
        r = KV.of(input, 0L);
      } else {
        if (accumulator.getKey().compareTo(input) == 0) {
          r = KV.of(input, accumulator.getValue());
        } else {
          r = KV.of(input, cursorPosition);
        }
      }
      return r;
    }

    @Override
    public KV<BigDecimal, Long> createAccumulator() {
      return null;
    }

    @Override
    public Long extractOutput(KV<BigDecimal, Long> accumulator) {
      // 1-based result
      return accumulator != null ? accumulator.getValue() + 1 : null;
    }
  }

  /**
   * CUME_DIST: the relative rank of the current row, computed as {@code (number of rows preceding
   * or peer with the current row) / (total rows in the partition)}.
   *
   * <p>Beam evaluates analytic functions over the per-row frame. With the SQL-standard default
   * frame for CUME_DIST (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), the frame handed to
   * this combiner already contains exactly the "preceding or peer" rows, so the numerator is simply
   * the number of {@code addInput} calls. The denominator is the partition size, which is supplied
   * to every {@code addInput} call as {@code countPartition}.
   */
  private static class CumeDistCombineFn<T>
      extends PositionAwareCombineFn<BigDecimal, KV<Long, Long>, Double> {

    @Override
    public KV<Long, Long> createAccumulator() {
      // KV(rowsInFrameSoFar, partitionSize)
      return KV.of(0L, 0L);
    }

    @Override
    public KV<Long, Long> addInput(
        KV<Long, Long> accumulator,
        BigDecimal input,
        Long cursorPosition,
        Long cursorPartition,
        Long countPartition) {
      return KV.of(accumulator.getKey() + 1L, countPartition);
    }

    @Override
    public Double extractOutput(KV<Long, Long> accumulator) {
      long total = accumulator.getValue();
      if (total <= 0L) {
        return 0.0;
      }
      return accumulator.getKey().doubleValue() / (double) total;
    }
  }

  private static class PercentRankCombineFn<T>
      extends PositionAwareCombineFn<BigDecimal, KV<Optional<Long>, KV<BigDecimal, Long>>, Double> {

    RankCombineFn internalRank;

    PercentRankCombineFn() {
      internalRank = new RankCombineFn();
    }

    @Override
    public KV<Optional<Long>, KV<BigDecimal, Long>> addInput(
        KV<Optional<Long>, KV<BigDecimal, Long>> accumulator,
        BigDecimal input,
        Long cursorPosition,
        Long cursorPartition,
        Long countPartition) {
      KV<BigDecimal, Long> ac1 =
          internalRank.addInput(
              accumulator.getValue(), input, cursorPosition, cursorPartition, countPartition);
      Optional<Long> ac2 = Optional.of(countPartition);
      return KV.of(ac2, ac1);
    }

    @Override
    public KV<Optional<Long>, KV<BigDecimal, Long>> createAccumulator() {
      return KV.of(Optional.empty(), internalRank.createAccumulator());
    }

    @Override
    public Double extractOutput(KV<Optional<Long>, KV<BigDecimal, Long>> accumulator) {
      Long nr = accumulator.getKey().orElse(null);
      Long rk = internalRank.extractOutput(accumulator.getValue());
      Double r = 0.0;
      if (nr != null && rk != null && nr > 1L) {
        r = (rk.doubleValue() - 1) / (nr.doubleValue() - 1);
      }
      return r;
    }
  }
}
