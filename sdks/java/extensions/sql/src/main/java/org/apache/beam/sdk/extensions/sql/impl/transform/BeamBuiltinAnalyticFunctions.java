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
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;

/** Built-in Analytic Functions for the aggregation analytics functionality. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
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

  private static class FirstValueCombineFn<T> extends Combine.CombineFn<T, Optional<T>, T> {
    private FirstValueCombineFn() {}

    @Override
    public Optional<T> createAccumulator() {
      return Optional.empty();
    }

    @Override
    public Optional<T> addInput(Optional<T> accumulator, T input) {
      Optional<T> r = accumulator;
      if (!accumulator.isPresent()) {
        r = Optional.of(input);
      }
      return r;
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

  private static class LastValueCombineFn<T> extends Combine.CombineFn<T, Optional<T>, T> {
    private LastValueCombineFn() {}

    @Override
    public Optional<T> createAccumulator() {
      return Optional.empty();
    }

    @Override
    public Optional<T> addInput(Optional<T> accumulator, T input) {
      Optional<T> r = Optional.of(input);
      return r;
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
