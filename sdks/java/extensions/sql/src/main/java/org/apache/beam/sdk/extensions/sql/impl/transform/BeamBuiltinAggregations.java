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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.ReadableInstant;

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG/VAR_POP/VAR_SAMP.
 *
 * <p>TODO: Consider making the interface in terms of (1-column) rows. reuvenlax
 */
class BeamBuiltinAggregations {
  private static MathContext mc = new MathContext(10, RoundingMode.HALF_UP);

  /** {@link CombineFn} for MAX based on {@link Max} and {@link Combine.BinaryCombineFn}. */
  public static CombineFn createMax(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Max.ofIntegers();
      case SMALLINT:
        return new CustMax<Short>();
      case TINYINT:
        return new CustMax<Byte>();
      case BIGINT:
        return Max.ofLongs();
      case FLOAT:
        return new CustMax<Float>();
      case DOUBLE:
        return Max.ofDoubles();
      case TIMESTAMP:
        return new CustMax<ReadableInstant>();
      case DECIMAL:
        return new CustMax<BigDecimal>();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in MAX", fieldType));
    }
  }

  /** {@link CombineFn} for MAX based on {@link Min} and {@link Combine.BinaryCombineFn}. */
  public static CombineFn createMin(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Min.ofIntegers();
      case SMALLINT:
        return new CustMin<Short>();
      case TINYINT:
        return new CustMin<Byte>();
      case BIGINT:
        return Min.ofLongs();
      case FLOAT:
        return new CustMin<Float>();
      case DOUBLE:
        return Min.ofDoubles();
      case TIMESTAMP:
        return new CustMin<ReadableInstant>();
      case DECIMAL:
        return new CustMin<BigDecimal>();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in MIN", fieldType));
    }
  }

  /** {@link CombineFn} for MAX based on {@link Sum} and {@link Combine.BinaryCombineFn}. */
  public static CombineFn createSum(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return Sum.ofIntegers();
      case SMALLINT:
        return new ShortSum();
      case TINYINT:
        return new ByteSum();
      case BIGINT:
        return Sum.ofLongs();
      case FLOAT:
        return new FloatSum();
      case DOUBLE:
        return Sum.ofDoubles();
      case DECIMAL:
        return new BigDecimalSum();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in SUM", fieldType));
    }
  }

  /** {@link CombineFn} for AVG. */
  public static CombineFn createAvg(SqlTypeName fieldType) {
    switch (fieldType) {
      case INTEGER:
        return new IntegerAvg();
      case SMALLINT:
        return new ShortAvg();
      case TINYINT:
        return new ByteAvg();
      case BIGINT:
        return new LongAvg();
      case FLOAT:
        return new FloatAvg();
      case DOUBLE:
        return new DoubleAvg();
      case DECIMAL:
        return new BigDecimalAvg();
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in AVG", fieldType));
    }
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
    @Nullable
    public Integer extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).intValue();
    }

    @Override
    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class LongAvg extends Avg<Long> {
    @Override
    @Nullable
    public Long extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).longValue();
    }

    @Override
    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class ShortAvg extends Avg<Short> {
    @Override
    @Nullable
    public Short extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).shortValue();
    }

    @Override
    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteAvg extends Avg<Byte> {
    @Override
    @Nullable
    public Byte extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).byteValue();
    }

    @Override
    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class FloatAvg extends Avg<Float> {
    @Override
    @Nullable
    public Float extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).floatValue();
    }

    @Override
    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleAvg extends Avg<Double> {
    @Override
    @Nullable
    public Double extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalAvg extends Avg<BigDecimal> {
    @Override
    @Nullable
    public BigDecimal extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator);
    }

    @Override
    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }
}
