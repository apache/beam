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
import java.util.Date;
import java.util.Iterator;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
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

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG.
 */
class BeamBuiltinAggregations {
  /**
   * {@link CombineFn} for MAX based on {@link Max} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createMax(SqlTypeName fieldType) {
    switch (fieldType) {
    case INTEGER:
      return Max.ofIntegers();
    case SMALLINT:
      return new Combine.BinaryCombineFn<Short>() {
        @Override
        public Short apply(Short left, Short right) {
          return left > right ? left : right;
        }
      };
    case TINYINT:
      return new Combine.BinaryCombineFn<Byte>() {
        @Override
        public Byte apply(Byte left, Byte right) {
          return left > right ? left : right;
        }
      };
    case BIGINT:
      return Max.ofLongs();
    case FLOAT:
      return new Combine.BinaryCombineFn<Float>() {
        @Override
        public Float apply(Float left, Float right) {
          return left > right ? left : right;
        }
      };
    case DOUBLE:
      return Max.ofDoubles();
    case TIMESTAMP:
      return new Combine.BinaryCombineFn<Date>() {
        @Override
        public Date apply(Date left, Date right) {
          return left.after(right) ? left : right;
        }
      };
    case DECIMAL:
      return new Combine.BinaryCombineFn<BigDecimal>() {
        @Override
        public BigDecimal apply(BigDecimal left, BigDecimal right) {
          return left.compareTo(right) > 0 ? left : right;
        }
      };
    default:
      throw new UnsupportedOperationException(
          String.format("[%s] is not support in MAX", fieldType));
  }
  }

  /**
   * {@link CombineFn} for MAX based on {@link Min} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createMin(SqlTypeName fieldType) {
    switch (fieldType) {
    case INTEGER:
      return Min.ofIntegers();
    case SMALLINT:
      return new Combine.BinaryCombineFn<Short>() {
        @Override
        public Short apply(Short left, Short right) {
          return left < right ? left : right;
        }
      };
    case TINYINT:
      return new Combine.BinaryCombineFn<Byte>() {
        @Override
        public Byte apply(Byte left, Byte right) {
          return left < right ? left : right;
        }
      };
    case BIGINT:
      return Min.ofLongs();
    case FLOAT:
      return new Combine.BinaryCombineFn<Float>() {
        @Override
        public Float apply(Float left, Float right) {
          return left < right ? left : right;
        }
      };
    case DOUBLE:
      return Min.ofDoubles();
    case TIMESTAMP:
      return new Combine.BinaryCombineFn<Date>() {
        @Override
        public Date apply(Date left, Date right) {
          return left.before(right) ? left : right;
        }
      };
    case DECIMAL:
      return new Combine.BinaryCombineFn<BigDecimal>() {
        @Override
        public BigDecimal apply(BigDecimal left, BigDecimal right) {
          return left.compareTo(right) < 0 ? left : right;
        }
      };
    default:
      throw new UnsupportedOperationException(
          String.format("[%s] is not support in MIN", fieldType));
  }
  }

  /**
   * {@link CombineFn} for MAX based on {@link Sum} and {@link Combine.BinaryCombineFn}.
   */
  public static CombineFn createSum(SqlTypeName fieldType) {
    switch (fieldType) {
    case INTEGER:
      return Sum.ofIntegers();
    case SMALLINT:
      return new Combine.BinaryCombineFn<Short>() {
        @Override
        public Short apply(Short left, Short right) {
          return (short) (left + right);
        }
      };
    case TINYINT:
      return new Combine.BinaryCombineFn<Byte>() {
        @Override
        public Byte apply(Byte left, Byte right) {
          return (byte) (left + right);
        }
      };
    case BIGINT:
      return Sum.ofLongs();
    case FLOAT:
      return new Combine.BinaryCombineFn<Float>() {
        @Override
        public Float apply(Float left, Float right) {
          return left + right;
        }
      };
    case DOUBLE:
      return Sum.ofDoubles();
    case DECIMAL:
      return new Combine.BinaryCombineFn<BigDecimal>() {
        @Override
        public BigDecimal apply(BigDecimal left, BigDecimal right) {
          return left.add(right);
        }
      };
    default:
      throw new UnsupportedOperationException(
          String.format("[%s] is not support in SUM", fieldType));
  }
  }

  /**
   * {@link CombineFn} for AVG.
   */
  public static CombineFn createAvg(SqlTypeName fieldType) {
    switch (fieldType) {
    case INTEGER:
      return new Avg<Integer>() {
        @Override
        public Integer extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).intValue();
        }

        @Override
        public BigDecimal toBigDecimal(Integer record) {
          return new BigDecimal(record);
        }
      };
    case SMALLINT:
      return new Avg<Short>() {
        @Override
        public Short extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).shortValue();
        }

        @Override
        public BigDecimal toBigDecimal(Short record) {
          return new BigDecimal(record);
        }
      };
    case TINYINT:
      return new Avg<Byte>() {
        @Override
        public Byte extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).byteValue();
        }

        @Override
        public BigDecimal toBigDecimal(Byte record) {
          return new BigDecimal(record);
        }
      };
    case BIGINT:
      return new Avg<Long>() {
        @Override
        public Long extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).longValue();
        }

        @Override
        public BigDecimal toBigDecimal(Long record) {
          return new BigDecimal(record);
        }
      };
    case FLOAT:
      return new Avg<Float>() {
        @Override
        public Float extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).floatValue();
        }

        @Override
        public BigDecimal toBigDecimal(Float record) {
          return new BigDecimal(record);
        }
      };
    case DOUBLE:
      return new Avg<Double>() {
        @Override
        public Double extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey())).doubleValue();
        }

        @Override
        public BigDecimal toBigDecimal(Double record) {
          return new BigDecimal(record);
        }
      };
    case DECIMAL:
      return new Avg<BigDecimal>() {
        @Override
        public BigDecimal extractOutput(KV<Integer, BigDecimal> accumulator) {
          return accumulator.getKey() == 0 ? null
              : accumulator.getValue().divide(new BigDecimal(accumulator.getKey()));
        }

        @Override
        public BigDecimal toBigDecimal(BigDecimal record) {
          return record;
        }
      };
    default:
      throw new UnsupportedOperationException(
          String.format("[%s] is not support in AVG", fieldType));
  }
  }

  /**
   * {@link CombineFn} for <em>AVG</em> on {@link Number} types.
   */
  public abstract static class Avg<T extends Number>
      extends CombineFn<T, KV<Integer, BigDecimal>, T> {
    @Override
    public KV<Integer, BigDecimal> createAccumulator() {
      return KV.of(0, new BigDecimal(0));
    }

    @Override
    public KV<Integer, BigDecimal> addInput(KV<Integer, BigDecimal> accumulator, T input) {
      return KV.of(accumulator.getKey() + 1, accumulator.getValue().add(toBigDecimal(input)));
    }

    @Override
    public KV<Integer, BigDecimal> mergeAccumulators(
        Iterable<KV<Integer, BigDecimal>> accumulators) {
      int size = 0;
      BigDecimal acc = new BigDecimal(0);
      Iterator<KV<Integer, BigDecimal>> ite = accumulators.iterator();
      while (ite.hasNext()) {
        KV<Integer, BigDecimal> ele = ite.next();
        size += ele.getKey();
        acc = acc.add(ele.getValue());
      }
      return KV.of(size, acc);
    }

    @Override
    public Coder<KV<Integer, BigDecimal>> getAccumulatorCoder(CoderRegistry registry,
        Coder<T> inputCoder) throws CannotProvideCoderException {
      return KvCoder.of(BigEndianIntegerCoder.of(), BigDecimalCoder.of());
    }

    public abstract T extractOutput(KV<Integer, BigDecimal> accumulator);
    public abstract BigDecimal toBigDecimal(T record);
  }
}
