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
package org.apache.beam.dsls.sql.transform;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import org.apache.beam.dsls.sql.schema.BeamSqlUdaf;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.ByteCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG.
 */
class BeamBuiltinAggregations {
  /**
   * Built-in aggregation for COUNT.
   */
  public static final class Count<T> extends BeamSqlUdaf<T, Long, Long> {
    public Count() {}

    @Override
    public Long init() {
      return 0L;
    }

    @Override
    public Long add(Long accumulator, T input) {
      return accumulator + 1;
    }

    @Override
    public Long merge(Iterable<Long> accumulators) {
      long v = 0L;
      Iterator<Long> ite = accumulators.iterator();
      while (ite.hasNext()) {
        v += ite.next();
      }
      return v;
    }

    @Override
    public Long result(Long accumulator) {
      return accumulator;
    }
  }

  /**
   * Built-in aggregation for MAX.
   */
  public static final class Max<T extends Comparable<T>> extends BeamSqlUdaf<T, T, T> {
    public static Max create(SqlTypeName fieldType) {
      switch (fieldType) {
        case INTEGER:
          return new BeamBuiltinAggregations.Max<Integer>(fieldType);
        case SMALLINT:
          return new BeamBuiltinAggregations.Max<Short>(fieldType);
        case TINYINT:
          return new BeamBuiltinAggregations.Max<Byte>(fieldType);
        case BIGINT:
          return new BeamBuiltinAggregations.Max<Long>(fieldType);
        case FLOAT:
          return new BeamBuiltinAggregations.Max<Float>(fieldType);
        case DOUBLE:
          return new BeamBuiltinAggregations.Max<Double>(fieldType);
        case TIMESTAMP:
          return new BeamBuiltinAggregations.Max<Date>(fieldType);
        case DECIMAL:
          return new BeamBuiltinAggregations.Max<BigDecimal>(fieldType);
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in MAX", fieldType));
      }
    }

    private SqlTypeName fieldType;
    private Max(SqlTypeName fieldType) {
      this.fieldType = fieldType;
    }

    @Override
    public T init() {
      return null;
    }

    @Override
    public T add(T accumulator, T input) {
      return (accumulator == null || accumulator.compareTo(input) < 0) ? input : accumulator;
    }

    @Override
    public T merge(Iterable<T> accumulators) {
      Iterator<T> ite = accumulators.iterator();
      T mergedV = ite.next();
      while (ite.hasNext()) {
        T v = ite.next();
        mergedV = mergedV.compareTo(v) > 0 ? mergedV : v;
      }
      return mergedV;
    }

    @Override
    public T result(T accumulator) {
      return accumulator;
    }

    @Override
    public Coder<T> getAccumulatorCoder(CoderRegistry registry) throws CannotProvideCoderException {
      switch (fieldType) {
        case INTEGER:
          return (Coder<T>) VarIntCoder.of();
        case SMALLINT:
          return (Coder<T>) SerializableCoder.of(Short.class);
        case TINYINT:
          return (Coder<T>) ByteCoder.of();
        case BIGINT:
          return (Coder<T>) VarLongCoder.of();
        case FLOAT:
          return (Coder<T>) SerializableCoder.of(Float.class);
        case DOUBLE:
          return (Coder<T>) DoubleCoder.of();
        case TIMESTAMP:
          return (Coder<T>) SerializableCoder.of(Date.class);
        case DECIMAL:
          return (Coder<T>) BigDecimalCoder.of();
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in MAX", fieldType));
      }
    }
  }

  /**
   * Built-in aggregation for MIN.
   */
  public static final class Min<T extends Comparable<T>> extends BeamSqlUdaf<T, T, T> {
    public static Min create(SqlTypeName fieldType) {
      switch (fieldType) {
        case INTEGER:
          return new BeamBuiltinAggregations.Min<Integer>(fieldType);
        case SMALLINT:
          return new BeamBuiltinAggregations.Min<Short>(fieldType);
        case TINYINT:
          return new BeamBuiltinAggregations.Min<Byte>(fieldType);
        case BIGINT:
          return new BeamBuiltinAggregations.Min<Long>(fieldType);
        case FLOAT:
          return new BeamBuiltinAggregations.Min<Float>(fieldType);
        case DOUBLE:
          return new BeamBuiltinAggregations.Min<Double>(fieldType);
        case TIMESTAMP:
          return new BeamBuiltinAggregations.Min<Date>(fieldType);
        case DECIMAL:
          return new BeamBuiltinAggregations.Min<BigDecimal>(fieldType);
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in MIN", fieldType));
      }
    }

    private SqlTypeName fieldType;
    private Min(SqlTypeName fieldType) {
      this.fieldType = fieldType;
    }

    @Override
    public T init() {
      return null;
    }

    @Override
    public T add(T accumulator, T input) {
      return (accumulator == null || accumulator.compareTo(input) > 0) ? input : accumulator;
    }

    @Override
    public T merge(Iterable<T> accumulators) {
      Iterator<T> ite = accumulators.iterator();
      T mergedV = ite.next();
      while (ite.hasNext()) {
        T v = ite.next();
        mergedV = mergedV.compareTo(v) < 0 ? mergedV : v;
      }
      return mergedV;
    }

    @Override
    public T result(T accumulator) {
      return accumulator;
    }

    @Override
    public Coder<T> getAccumulatorCoder(CoderRegistry registry) throws CannotProvideCoderException {
      switch (fieldType) {
        case INTEGER:
          return (Coder<T>) VarIntCoder.of();
        case SMALLINT:
          return (Coder<T>) SerializableCoder.of(Short.class);
        case TINYINT:
          return (Coder<T>) ByteCoder.of();
        case BIGINT:
          return (Coder<T>) VarLongCoder.of();
        case FLOAT:
          return (Coder<T>) SerializableCoder.of(Float.class);
        case DOUBLE:
          return (Coder<T>) DoubleCoder.of();
        case TIMESTAMP:
          return (Coder<T>) SerializableCoder.of(Date.class);
        case DECIMAL:
          return (Coder<T>) BigDecimalCoder.of();
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in MIN", fieldType));
      }
    }
  }

  /**
   * Built-in aggregation for SUM.
   */
  public static final class Sum<T> extends BeamSqlUdaf<T, BigDecimal, T> {
    public static Sum create(SqlTypeName fieldType) {
      switch (fieldType) {
        case INTEGER:
          return new BeamBuiltinAggregations.Sum<Integer>(fieldType);
        case SMALLINT:
          return new BeamBuiltinAggregations.Sum<Short>(fieldType);
        case TINYINT:
          return new BeamBuiltinAggregations.Sum<Byte>(fieldType);
        case BIGINT:
          return new BeamBuiltinAggregations.Sum<Long>(fieldType);
        case FLOAT:
          return new BeamBuiltinAggregations.Sum<Float>(fieldType);
        case DOUBLE:
          return new BeamBuiltinAggregations.Sum<Double>(fieldType);
        case TIMESTAMP:
          return new BeamBuiltinAggregations.Sum<Date>(fieldType);
        case DECIMAL:
          return new BeamBuiltinAggregations.Sum<BigDecimal>(fieldType);
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in SUM", fieldType));
      }
    }

    private SqlTypeName fieldType;
      private Sum(SqlTypeName fieldType) {
        this.fieldType = fieldType;
      }

    @Override
    public BigDecimal init() {
      return new BigDecimal(0);
    }

    @Override
    public BigDecimal add(BigDecimal accumulator, T input) {
      return accumulator.add(new BigDecimal(input.toString()));
    }

    @Override
    public BigDecimal merge(Iterable<BigDecimal> accumulators) {
      BigDecimal v = new BigDecimal(0);
      Iterator<BigDecimal> ite = accumulators.iterator();
      while (ite.hasNext()) {
        v = v.add(ite.next());
      }
      return v;
    }

    @Override
    public T result(BigDecimal accumulator) {
      Object result = null;
      switch (fieldType) {
        case INTEGER:
          result = accumulator.intValue();
          break;
        case BIGINT:
          result = accumulator.longValue();
          break;
        case SMALLINT:
          result = accumulator.shortValue();
          break;
        case TINYINT:
          result = accumulator.byteValue();
          break;
        case DOUBLE:
          result = accumulator.doubleValue();
          break;
        case FLOAT:
          result = accumulator.floatValue();
          break;
        case DECIMAL:
          result = accumulator;
          break;
        default:
          break;
      }
      return (T) result;
    }
  }

  /**
   * Built-in aggregation for AVG.
   */
  public static final class Avg<T> extends BeamSqlUdaf<T, KV<BigDecimal, Long>, T> {
    public static Avg create(SqlTypeName fieldType) {
      switch (fieldType) {
        case INTEGER:
          return new BeamBuiltinAggregations.Avg<Integer>(fieldType);
        case SMALLINT:
          return new BeamBuiltinAggregations.Avg<Short>(fieldType);
        case TINYINT:
          return new BeamBuiltinAggregations.Avg<Byte>(fieldType);
        case BIGINT:
          return new BeamBuiltinAggregations.Avg<Long>(fieldType);
        case FLOAT:
          return new BeamBuiltinAggregations.Avg<Float>(fieldType);
        case DOUBLE:
          return new BeamBuiltinAggregations.Avg<Double>(fieldType);
        case TIMESTAMP:
          return new BeamBuiltinAggregations.Avg<Date>(fieldType);
        case DECIMAL:
          return new BeamBuiltinAggregations.Avg<BigDecimal>(fieldType);
        default:
          throw new UnsupportedOperationException(
              String.format("[%s] is not support in AVG", fieldType));
      }
    }

    private SqlTypeName fieldType;
      private Avg(SqlTypeName fieldType) {
        this.fieldType = fieldType;
      }

    @Override
    public KV<BigDecimal, Long> init() {
      return KV.of(new BigDecimal(0), 0L);
    }

    @Override
    public KV<BigDecimal, Long> add(KV<BigDecimal, Long> accumulator, T input) {
      return KV.of(
              accumulator.getKey().add(new BigDecimal(input.toString())),
              accumulator.getValue() + 1);
    }

    @Override
    public KV<BigDecimal, Long> merge(Iterable<KV<BigDecimal, Long>> accumulators) {
      BigDecimal v = new BigDecimal(0);
      long s = 0;
      Iterator<KV<BigDecimal, Long>> ite = accumulators.iterator();
      while (ite.hasNext()) {
        KV<BigDecimal, Long> r = ite.next();
        v = v.add(r.getKey());
        s += r.getValue();
      }
      return KV.of(v, s);
    }

    @Override
    public T result(KV<BigDecimal, Long> accumulator) {
      BigDecimal decimalAvg = accumulator.getKey().divide(
          new BigDecimal(accumulator.getValue()));
      Object result = null;
      switch (fieldType) {
        case INTEGER:
          result = decimalAvg.intValue();
          break;
        case BIGINT:
          result = decimalAvg.longValue();
          break;
        case SMALLINT:
          result = decimalAvg.shortValue();
          break;
        case TINYINT:
          result = decimalAvg.byteValue();
          break;
        case DOUBLE:
          result = decimalAvg.doubleValue();
          break;
        case FLOAT:
          result = decimalAvg.floatValue();
          break;
        case DECIMAL:
          result = decimalAvg;
          break;
        default:
          break;
      }
      return (T) result;
    }

    @Override
    public Coder<KV<BigDecimal, Long>> getAccumulatorCoder(CoderRegistry registry)
        throws CannotProvideCoderException {
      return KvCoder.of(BigDecimalCoder.of(), VarLongCoder.of());
    }
  }

}
