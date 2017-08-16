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
import java.util.Date;
import java.util.Iterator;
import org.apache.beam.sdk.coders.BigDecimalCoder;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Built-in aggregations functions for COUNT/MAX/MIN/SUM/AVG/VAR_POP/VAR_SAMP.
 */
class BeamBuiltinAggregations {
  private static MathContext mc = new MathContext(10, RoundingMode.HALF_UP);

  /**
   * {@link CombineFn} for MAX based on {@link Max} and {@link Combine.BinaryCombineFn}.
   */
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
        return new CustMax<Date>();
      case DECIMAL:
        return new CustMax<BigDecimal>();
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
        return new CustMin<Date>();
      case DECIMAL:
        return new CustMin<BigDecimal>();
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

  /**
   * {@link CombineFn} for AVG.
   */
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

  /**
   * {@link CombineFn} for VAR_POP and VAR_SAMP.
   */
  public static CombineFn createVar(SqlTypeName fieldType, boolean isSamp) {
    switch (fieldType) {
      case INTEGER:
        return new IntegerVar(isSamp);
      case SMALLINT:
        return new ShortVar(isSamp);
      case TINYINT:
        return new ByteVar(isSamp);
      case BIGINT:
        return new LongVar(isSamp);
      case FLOAT:
        return new FloatVar(isSamp);
      case DOUBLE:
        return new DoubleVar(isSamp);
      case DECIMAL:
        return new BigDecimalVar(isSamp);
      default:
        throw new UnsupportedOperationException(
            String.format("[%s] is not support in AVG", fieldType));
    }
  }

  static class CustMax<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    public T apply(T left, T right) {
      return (right == null || right.compareTo(left) < 0) ? left : right;
    }
  }

  static class CustMin<T extends Comparable<T>> extends Combine.BinaryCombineFn<T> {
    public T apply(T left, T right) {
      return (left == null || left.compareTo(right) < 0) ? left : right;
    }
  }

  static class ShortSum extends Combine.BinaryCombineFn<Short> {
    public Short apply(Short left, Short right) {
      return (short) (left + right);
    }
  }

  static class ByteSum extends Combine.BinaryCombineFn<Byte> {
    public Byte apply(Byte left, Byte right) {
      return (byte) (left + right);
    }
  }

  static class FloatSum extends Combine.BinaryCombineFn<Float> {
    public Float apply(Float left, Float right) {
      return left + right;
    }
  }

  static class BigDecimalSum extends Combine.BinaryCombineFn<BigDecimal> {
    public BigDecimal apply(BigDecimal left, BigDecimal right) {
      return left.add(right);
    }
  }

  /**
   * {@link CombineFn} for <em>AVG</em> on {@link Number} types.
   */
  abstract static class Avg<T extends Number>
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

    protected BigDecimal prepareOutput(KV<Integer, BigDecimal> accumulator){
      return accumulator.getValue().divide(new BigDecimal(accumulator.getKey()), mc);
    }

    public abstract T extractOutput(KV<Integer, BigDecimal> accumulator);
    public abstract BigDecimal toBigDecimal(T record);
  }

  static class IntegerAvg extends Avg<Integer>{
    public Integer extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).intValue();
    }

    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class LongAvg extends Avg<Long>{
    public Long extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).longValue();
    }

    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class ShortAvg extends Avg<Short>{
    public Short extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).shortValue();
    }

    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteAvg extends Avg<Byte>{
    public Byte extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).byteValue();
    }

    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class FloatAvg extends Avg<Float>{
    public Float extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).floatValue();
    }

    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleAvg extends Avg<Double>{
    public Double extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator).doubleValue();
    }

    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalAvg extends Avg<BigDecimal>{
    public BigDecimal extractOutput(KV<Integer, BigDecimal> accumulator) {
      return accumulator.getKey() == 0 ? null : prepareOutput(accumulator);
    }

    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }

  static class VarAgg implements Serializable {
    long count; // number of elements
    BigDecimal sum; // sum of elements

    public VarAgg(long count, BigDecimal sum) {
      this.count = count;
      this.sum = sum;
   }
  }

  /**
   * {@link CombineFn} for <em>Var</em> on {@link Number} types.
   * Variance Pop and Variance Sample
   * <p>Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
   * "Algorithms for computing the sample variance: analysis and recommendations"
   * The American Statistician, 37 (1983) pp. 242--247.</p>
   * <p>variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)</p>
   * <p>where: - variance is sum[x-avg^2] (this is actually n times the variance)
   * and is updated at every step. - n is the count of elements in chunk1 - m is
   * the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
   * sum of elements in chunk2.</p>
   */
  abstract static class Var<T extends Number>
          extends CombineFn<T, KV<BigDecimal, VarAgg>, T> {
    boolean isSamp;  // flag to determine return value should be Variance Pop or Variance Sample

    public Var(boolean isSamp){
      this.isSamp = isSamp;
    }

    @Override
    public KV<BigDecimal, VarAgg> createAccumulator() {
      VarAgg varagg = new VarAgg(0L, new BigDecimal(0));
      return KV.of(new BigDecimal(0), varagg);
    }

    @Override
    public KV<BigDecimal, VarAgg> addInput(KV<BigDecimal, VarAgg> accumulator, T input) {
      BigDecimal v;
      if (input == null) {
        return accumulator;
      } else {
        v = new BigDecimal(input.toString());
        accumulator.getValue().count++;
        accumulator.getValue().sum = accumulator.getValue().sum
                .add(new BigDecimal(input.toString()));
        BigDecimal variance;
        if (accumulator.getValue().count > 1) {

//          pseudo code for the formula
//          t = count * v - sum;
//          variance = (t^2) / (count * (count - 1));
          BigDecimal t = v.multiply(new BigDecimal(accumulator.getValue().count))
                                    .subtract(accumulator.getValue().sum);
          variance = t.pow(2)
                  .divide(new BigDecimal(accumulator.getValue().count)
                            .multiply(new BigDecimal(accumulator.getValue().count)
                                      .subtract(BigDecimal.ONE)), mc);
        } else {
          variance = BigDecimal.ZERO;
        }
       return KV.of(accumulator.getKey().add(variance), accumulator.getValue());
      }
    }

    @Override
    public KV<BigDecimal, VarAgg> mergeAccumulators(
            Iterable<KV<BigDecimal, VarAgg>> accumulators) {
      BigDecimal variance = new BigDecimal(0);
      long count = 0;
      BigDecimal sum = new BigDecimal(0);

      Iterator<KV<BigDecimal, VarAgg>> ite = accumulators.iterator();
      while (ite.hasNext()) {
        KV<BigDecimal, VarAgg> r = ite.next();

        BigDecimal b = r.getValue().sum;

        count += r.getValue().count;
        sum = sum.add(b);

//        t = ( r.count / count ) * sum - b;
//        d = t^2 * ( ( count / r.count ) / ( count + r.count ) );
        BigDecimal t = new BigDecimal(r.getValue().count).divide(new BigDecimal(count), mc)
                .multiply(sum).subtract(b);
        BigDecimal d = t.pow(2)
                .multiply(new BigDecimal(r.getValue().count).divide(new BigDecimal(count), mc)
                          .divide(new BigDecimal(count)
                                  .add(new BigDecimal(r.getValue().count))), mc);
        variance = variance.add(r.getKey().add(d));
      }

      return KV.of(variance, new VarAgg(count, sum));
    }

    @Override
    public Coder<KV<BigDecimal, VarAgg>> getAccumulatorCoder(CoderRegistry registry,
        Coder<T> inputCoder) throws CannotProvideCoderException {
      return KvCoder.of(BigDecimalCoder.of(), SerializableCoder.of(VarAgg.class));
    }

    protected BigDecimal prepareOutput(KV<BigDecimal, VarAgg> accumulator){
      BigDecimal decimalVar;
      if (accumulator.getValue().count > 1) {
        BigDecimal a = accumulator.getKey();
        BigDecimal b = new BigDecimal(accumulator.getValue().count)
                .subtract(this.isSamp ? BigDecimal.ONE : BigDecimal.ZERO);

        decimalVar = a.divide(b, mc);
      } else {
        decimalVar = BigDecimal.ZERO;
      }
      return decimalVar;
    }

    public abstract T extractOutput(KV<BigDecimal, VarAgg> accumulator);

    public abstract BigDecimal toBigDecimal(T record);
  }

  static class IntegerVar extends Var<Integer> {
    public IntegerVar(boolean isSamp) {
      super(isSamp);
    }

    public Integer extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).intValue();
    }

    @Override
    public BigDecimal toBigDecimal(Integer record) {
      return new BigDecimal(record);
    }
  }

  static class ShortVar extends Var<Short> {
    public ShortVar(boolean isSamp) {
      super(isSamp);
    }

    public Short extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).shortValue();
    }

    @Override
    public BigDecimal toBigDecimal(Short record) {
      return new BigDecimal(record);
    }
  }

  static class ByteVar extends Var<Byte> {
    public ByteVar(boolean isSamp) {
      super(isSamp);
    }

    public Byte extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).byteValue();
    }

    @Override
    public BigDecimal toBigDecimal(Byte record) {
      return new BigDecimal(record);
    }
  }

  static class LongVar extends Var<Long> {
    public LongVar(boolean isSamp) {
      super(isSamp);
    }

    public Long extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).longValue();
    }

    @Override
    public BigDecimal toBigDecimal(Long record) {
      return new BigDecimal(record);
    }
  }

  static class FloatVar extends Var<Float> {
    public FloatVar(boolean isSamp) {
      super(isSamp);
    }

    public Float extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).floatValue();
    }

    @Override
    public BigDecimal toBigDecimal(Float record) {
      return new BigDecimal(record);
    }
  }

  static class DoubleVar extends Var<Double> {
    public DoubleVar(boolean isSamp) {
      super(isSamp);
    }

    public Double extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator).doubleValue();
    }

    @Override
    public BigDecimal toBigDecimal(Double record) {
      return new BigDecimal(record);
    }
  }

  static class BigDecimalVar extends Var<BigDecimal> {
    public BigDecimalVar(boolean isSamp) {
      super(isSamp);
    }

    public BigDecimal extractOutput(KV<BigDecimal, VarAgg> accumulator) {
      return prepareOutput(accumulator);
    }

    @Override
    public BigDecimal toBigDecimal(BigDecimal record) {
      return record;
    }
  }
}
