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
package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import static org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.impl.utils.BigDecimalConverter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.runtime.SqlFunctions;

/**
 * {@link Combine.CombineFn} for <em>Covariance</em> on {@link Number} types.
 *
 * <p>Calculates Population Covariance and Sample Covariance using incremental formulas described in
 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance, presumably by Pébay, Philippe
 * (2008), in "Formulas for Robust, One-Pass Parallel Computation of Covariances and Arbitrary-Order
 * Statistical Moments".
 */
@Internal
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class CovarianceFn<T extends Number>
    extends Combine.CombineFn<Row, CovarianceAccumulator, T> {

  static final MathContext MATH_CTX = new MathContext(10, RoundingMode.HALF_UP);

  private static final boolean SAMPLE = true;
  private static final boolean POP = false;

  private boolean isSample; // flag to determine return value should be Covariance Pop or Sample
  private SerializableFunction<BigDecimal, T> decimalConverter;

  public static <V extends Number> CovarianceFn newPopulation(Schema.TypeName typeName) {
    return newPopulation(BigDecimalConverter.forSqlType(typeName));
  }

  public static <V extends Number> CovarianceFn newPopulation(
      SerializableFunction<BigDecimal, V> decimalConverter) {

    return new CovarianceFn<>(POP, decimalConverter);
  }

  public static <V extends Number> CovarianceFn newSample(Schema.TypeName typeName) {
    return newSample(BigDecimalConverter.forSqlType(typeName));
  }

  public static <V extends Number> CovarianceFn newSample(
      SerializableFunction<BigDecimal, V> decimalConverter) {

    return new CovarianceFn<>(SAMPLE, decimalConverter);
  }

  private CovarianceFn(boolean isSample, SerializableFunction<BigDecimal, T> decimalConverter) {
    this.isSample = isSample;
    this.decimalConverter = decimalConverter;
  }

  @Override
  public CovarianceAccumulator createAccumulator() {
    return CovarianceAccumulator.ofZeroElements();
  }

  @Override
  public CovarianceAccumulator addInput(CovarianceAccumulator currentVariance, Row rawInput) {
    if (rawInput == null) {
      return currentVariance;
    }

    checkArgument(rawInput.getFieldCount() > 1);

    return currentVariance.combineWith(
        CovarianceAccumulator.ofSingleElement(
            SqlFunctions.toBigDecimal((Object) rawInput.getBaseValue(0, Object.class)),
            SqlFunctions.toBigDecimal((Object) rawInput.getBaseValue(1, Object.class))));
  }

  @Override
  public CovarianceAccumulator mergeAccumulators(Iterable<CovarianceAccumulator> covariances) {
    return StreamSupport.stream(covariances.spliterator(), false)
        .reduce(CovarianceAccumulator.ofZeroElements(), CovarianceAccumulator::combineWith);
  }

  @Override
  public Coder<CovarianceAccumulator> getAccumulatorCoder(
      CoderRegistry registry, Coder<Row> inputCoder) {
    return SerializableCoder.of(CovarianceAccumulator.class);
  }

  @Override
  public T extractOutput(CovarianceAccumulator accumulator) {
    return decimalConverter.apply(getCovariance(accumulator));
  }

  private BigDecimal getCovariance(CovarianceAccumulator covariance) {

    BigDecimal adjustedCount =
        this.isSample ? covariance.count().subtract(BigDecimal.ONE) : covariance.count();

    return covariance.covariance().divide(adjustedCount, MATH_CTX);
  }
}
