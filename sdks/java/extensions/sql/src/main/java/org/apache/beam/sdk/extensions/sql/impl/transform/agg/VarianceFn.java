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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.runtime.SqlFunctions;

/**
 * {@link Combine.CombineFn} for <em>Variance</em> on {@link Number} types.
 *
 * <p>Calculates Population Variance and Sample Variance using incremental formulas described, for
 * example, by Chan, Golub, and LeVeque in "Algorithms for computing the sample variance: analysis
 * and recommendations", The American Statistician, 37 (1983) pp. 242--247.
 *
 * <p>If variance is defined like this:
 *
 * <ul>
 *   <li>Input elements: {@code (x[1], ... , x[n])}
 *   <li>Sum of elements: {sum(x) = x[1] + ... + x[n]}
 *   <li>Average of all elements in the input: {@code mean(x) = sum(x) / n}
 *   <li>Deviation of {@code i}th element from the current mean: {@code deviation(x, i) = x[i] -
 *       mean(n)}
 *   <li>Variance: {@code variance(x) = deviation(x, 1)^2 + ... + deviation(x, n)^2}
 * </ul>
 *
 * <p>Then variance of combined input of 2 samples {@code (x[1], ... , x[n])} and {@code (y[1], ...
 * , y[m])} is calculated using this formula:
 *
 * <ul>
 *   <li>{@code variance(concat(x,y)) = variance(x) + variance(y) + increment}, where:
 *   <li>{@code increment = m/(n(m+n)) * (n/m * sum(x) - sum(y))^2}
 * </ul>
 *
 * <p>This is also applicable for a single element increment, assuming that variance of a single
 * element input is zero
 *
 * <p>To implement the above formula we keep track of the current variation, sum, and count of
 * elements, and then use the formula whenever new element comes or we need to merge variances for 2
 * samples.
 */
@Internal
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class VarianceFn<T extends Number> extends Combine.CombineFn<T, VarianceAccumulator, T> {

  static final MathContext MATH_CTX = new MathContext(10, RoundingMode.HALF_UP);

  private static final boolean SAMPLE = true;
  private static final boolean POP = false;

  private boolean isSample; // flag to determine return value should be Variance Pop or Sample
  private SerializableFunction<BigDecimal, T> decimalConverter;

  public static <V extends Number> VarianceFn newPopulation(Schema.TypeName typeName) {
    return newPopulation(BigDecimalConverter.forSqlType(typeName));
  }

  public static <V extends Number> VarianceFn newPopulation(
      SerializableFunction<BigDecimal, V> decimalConverter) {

    return new VarianceFn<>(POP, decimalConverter);
  }

  public static <V extends Number> VarianceFn newSample(Schema.TypeName typeName) {
    return newSample(BigDecimalConverter.forSqlType(typeName));
  }

  public static <V extends Number> VarianceFn newSample(
      SerializableFunction<BigDecimal, V> decimalConverter) {

    return new VarianceFn<>(SAMPLE, decimalConverter);
  }

  private VarianceFn(boolean isSample, SerializableFunction<BigDecimal, T> decimalConverter) {
    this.isSample = isSample;
    this.decimalConverter = decimalConverter;
  }

  @Override
  public VarianceAccumulator createAccumulator() {
    return VarianceAccumulator.ofZeroElements();
  }

  @Override
  public VarianceAccumulator addInput(VarianceAccumulator currentVariance, T rawInput) {

    if (rawInput == null) {
      return currentVariance;
    }

    return currentVariance.combineWith(
        VarianceAccumulator.ofSingleElement(SqlFunctions.toBigDecimal(rawInput)));
  }

  @Override
  public VarianceAccumulator mergeAccumulators(Iterable<VarianceAccumulator> variances) {
    return StreamSupport.stream(variances.spliterator(), false)
        .reduce(VarianceAccumulator.ofZeroElements(), VarianceAccumulator::combineWith);
  }

  @Override
  public Coder<VarianceAccumulator> getAccumulatorCoder(
      CoderRegistry registry, Coder<T> inputCoder) {
    return SerializableCoder.of(VarianceAccumulator.class);
  }

  @Override
  public T extractOutput(VarianceAccumulator accumulator) {
    return decimalConverter.apply(getVariance(accumulator));
  }

  private BigDecimal getVariance(VarianceAccumulator variance) {

    BigDecimal adjustedCount =
        this.isSample ? variance.count().subtract(BigDecimal.ONE) : variance.count();

    return variance.variance().divide(adjustedCount, MATH_CTX);
  }
}
