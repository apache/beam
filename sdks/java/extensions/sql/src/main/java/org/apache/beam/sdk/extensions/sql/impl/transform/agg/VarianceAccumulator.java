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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Accumulates current variance of a sample, its sum, and number of elements.
 */
@AutoValue
abstract class VarianceAccumulator implements Serializable {
  static final VarianceAccumulator EMPTY =
      newVarianceAccumulator(BigDecimal.ZERO, BigDecimal.ZERO, BigDecimal.ZERO);

  abstract BigDecimal variance();
  abstract BigDecimal count();
  abstract BigDecimal sum();

  static VarianceAccumulator newVarianceAccumulator(
      BigDecimal variance,
      BigDecimal count,
      BigDecimal sum) {

    return new AutoValue_VarianceAccumulator(variance, count, sum);
  }

  static VarianceAccumulator ofZeroElements() {
    return EMPTY;
  }

  static VarianceAccumulator ofSingleElement(BigDecimal inputElement) {
    return newVarianceAccumulator(BigDecimal.ZERO, BigDecimal.ONE, inputElement);
  }

  /**
   * See {@link VarianceFn} doc above for explanation.
   */
  VarianceAccumulator combineWith(VarianceAccumulator otherVariance) {
    if (EMPTY.equals(this)) {
      return otherVariance;
    }

    if (EMPTY.equals(otherVariance)) {
      return this;
    }

    BigDecimal increment = calculateIncrement(this, otherVariance);
    BigDecimal combinedVariance =
        this.variance()
            .add(otherVariance.variance())
            .add(increment);

    return newVarianceAccumulator(
        combinedVariance,
        this.count().add(otherVariance.count()),
        this.sum().add(otherVariance.sum()));
  }

  /**
   * Implements this part: {@code increment = m/(n(m+n)) * (sum(x) * n/m  - sum(y))^2 }.
   */
  private BigDecimal calculateIncrement(
      VarianceAccumulator varianceX,
      VarianceAccumulator varianceY) {

    BigDecimal m = varianceX.count();
    BigDecimal n = varianceY.count();
    BigDecimal sumX = varianceX.sum();
    BigDecimal sumY = varianceY.sum();

    // m/(n(m+n))
    BigDecimal multiplier = m.divide(n.multiply(m.add(n)), VarianceFn.MATH_CTX);

    // (n/m * sum(x) - sum(y))^2
    BigDecimal square = (sumX.multiply(n).divide(m, VarianceFn.MATH_CTX)).subtract(sumY).pow(2);

    return multiplier.multiply(square);
  }
}
