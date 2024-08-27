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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.DoubleMath.roundToLong;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath.divide;

import java.math.RoundingMode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Evenly distributes the provided budget across the available {@link GetWorkBudgetSpender}(s). */
@Internal
final class EvenGetWorkBudgetDistributor implements GetWorkBudgetDistributor {
  private static final Logger LOG = LoggerFactory.getLogger(EvenGetWorkBudgetDistributor.class);
  private final Supplier<GetWorkBudget> activeWorkBudgetSupplier;
  private final boolean isActiveWorkBudgetAware;

  EvenGetWorkBudgetDistributor(
      Supplier<GetWorkBudget> activeWorkBudgetSupplier, boolean isActiveWorkBudgetAware) {
    this.activeWorkBudgetSupplier = activeWorkBudgetSupplier;
    this.isActiveWorkBudgetAware = isActiveWorkBudgetAware;
  }

  private static boolean isBelowFiftyPercentOfTarget(
      GetWorkBudget remaining, GetWorkBudget target) {
    return remaining.items() < roundToLong(target.items() * 0.5, RoundingMode.CEILING)
        || remaining.bytes() < roundToLong(target.bytes() * 0.5, RoundingMode.CEILING);
  }

  @Override
  public <T extends GetWorkBudgetSpender> void distributeBudget(
      ImmutableCollection<T> budgetOwners, GetWorkBudget getWorkBudget) {
    if (budgetOwners.isEmpty()) {
      LOG.debug("Cannot distribute budget to no owners.");
      return;
    }

    if (getWorkBudget.equals(GetWorkBudget.noBudget())) {
      LOG.debug("Cannot distribute 0 budget.");
      return;
    }

    Map<T, GetWorkBudget> desiredBudgets = computeDesiredBudgets(budgetOwners, getWorkBudget);

    for (Entry<T, GetWorkBudget> streamAndDesiredBudget : desiredBudgets.entrySet()) {
      GetWorkBudgetSpender getWorkBudgetSpender = streamAndDesiredBudget.getKey();
      GetWorkBudget desired = streamAndDesiredBudget.getValue();
      GetWorkBudget remaining = getWorkBudgetSpender.remainingBudget();
      if (isBelowFiftyPercentOfTarget(remaining, desired) && isActiveWorkBudgetAware) {
        GetWorkBudget adjustment = desired.subtract(remaining);
        getWorkBudgetSpender.adjustBudget(adjustment);
      } else {
        getWorkBudgetSpender.adjustBudget(desired);
      }
    }
  }

  private <T extends GetWorkBudgetSpender> ImmutableMap<T, GetWorkBudget> computeDesiredBudgets(
      ImmutableCollection<T> streams, GetWorkBudget totalGetWorkBudget) {
    GetWorkBudget activeWorkBudget = activeWorkBudgetSupplier.get();
    // TODO: Fix possibly non-deterministic handing out of budgets.
    // Rounding up here will drift upwards over the lifetime of the streams.
    GetWorkBudget budgetPerStream =
        GetWorkBudget.builder()
            .setItems(
                divide(
                    totalGetWorkBudget.items() - activeWorkBudget.items(),
                    streams.size(),
                    RoundingMode.CEILING))
            .setBytes(
                divide(
                    totalGetWorkBudget.bytes() - activeWorkBudget.bytes(),
                    streams.size(),
                    RoundingMode.CEILING))
            .build();
    return streams.stream().collect(toImmutableMap(Function.identity(), unused -> budgetPerStream));
  }
}
