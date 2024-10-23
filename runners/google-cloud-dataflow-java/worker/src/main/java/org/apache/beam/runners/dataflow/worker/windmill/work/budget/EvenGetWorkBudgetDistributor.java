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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath.divide;

import java.math.RoundingMode;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Evenly distributes the provided budget across the available {@link GetWorkBudgetSpender}(s). */
@Internal
final class EvenGetWorkBudgetDistributor implements GetWorkBudgetDistributor {
  private static final Logger LOG = LoggerFactory.getLogger(EvenGetWorkBudgetDistributor.class);

  @Override
  public <T extends GetWorkBudgetSpender> void distributeBudget(
      ImmutableCollection<T> budgetSpenders, GetWorkBudget getWorkBudget) {
    if (budgetSpenders.isEmpty()) {
      LOG.debug("Cannot distribute budget to no owners.");
      return;
    }

    if (getWorkBudget.equals(GetWorkBudget.noBudget())) {
      LOG.debug("Cannot distribute 0 budget.");
      return;
    }

    GetWorkBudget budgetPerStream = computeDesiredPerStreamBudget(budgetSpenders, getWorkBudget);
    budgetSpenders.forEach(getWorkBudgetSpender -> getWorkBudgetSpender.setBudget(budgetPerStream));
  }

  private <T extends GetWorkBudgetSpender> GetWorkBudget computeDesiredPerStreamBudget(
      ImmutableCollection<T> streams, GetWorkBudget totalGetWorkBudget) {
    return GetWorkBudget.builder()
        .setItems(divide(totalGetWorkBudget.items(), streams.size(), RoundingMode.CEILING))
        .setBytes(divide(totalGetWorkBudget.bytes(), streams.size(), RoundingMode.CEILING))
        .build();
  }
}
