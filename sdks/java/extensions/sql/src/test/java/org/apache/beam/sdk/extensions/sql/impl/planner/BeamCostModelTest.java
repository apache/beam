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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import org.junit.Assert;
import org.junit.Test;

/** Tests the behavior of BeamCostModel. */
public class BeamCostModelTest {

  @Test
  public void testDefaultConstructorIsInfinite() {
    BeamCostModel cost = BeamCostModel.FACTORY.makeCost(1, 1, 1);
    Assert.assertTrue(cost.isInfinite());
  }

  @Test
  public void testOneInfiniteValue() {
    BeamCostModel cost = BeamCostModel.FACTORY.makeCost(Double.POSITIVE_INFINITY, 1);
    Assert.assertTrue(cost.isInfinite());
  }

  @Test
  public void testComparisonOfBoundedCost() {
    BeamCostModel cost1 = BeamCostModel.FACTORY.makeCost(10, 0);
    BeamCostModel cost2 = BeamCostModel.FACTORY.makeCost(1, 0);
    BeamCostModel cost3 = BeamCostModel.FACTORY.makeCost(10, 0);
    Assert.assertTrue(cost2.isLt(cost1));
    Assert.assertFalse(cost1.isLt(cost2));

    Assert.assertFalse(cost1.isLt(cost3));
    Assert.assertFalse(cost3.isLt(cost1));
    Assert.assertTrue(cost3.isLe(cost1));
  }

  @Test
  public void testComparisonOfUnboundedCost() {
    BeamCostModel cost1 = BeamCostModel.FACTORY.makeCost(0, 10);
    BeamCostModel cost2 = BeamCostModel.FACTORY.makeCost(0, 1);
    BeamCostModel cost3 = BeamCostModel.FACTORY.makeCost(0, 10);
    Assert.assertTrue(cost2.isLt(cost1));
    Assert.assertFalse(cost1.isLt(cost2));

    Assert.assertTrue(cost1.equals(cost3));
    Assert.assertFalse(cost1.isLt(cost3));
    Assert.assertFalse(cost3.isLt(cost1));
    Assert.assertTrue(cost3.isLe(cost1));
  }

  @Test
  public void testEffectOfRateVsRowCount() {
    BeamCostModel boundedCost = BeamCostModel.FACTORY.makeCost(10, 0);
    BeamCostModel unboundedCost = BeamCostModel.FACTORY.makeCost(0, 10);

    Assert.assertTrue(boundedCost.isLt(unboundedCost));
    Assert.assertTrue(boundedCost.isLe(unboundedCost));
    Assert.assertFalse(unboundedCost.isLe(boundedCost));
  }

  @Test
  public void testComparisonInfiniteVsInfinite() {
    BeamCostModel inf1 = BeamCostModel.FACTORY.makeCost(Double.POSITIVE_INFINITY, 0);
    BeamCostModel inf2 = BeamCostModel.FACTORY.makeInfiniteCost();

    Assert.assertTrue(inf1.isLe(inf2));
    Assert.assertTrue(inf2.isLe(inf1));
    Assert.assertFalse(inf1.isLt(inf2));
    Assert.assertFalse(inf2.isLt(inf1));
  }

  @Test
  public void testComparisonInfiniteVsHuge() {
    BeamCostModel inf = BeamCostModel.FACTORY.makeCost(Double.POSITIVE_INFINITY, 0);
    BeamCostModel huge = BeamCostModel.FACTORY.makeHugeCost();

    Assert.assertTrue(huge.isLe(inf));
    Assert.assertTrue(huge.isLt(inf));
    Assert.assertFalse(inf.isLt(huge));
    Assert.assertFalse(inf.isLt(huge));
  }

  @Test
  public void testHugePlusHugeIsInfinite() {
    BeamCostModel cost =
        BeamCostModel.FACTORY.makeHugeCost().plus(BeamCostModel.FACTORY.makeHugeCost());
    Assert.assertTrue(cost.isInfinite());
  }
}
