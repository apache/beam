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
package org.apache.beam.runners.core.metrics;

import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DistributionCell}. */
@RunWith(JUnit4.class)
public class DistributionCellTest {
  @Test
  public void testDeltaAndCumulative() {
    DistributionCell cell =
        new DistributionCell(
            DistributionMetricKey.create(
                MetricName.named("hello", "world"), ImmutableSet.of(95.0D, 99.0D)));
    cell.update(5);
    cell.update(7);
    ImmutableMap<Double, Double> percentiles =
        ImmutableMap.<Double, Double>builder().put(95.0D, 7.0D).put(99.0D, 7.0D).build();
    Assert.assertEquals(
        cell.getCumulative().extractResult(), DistributionResult.create(12, 2, 5, 7, percentiles));
    Assert.assertTrue(cell.getDirty().beforeCommit());
    cell.getDirty().afterCommit();
    Assert.assertFalse(cell.getDirty().beforeCommit());

    cell.update(30);
    ImmutableMap<Double, Double> percentiles1 =
        ImmutableMap.<Double, Double>builder().put(95.0D, 30.0D).put(99.0D, 30.0D).build();
    Assert.assertEquals(
        cell.getCumulative().extractResult(),
        DistributionResult.create(42, 3, 5, 30, percentiles1));
  }

  @Test
  public void testDeltaAndCumulativeWithPercentilesAsEmpty() {
    DistributionCell cell =
        new DistributionCell(
            DistributionMetricKey.create(MetricName.named("hello", "world"), ImmutableSet.of()));
    cell.update(5);
    cell.update(7);
    Assert.assertEquals(
        cell.getCumulative().extractResult(),
        DistributionResult.create(12, 2, 5, 7, ImmutableMap.of()));
    Assert.assertTrue(cell.getDirty().beforeCommit());
    cell.getDirty().afterCommit();
    Assert.assertFalse(cell.getDirty().beforeCommit());
  }

  @Test
  public void testEquals() {
    DistributionCell cell11 = new DistributionCell(MetricName.named("hello", "world"));
    DistributionCell cell21 = new DistributionCell(MetricName.named("hello", "world"));
    cell11.update(5);
    cell21.update(5);
    Assert.assertEquals(cell11, cell21);
    Assert.assertEquals(cell11.hashCode(), cell21.hashCode());

    DistributionCell cell1 =
        new DistributionCell(
            DistributionMetricKey.create(
                MetricName.named("hello", "world"), ImmutableSet.of(95.0D, 99.0D)));
    cell1.update(5);
    DistributionCell cell2 =
        new DistributionCell(
            DistributionMetricKey.create(
                MetricName.named("hello", "world"), ImmutableSet.of(95.0D, 99.0D)));
    cell2.update(5);
    Assert.assertEquals(cell1, cell2);
    Assert.assertEquals(cell1.hashCode(), cell2.hashCode());
  }

  @Test
  public void testNotEquals() {
    DistributionCell distributionCell = new DistributionCell(MetricName.named("namespace", "name"));

    Assert.assertNotEquals(distributionCell, new Object());

    DistributionCell differentDirty = new DistributionCell(MetricName.named("namespace", "name"));
    differentDirty.getDirty().beforeCommit();
    Assert.assertNotEquals(distributionCell, differentDirty);
    Assert.assertNotEquals(distributionCell.hashCode(), differentDirty.hashCode());

    DistributionCell differentValue = new DistributionCell(MetricName.named("namespace", "name"));
    differentValue.update(1);
    Assert.assertNotEquals(distributionCell, differentValue);
    Assert.assertNotEquals(distributionCell.hashCode(), differentValue.hashCode());

    DistributionCell differentName =
        new DistributionCell(MetricName.named("DIFFERENT", "DIFFERENT"));
    Assert.assertNotEquals(distributionCell, differentName);
    Assert.assertNotEquals(distributionCell.hashCode(), differentName.hashCode());
  }

  @Test
  public void testReset() {
    DistributionCell distributionCell =
        new DistributionCell(
            DistributionMetricKey.create(
                MetricName.named("namespace", "name"), ImmutableSet.of(95.0D, 99.0D)));
    distributionCell.update(2);
    ImmutableMap<Double, Double> percentiles =
        ImmutableMap.<Double, Double>builder().put(95.0D, 2.0D).put(99.0D, 2.0D).build();
    Assert.assertEquals(distributionCell.getCumulative().percentiles(), percentiles);
    Assert.assertEquals(
        distributionCell.getCumulative().extractResult(),
        DistributionResult.create(2, 1, 2, 2, percentiles));
    distributionCell.reset();
    Assert.assertEquals(distributionCell.getCumulative(), DistributionData.empty());
    Assert.assertEquals(distributionCell.getDirty(), new DirtyState());
  }
}
