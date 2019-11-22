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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DistributionCell}. */
@RunWith(JUnit4.class)
public class DistributionCellTest {
  private DistributionCell cell = new DistributionCell(MetricName.named("hello", "world"));

  @Test
  public void testDeltaAndCumulative() {
    cell.update(5);
    cell.update(7);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(12, 2, 5, 7)));
    assertThat(
        "getCumulative is idempotent",
        cell.getCumulative(),
        equalTo(DistributionData.create(12, 2, 5, 7)));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    cell.update(30);
    assertThat(cell.getCumulative(), equalTo(DistributionData.create(42, 3, 5, 30)));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }

  @Test
  public void testEquals() {
    DistributionCell distributionCell = new DistributionCell(MetricName.named("namespace", "name"));
    DistributionCell equal = new DistributionCell(MetricName.named("namespace", "name"));
    Assert.assertEquals(distributionCell, equal);
    Assert.assertEquals(distributionCell.hashCode(), equal.hashCode());
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
    differentValue.update(DistributionData.create(1, 1, 1, 1));
    Assert.assertNotEquals(distributionCell, differentValue);
    Assert.assertNotEquals(distributionCell.hashCode(), differentValue.hashCode());

    DistributionCell differentName =
        new DistributionCell(MetricName.named("DIFFERENT", "DIFFERENT"));
    Assert.assertNotEquals(distributionCell, differentName);
    Assert.assertNotEquals(distributionCell.hashCode(), differentName.hashCode());
  }

  @Test
  public void testReset() {
    DistributionCell distributionCell = new DistributionCell(MetricName.named("namespace", "name"));
    distributionCell.update(2);
    assertThat(distributionCell.getCumulative(), equalTo(DistributionData.create(2, 1, 2, 2)));

    distributionCell.reset();
    assertThat(distributionCell.getCumulative(), equalTo(DistributionData.EMPTY));
    assertThat(distributionCell.getDirty(), equalTo(new DirtyState()));
  }
}
