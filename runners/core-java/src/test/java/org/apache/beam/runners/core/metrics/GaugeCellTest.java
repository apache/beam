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

/** Tests for {@link GaugeCell}. */
public class GaugeCellTest {
  private GaugeCell cell = new GaugeCell(MetricName.named("hello", "world"));

  @Test
  public void testDeltaAndCumulative() {
    cell.set(5);
    cell.set(7);
    assertThat(cell.getCumulative().value(), equalTo(GaugeData.create(7).value()));
    assertThat("getCumulative is idempotent", cell.getCumulative().value(), equalTo(7L));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    cell.set(30);
    assertThat(cell.getCumulative().value(), equalTo(30L));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }

  @Test
  public void testEquals() {
    GaugeCell gaugeCell = new GaugeCell(MetricName.named("namespace", "name"));
    GaugeCell equal = new GaugeCell(MetricName.named("namespace", "name"));
    Assert.assertEquals(gaugeCell, equal);
    Assert.assertEquals(gaugeCell.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    GaugeCell gaugeCell = new GaugeCell(MetricName.named("namespace", "name"));

    Assert.assertNotEquals(gaugeCell, new Object());

    GaugeCell differentDirty = new GaugeCell(MetricName.named("namespace", "name"));
    differentDirty.getDirty().beforeCommit();
    Assert.assertNotEquals(gaugeCell, differentDirty);
    Assert.assertNotEquals(gaugeCell.hashCode(), differentDirty.hashCode());

    GaugeCell differentGaugeValue = new GaugeCell(MetricName.named("namespace", "name"));
    differentGaugeValue.update(GaugeData.create(1));
    Assert.assertNotEquals(gaugeCell, differentGaugeValue);
    Assert.assertNotEquals(gaugeCell.hashCode(), differentGaugeValue.hashCode());

    GaugeCell differentName = new GaugeCell(MetricName.named("DIFFERENT", "DIFFERENT"));
    Assert.assertNotEquals(gaugeCell, differentName);
    Assert.assertNotEquals(gaugeCell.hashCode(), differentName.hashCode());
  }

  @Test
  public void testReset() {
    GaugeCell gaugeCell = new GaugeCell(MetricName.named("namespace", "name"));
    gaugeCell.set(2);
    assertThat(gaugeCell.getCumulative().value(), equalTo(GaugeData.create(2).value()));

    gaugeCell.reset();
    assertThat(gaugeCell.getCumulative(), equalTo(GaugeData.empty()));
    assertThat(gaugeCell.getDirty(), equalTo(new DirtyState()));
  }
}
