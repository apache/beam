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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@link StringSetCell}. */
public class StringSetCellTest {
  private final StringSetCell cell = new StringSetCell(MetricName.named("lineage", "sources"));

  @Test
  public void testDeltaAndCumulative() {
    cell.add("pubsub");
    cell.add("bq", "spanner");
    assertEquals(cell.getCumulative().stringSet(), ImmutableSet.of("spanner", "pubsub", "bq"));
    assertEquals(
        "getCumulative is idempotent",
        cell.getCumulative().stringSet(),
        ImmutableSet.of("spanner", "pubsub", "bq"));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    cell.add("gcs");
    assertEquals(
        cell.getCumulative().stringSet(), ImmutableSet.of("spanner", "pubsub", "bq", "gcs"));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }

  @Test
  public void testEquals() {
    StringSetCell stringSetCell = new StringSetCell(MetricName.named("namespace", "name"));
    StringSetCell equal = new StringSetCell(MetricName.named("namespace", "name"));
    assertEquals(stringSetCell, equal);
    assertEquals(stringSetCell.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    StringSetCell stringSetCell = new StringSetCell(MetricName.named("namespace", "name"));

    Assert.assertNotEquals(stringSetCell, new Object());

    StringSetCell differentDirty = new StringSetCell(MetricName.named("namespace", "name"));
    differentDirty.getDirty().afterModification();
    Assert.assertNotEquals(stringSetCell, differentDirty);
    Assert.assertNotEquals(stringSetCell.hashCode(), differentDirty.hashCode());

    StringSetCell differentSetValues = new StringSetCell(MetricName.named("namespace", "name"));
    differentSetValues.update(StringSetData.create(ImmutableSet.of("hello")));
    Assert.assertNotEquals(stringSetCell, differentSetValues);
    Assert.assertNotEquals(stringSetCell.hashCode(), differentSetValues.hashCode());

    StringSetCell differentName = new StringSetCell(MetricName.named("DIFFERENT", "DIFFERENT"));
    Assert.assertNotEquals(stringSetCell, differentName);
    Assert.assertNotEquals(stringSetCell.hashCode(), differentName.hashCode());
  }

  @Test
  public void testReset() {
    StringSetCell stringSetCell = new StringSetCell(MetricName.named("namespace", "name"));
    stringSetCell.add("hello");
    Assert.assertNotEquals(stringSetCell.getDirty(), new DirtyState());
    assertThat(
        stringSetCell.getCumulative().stringSet(),
        equalTo(StringSetData.create(ImmutableSet.of("hello")).stringSet()));

    stringSetCell.reset();
    assertThat(stringSetCell.getCumulative(), equalTo(StringSetData.empty()));
    assertThat(stringSetCell.getDirty(), equalTo(new DirtyState()));
  }
}
