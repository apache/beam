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

import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HistogramCell}. */
@RunWith(JUnit4.class)
public class HistogramCellTest {
  private HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 10, 100);

  @Test
  public void testDeltaAndCumulative() {
    HistogramCell cell = new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    cell.update(5);
    cell.update(7);
    HistogramData expected = HistogramData.linear(0, 10, 100);
    expected.record(5, 7);
    assertThat(cell.getCumulative(), equalTo(expected));
    assertThat("getCumulative is idempotent", cell.getCumulative(), equalTo(expected));

    assertThat(cell.getDirty().beforeCommit(), equalTo(true));
    cell.getDirty().afterCommit();
    assertThat(cell.getDirty().beforeCommit(), equalTo(false));

    cell.update(30);
    expected.record(30);
    assertThat(cell.getCumulative(), equalTo(expected));

    assertThat(
        "Adding a new value made the cell dirty", cell.getDirty().beforeCommit(), equalTo(true));
  }

  @Test
  public void testEquals() {
    HistogramCell cell = new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    HistogramCell equalCell =
        new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    Assert.assertEquals(equalCell, cell);
    Assert.assertEquals(equalCell.hashCode(), cell.hashCode());
  }

  @Test
  public void testUpdateWithHistogramData() {
    HistogramCell cell = new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    HistogramData data = HistogramData.linear(0, 10, 100);
    data.record(5, 7, 9);
    cell.update(data);
    assertThat(cell.getCumulative(), equalTo(data));
  }

  @Test
  public void testNotEquals() {
    HistogramCell cell = new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    Assert.assertNotEquals(cell, new Object());

    HistogramCell differentDirty =
        new HistogramCell(KV.of(MetricName.named("namespace", "name"), bucketType));
    differentDirty.getDirty().afterModification();
    Assert.assertNotEquals(cell, differentDirty);
    Assert.assertNotEquals(cell.hashCode(), differentDirty.hashCode());

    HistogramCell differentValue =
        new HistogramCell(KV.of(MetricName.named("namespace", "name"), bucketType));
    differentValue.update(1);
    Assert.assertNotEquals(cell, differentValue);
    Assert.assertNotEquals(cell.hashCode(), differentValue.hashCode());

    HistogramCell differentName =
        new HistogramCell(KV.of(MetricName.named("DIFFERENT", "DIFFERENT"), bucketType));
    Assert.assertNotEquals(cell, differentName);
    Assert.assertNotEquals(cell.hashCode(), differentName.hashCode());

    HistogramCell differentBuckets =
        new HistogramCell(
            KV.of(
                MetricName.named("DIFFERENT", "DIFFERENT"),
                HistogramData.LinearBuckets.of(1, 10, 100)));
    Assert.assertNotEquals(cell, differentBuckets);
    Assert.assertNotEquals(cell.hashCode(), differentBuckets.hashCode());
  }

  @Test
  public void testReset() {
    HistogramCell cell = new HistogramCell(KV.of(MetricName.named("hello", "world"), bucketType));
    cell.update(2);
    Assert.assertEquals(1, cell.getCumulative().getTotalCount());
    Assert.assertNotEquals(cell.getDirty(), new DirtyState());

    cell.reset();
    assertThat(cell.getCumulative(), equalTo(HistogramData.linear(0, 10, 100)));
    assertThat(cell.getDirty(), equalTo(new DirtyState()));
  }
}
