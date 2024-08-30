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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.metrics.MetricName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RemoveSafeDeltaCounterCell}. */
@RunWith(JUnit4.class)
public class RemoveSafeDeltaCounterCellTest {

  @Test
  public void testRemoveSafeDeltaCounterCell_deleteZero() throws Exception {
    ConcurrentHashMap<MetricName, AtomicLong> metric_map = new ConcurrentHashMap<>();
    MetricName metric1 = MetricName.named("namespace", "name_1");

    RemoveSafeDeltaCounterCell cell_1 = new RemoveSafeDeltaCounterCell(metric1, metric_map);
    RemoveSafeDeltaCounterCell cell_2 = new RemoveSafeDeltaCounterCell(metric1, metric_map);

    cell_2.inc(10);
    assertThat(metric_map.get(metric1).get(), equalTo(10L));

    cell_2.reset();
    assertThat(metric_map.get(metric1).get(), equalTo(0L));
    cell_2.deleteIfZero();

    assertThat(metric_map.get(metric1), is(nullValue()));

    // Incrementing a deleted counter will recreate it.
    cell_1.inc(20);
    assertThat(metric_map.get(metric1).get(), equalTo(20L));
  }

  @Test
  public void testRemoveSafeDeltaCounterCell_deleteNonZero() throws Exception {
    ConcurrentHashMap<MetricName, AtomicLong> metric_map = new ConcurrentHashMap<>();
    MetricName metric1 = MetricName.named("namespace", "name_1");

    RemoveSafeDeltaCounterCell cell_1 = new RemoveSafeDeltaCounterCell(metric1, metric_map);
    cell_1.inc(10);

    cell_1.deleteIfZero();
    assertThat(metric_map.size(), equalTo(1));
    assertThat(metric_map.get(metric1).get(), equalTo(10L));
  }

  @Test
  public void testRemoveSafeDeltaCounterCell_multipleUpdates() throws Exception {
    ConcurrentHashMap<MetricName, AtomicLong> metric_map = new ConcurrentHashMap<>();
    MetricName metric1 = MetricName.named("namespace", "name_1");

    RemoveSafeDeltaCounterCell cell_1 = new RemoveSafeDeltaCounterCell(metric1, metric_map);

    cell_1.inc();
    assertThat(metric_map.get(metric1).get(), equalTo(1L));

    cell_1.inc(5);
    assertThat(metric_map.get(metric1).get(), equalTo(6L));

    cell_1.dec(10);
    assertThat(metric_map.get(metric1).get(), equalTo(-4L));

    cell_1.dec();
    assertThat(metric_map.get(metric1).get(), equalTo(-5L));
  }

  @Test
  public void testRemoveSafeDeltaCounterCell_resetNonExistant() throws Exception {
    ConcurrentHashMap<MetricName, AtomicLong> metric_map = new ConcurrentHashMap<>();
    MetricName metric1 = MetricName.named("namespace", "name_1");

    RemoveSafeDeltaCounterCell cell_1 = new RemoveSafeDeltaCounterCell(metric1, metric_map);
    assertThat(metric_map.size(), equalTo(0));

    cell_1.reset();
    assertThat(metric_map.size(), equalTo(0));
  }
}
