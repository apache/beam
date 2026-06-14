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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.OptionalLong;
import org.junit.Test;

/** Tests for {@link WatermarkManager}. */
public class WatermarkManagerTest {

  @Test
  public void holdsBeforeAnyReport() {
    WatermarkManager manager = new WatermarkManager();
    assertThat(manager.isComplete(), is(false));
    assertThat(manager.advance().isPresent(), is(false));
  }

  @Test
  public void holdsUntilEverySourcePartitionReports() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 100L, 4);
    manager.observe(1, 100L, 4);
    manager.observe(2, 100L, 4);
    // Three of four partitions reported — still holding.
    assertThat(manager.isComplete(), is(false));
    assertThat(manager.advance().isPresent(), is(false));

    manager.observe(3, 100L, 4);
    assertThat(manager.isComplete(), is(true));
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void emitsMinAcrossPartitionsOnceComplete() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 300L, 4);
    manager.observe(1, 100L, 4);
    manager.observe(2, 500L, 4);
    manager.observe(3, 200L, 4);
    // min(300, 100, 500, 200) = 100
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void perPartitionWatermarkIsMonotonic() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 100L, 1);
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
    // A lower out-of-order report for the same partition is ignored.
    manager.observe(0, 50L, 1);
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void emittedWatermarkDoesNotRegressWhenNewPartitionReportsOlderWatermark() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 100L, 2);
    manager.observe(1, 100L, 2);
    assertThat(manager.advance(), is(OptionalLong.of(100L)));

    // A repartition grows the source set to 3; a freshly appeared partition reports an older
    // watermark. The stage watermark must not go backwards.
    manager.observe(2, 50L, 3);
    assertThat(manager.isComplete(), is(true));
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void partitionCountIncreaseReopensHold() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 100L, 2);
    manager.observe(1, 100L, 2);
    assertThat(manager.advance(), is(OptionalLong.of(100L)));

    // Source set grows to 4; until partitions 2 and 3 report, the stage holds again.
    manager.observe(2, 200L, 4);
    assertThat(manager.isComplete(), is(false));
    assertThat(manager.advance().isPresent(), is(false));

    manager.observe(3, 200L, 4);
    assertThat(manager.isComplete(), is(true));
    // min(100, 100, 200, 200) = 100, which also satisfies the non-regression clamp.
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void partitionCountDecreasePrunesStalePartitions() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, 100L, 4);
    manager.observe(1, 100L, 4);
    manager.observe(2, 100L, 4);
    manager.observe(3, 50L, 4);
    assertThat(manager.advance(), is(OptionalLong.of(50L)));

    // Source set shrinks to 2; partitions 2 and 3 no longer exist and are dropped. The remaining
    // set {0, 1} is complete and its min is 100, but the output must not regress below 50.
    manager.observe(0, 100L, 2);
    assertThat(manager.expectedSourcePartitionCount(), is(2));
    assertThat(manager.reportedPartitionCount(), is(2));
    assertThat(manager.isComplete(), is(true));
    assertThat(manager.advance(), is(OptionalLong.of(100L)));
  }

  @Test
  public void rejectsNonPositiveTotalSourcePartitions() {
    WatermarkManager manager = new WatermarkManager();
    assertThrows(IllegalArgumentException.class, () -> manager.observe(0, 100L, 0));
    assertThrows(IllegalArgumentException.class, () -> manager.observe(0, 100L, -1));
  }

  @Test
  public void rejectsOutOfRangeSourcePartition() {
    WatermarkManager manager = new WatermarkManager();
    assertThrows(IllegalArgumentException.class, () -> manager.observe(-1, 100L, 4));
    assertThrows(IllegalArgumentException.class, () -> manager.observe(4, 100L, 4));
  }
}
