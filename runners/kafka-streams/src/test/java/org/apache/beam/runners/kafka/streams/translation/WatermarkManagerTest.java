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

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link WatermarkManager}. */
public class WatermarkManagerTest {

  private static Instant ts(long millis) {
    return new Instant(millis);
  }

  @Test
  public void holdsBeforeAnyReport() {
    WatermarkManager manager = new WatermarkManager();
    assertThat(manager.isReady(), is(false));
    assertThat(manager.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  @Test
  public void holdsUntilEverySourcePartitionReports() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(100L), 4);
    manager.observe(1, ts(100L), 4);
    manager.observe(2, ts(100L), 4);
    // Three of four partitions reported — still holding.
    assertThat(manager.isReady(), is(false));
    assertThat(manager.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    manager.observe(3, ts(100L), 4);
    assertThat(manager.isReady(), is(true));
    assertThat(manager.advance(), is(ts(100L)));
  }

  @Test
  public void emitsMinAcrossPartitionsOnceReady() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(300L), 4);
    manager.observe(1, ts(100L), 4);
    manager.observe(2, ts(500L), 4);
    manager.observe(3, ts(200L), 4);
    // min(300, 100, 500, 200) = 100
    assertThat(manager.advance(), is(ts(100L)));
  }

  @Test
  public void perPartitionWatermarkIsMonotonic() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(100L), 1);
    assertThat(manager.advance(), is(ts(100L)));
    // A lower out-of-order report for the same partition is ignored.
    manager.observe(0, ts(50L), 1);
    assertThat(manager.advance(), is(ts(100L)));
  }

  @Test
  public void partitionCountChangeClearsReportsAndReopensHold() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(100L), 2);
    manager.observe(1, ts(100L), 2);
    assertThat(manager.advance(), is(ts(100L)));

    // Source set grows to 4. The previous reports are dropped, so the stage holds again until all
    // four of the new partition set have reported.
    manager.observe(0, ts(200L), 4);
    assertThat(manager.reportedPartitionCount(), is(1));
    assertThat(manager.isReady(), is(false));
    assertThat(manager.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    manager.observe(1, ts(200L), 4);
    manager.observe(2, ts(200L), 4);
    manager.observe(3, ts(200L), 4);
    assertThat(manager.isReady(), is(true));
    assertThat(manager.advance(), is(ts(200L)));
  }

  @Test
  public void emittedWatermarkDoesNotRegressAfterRepartition() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(100L), 2);
    manager.observe(1, ts(100L), 2);
    assertThat(manager.advance(), is(ts(100L)));

    // After a repartition to 3, the new partitions report older watermarks. Once ready again the
    // min is 50, but the emitted stage watermark must not go backwards below 100.
    manager.observe(0, ts(50L), 3);
    manager.observe(1, ts(50L), 3);
    manager.observe(2, ts(50L), 3);
    assertThat(manager.isReady(), is(true));
    assertThat(manager.advance(), is(ts(100L)));
  }

  @Test
  public void partitionCountDecreaseClearsAndRecomputes() {
    WatermarkManager manager = new WatermarkManager();
    manager.observe(0, ts(100L), 4);
    manager.observe(1, ts(100L), 4);
    manager.observe(2, ts(100L), 4);
    manager.observe(3, ts(50L), 4);
    assertThat(manager.advance(), is(ts(50L)));

    // Source set shrinks to 2. Reports are cleared; the stage holds until {0, 1} report again.
    manager.observe(0, ts(100L), 2);
    assertThat(manager.expectedSourcePartitionCount(), is(2));
    assertThat(manager.reportedPartitionCount(), is(1));
    assertThat(manager.isReady(), is(false));

    manager.observe(1, ts(100L), 2);
    assertThat(manager.isReady(), is(true));
    // min is 100; the non-regression clamp keeps it >= the previously emitted 50.
    assertThat(manager.advance(), is(ts(100L)));
  }

  @Test
  public void rejectsNullWatermark() {
    WatermarkManager manager = new WatermarkManager();
    assertThrows(IllegalArgumentException.class, () -> manager.observe(0, null, 4));
  }

  @Test
  public void rejectsNonPositiveTotalSourcePartitions() {
    WatermarkManager manager = new WatermarkManager();
    assertThrows(IllegalArgumentException.class, () -> manager.observe(0, ts(100L), 0));
    assertThrows(IllegalArgumentException.class, () -> manager.observe(0, ts(100L), -1));
  }

  @Test
  public void rejectsOutOfRangeSourcePartition() {
    WatermarkManager manager = new WatermarkManager();
    assertThrows(IllegalArgumentException.class, () -> manager.observe(-1, ts(100L), 4));
    assertThrows(IllegalArgumentException.class, () -> manager.observe(4, ts(100L), 4));
  }
}
