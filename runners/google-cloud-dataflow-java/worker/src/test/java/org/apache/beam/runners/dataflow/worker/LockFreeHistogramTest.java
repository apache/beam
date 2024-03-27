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

import java.util.Optional;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.ImmutableLongArray;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LockFreeHistogram}. */
@RunWith(JUnit4.class)
public class LockFreeHistogramTest {

  @Test
  public void testUpdate_OverflowValues() {
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 10, 3);
    LockFreeHistogram histogram =
        new LockFreeHistogram(KV.of(MetricName.named("name", "namespace"), bucketType));
    histogram.update(35, 40, 45);
    Optional<LockFreeHistogram.Snapshot> snapshot = histogram.getSnapshotAndReset();

    LockFreeHistogram.OutlierStatistic expectedOverflow =
        LockFreeHistogram.OutlierStatistic.create(120.0, 3L);
    LockFreeHistogram.OutlierStatistic expectedUnderflow = LockFreeHistogram.OutlierStatistic.EMPTY;
    ImmutableLongArray expectedBuckets = ImmutableLongArray.of(0L, 0L, 0L);
    LockFreeHistogram.Snapshot expectedSnapshot =
        LockFreeHistogram.Snapshot.create(
            expectedUnderflow, expectedOverflow, expectedBuckets, bucketType);

    assertThat(snapshot.isPresent(), equalTo(true));
    assertThat(snapshot.get(), equalTo(expectedSnapshot));
    assertThat(snapshot.get().underflowStatistic().mean(), equalTo(0.0));
    assertThat(snapshot.get().overflowStatistic(), equalTo(expectedOverflow));
  }

  @Test
  public void testUpdate_UnderflowValues() {
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(100, 10, 3);
    LockFreeHistogram histogram =
        new LockFreeHistogram(KV.of(MetricName.named("name", "namespace"), bucketType));
    histogram.update(35, 40, 45);
    Optional<LockFreeHistogram.Snapshot> snapshot = histogram.getSnapshotAndReset();

    LockFreeHistogram.OutlierStatistic expectedUnderflow =
        LockFreeHistogram.OutlierStatistic.create(120.0, 3L);
    LockFreeHistogram.OutlierStatistic expectedOverflow = LockFreeHistogram.OutlierStatistic.EMPTY;
    ImmutableLongArray expectedBuckets = ImmutableLongArray.of(0L, 0L, 0L);
    LockFreeHistogram.Snapshot expectedSnapshot =
        LockFreeHistogram.Snapshot.create(
            expectedUnderflow, expectedOverflow, expectedBuckets, bucketType);

    assertThat(snapshot.isPresent(), equalTo(true));
    assertThat(snapshot.get(), equalTo(expectedSnapshot));
    assertThat(snapshot.get().underflowStatistic(), equalTo(expectedUnderflow));
  }

  @Test
  public void testUpdate_InBoundsValues() {
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 10, 3);
    LockFreeHistogram histogram =
        new LockFreeHistogram(KV.of(MetricName.named("name", "namespace"), bucketType));
    histogram.update(5, 15, 25);
    Optional<LockFreeHistogram.Snapshot> snapshot = histogram.getSnapshotAndReset();

    LockFreeHistogram.OutlierStatistic expectedOverflow = LockFreeHistogram.OutlierStatistic.EMPTY;
    LockFreeHistogram.OutlierStatistic expectedUnderflow = LockFreeHistogram.OutlierStatistic.EMPTY;
    ImmutableLongArray expectedBuckets = ImmutableLongArray.of(1L, 1L, 1L);
    LockFreeHistogram.Snapshot expectedSnapshot =
        LockFreeHistogram.Snapshot.create(
            expectedUnderflow, expectedOverflow, expectedBuckets, bucketType);

    assertThat(snapshot.isPresent(), equalTo(true));
    assertThat(snapshot.get(), equalTo(expectedSnapshot));
  }

  @Test
  public void testUpdate_EmptySnapshot() {
    HistogramData.BucketType bucketType = HistogramData.LinearBuckets.of(0, 10, 3);
    LockFreeHistogram histogram =
        new LockFreeHistogram(KV.of(MetricName.named("name", "namespace"), bucketType));
    histogram.update(5, 15, 25);
    Optional<LockFreeHistogram.Snapshot> snapshot_1 = histogram.getSnapshotAndReset();

    assertThat(snapshot_1.isPresent(), equalTo(true));

    Optional<LockFreeHistogram.Snapshot> snapshot_2 = histogram.getSnapshotAndReset();
    assertThat(snapshot_2.isPresent(), equalTo(false));
  }
}
