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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Unit tests for {@link WatermarkAggregator}: aggregation of watermark reports across multiple
 * upstream transforms, each with its own partition set. Per-partition behaviour within one upstream
 * transform (monotonicity, repartition reset) is covered in depth by {@code WatermarkManagerTest};
 * here it is exercised through the aggregate.
 */
public class WatermarkAggregatorTest {

  private static Instant ts(long millis) {
    return new Instant(millis);
  }

  /** A report from the given transform's partition {@code sourcePartition} of {@code total}. */
  private static WatermarkPayload report(
      String transformId, long millis, int sourcePartition, int total) {
    return KStreamsPayload.watermark(millis, transformId, sourcePartition, total).asWatermark();
  }

  @Test
  public void holdsBeforeAnyReport() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a"));
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  @Test
  public void singleUpstreamSinglePartitionAdvances() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a"));
    aggregator.observe(report("a", 100L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));
  }

  @Test
  public void holdsUntilEveryExpectedUpstreamReports() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a", "b"));
    aggregator.observe(report("a", 100L, 0, 1));
    // Only one of the two expected upstream transforms has reported — still holding.
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    aggregator.observe(report("b", 200L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));
  }

  @Test
  public void holdsUntilEveryPartitionOfEachUpstreamReports() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a", "b"));
    aggregator.observe(report("a", 100L, 0, 1));
    aggregator.observe(report("b", 200L, 0, 2));
    // Upstream "b" has reported only one of its two partitions — still holding.
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    aggregator.observe(report("b", 300L, 1, 2));
    // Ready: min(100, min(200, 300)) = 100.
    assertThat(aggregator.advance(), is(ts(100L)));
  }

  @Test
  public void aggregateIsMinAcrossUpstreams() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a", "b", "c"));
    aggregator.observe(report("a", 300L, 0, 1));
    aggregator.observe(report("b", 100L, 0, 1));
    aggregator.observe(report("c", 500L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));

    // The slowest upstream advances; the aggregate follows the new min.
    aggregator.observe(report("b", 400L, 0, 1));
    assertThat(aggregator.advance(), is(ts(300L)));
  }

  @Test
  public void perUpstreamWatermarkIsMonotonic() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a"));
    aggregator.observe(report("a", 200L, 0, 1));
    assertThat(aggregator.advance(), is(ts(200L)));

    // A late lower report from the same upstream partition is ignored.
    aggregator.observe(report("a", 100L, 0, 1));
    assertThat(aggregator.advance(), is(ts(200L)));
  }

  @Test
  public void duplicateReportDoesNotAdvance() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a"));
    aggregator.observe(report("a", 100L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));

    // The same report again (e.g. a broadcast duplicate) leaves the aggregate unchanged.
    aggregator.observe(report("a", 100L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));
  }

  @Test
  public void terminalMaxWatermarkAggregates() {
    long max = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a", "b"));
    aggregator.observe(report("a", max, 0, 1));
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    aggregator.observe(report("b", max, 0, 1));
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  @Test
  public void repartitionOfOneUpstreamReopensTheHold() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a", "b"));
    aggregator.observe(report("a", 100L, 0, 1));
    aggregator.observe(report("b", 200L, 0, 1));
    assertThat(aggregator.advance(), is(ts(100L)));

    // Upstream "b" changes its partition count (repartition): its per-partition state resets and
    // the aggregate holds again until b's new full partition set has reported.
    aggregator.observe(report("b", 250L, 0, 2));
    assertThat(aggregator.advance(), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    aggregator.observe(report("b", 300L, 1, 2));
    assertThat(aggregator.advance(), is(ts(100L)));
  }

  @Test
  public void reportFromUnexpectedTransformFailsFast() {
    WatermarkAggregator aggregator = new WatermarkAggregator(ImmutableSet.of("a"));
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class, () -> aggregator.observe(report("intruder", 100L, 0, 1)));
    assertThat(thrown.getMessage(), containsString("intruder"));
  }

  @Test
  public void emptyExpectedUpstreamsIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> new WatermarkAggregator(ImmutableSet.of()));
  }
}
