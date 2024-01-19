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
package org.apache.beam.sdk.io.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CustomTimestampPolicyWithLimitedDelay}. */
@RunWith(JUnit4.class)
public class CustomTimestampPolicyWithLimitedDelayTest {

  // Takes offsets of timestamps from now returns the results as offsets from 'now'.
  private static List<Long> getTimestampsForRecords(
      TimestampPolicy<String, String> policy, Instant now, List<Long> timestampOffsets) {

    return timestampOffsets.stream()
        .map(
            ts -> {
              Instant result =
                  policy.getTimestampForRecord(
                      null,
                      new KafkaRecord<>(
                          "topic",
                          0,
                          0,
                          now.getMillis() + ts,
                          KafkaTimestampType.CREATE_TIME,
                          new RecordHeaders(),
                          "key",
                          "value"));
              return result.getMillis() - now.getMillis();
            })
        .collect(Collectors.toList());
  }

  @Test
  public void testCustomTimestampPolicyWithLimitedDelay() {
    // Verifies that max delay is applies appropriately for reporting watermark

    Duration maxDelay = Duration.standardSeconds(60);

    CustomTimestampPolicyWithLimitedDelay<String, String> policy =
        new CustomTimestampPolicyWithLimitedDelay<>(
            record -> new Instant(record.getTimestamp()), maxDelay, Optional.empty());

    Instant now = Instant.now();

    TimestampPolicy.PartitionContext ctx = mock(TimestampPolicy.PartitionContext.class);
    when(ctx.getMessageBacklog()).thenReturn(100L);
    when(ctx.getBacklogCheckTime()).thenReturn(now);

    assertThat(policy.getWatermark(ctx), is(BoundedWindow.TIMESTAMP_MIN_VALUE));

    // (1) Test simple case : watermark == max_timesatmp - max_delay

    List<Long> input =
        ImmutableList.of(
            -200_000L, -150_000L, -120_000L, -140_000L, -100_000L, // <<< Max timestamp
            -110_000L);
    assertThat(getTimestampsForRecords(policy, now, input), is(input));

    // Watermark should be max_timestamp - maxDelay
    assertThat(
        policy.getWatermark(ctx), is(now.minus(Duration.standardSeconds(100)).minus(maxDelay)));

    // (2) Verify future timestamps

    input =
        ImmutableList.of(
            -200_000L, -150_000L, -120_000L, -140_000L, 100_000L, // <<< timestamp is in future
            -100_000L, -110_000L);

    assertThat(getTimestampsForRecords(policy, now, input), is(input));

    // Watermark should be now - max_delay (backlog in context still non zero)
    assertThat(policy.getWatermark(ctx, now), is(now.minus(maxDelay)));

    // (3) Verify that Watermark advances when there is no backlog

    // advance current time by 5 minutes
    now = now.plus(Duration.standardMinutes(5));
    Instant backlogCheckTime = now.minus(Duration.standardSeconds(10));

    when(ctx.getMessageBacklog()).thenReturn(0L);
    when(ctx.getBacklogCheckTime()).thenReturn(backlogCheckTime);

    assertThat(policy.getWatermark(ctx, now), is(backlogCheckTime.minus(maxDelay)));
  }
}
