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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** Tests {@link WatermarkPolicy}. */
@RunWith(PowerMockRunner.class)
@PrepareForTest(Instant.class)
public class WatermarkPolicyTest {
  private static final Instant NOW = Instant.now();

  @Test
  public void shouldAdvanceWatermarkWithTheArrivalTimeFromKinesisRecords() {
    WatermarkPolicy policy = WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1);
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2);
  }

  @Test
  public void shouldOnlyAdvanceTheWatermark() {
    WatermarkPolicy policy = WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);
    KinesisRecord c = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    Instant time3 = NOW.minus(Duration.standardSeconds(40L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);
    // time3 is before time2
    when(c.getApproximateArrivalTimestamp()).thenReturn(time3);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1);
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2);
    policy.update(c);
    // watermark doesn't go back in time
    assertThat(policy.getWatermark()).isEqualTo(time2);
  }

  @Test
  public void shouldAdvanceWatermarkWhenThereAreNoIncomingRecords() {
    WatermarkParameters standardWatermarkParams = WatermarkParameters.create();
    WatermarkPolicy policy =
        WatermarkPolicyFactory.withCustomWatermarkPolicy(standardWatermarkParams)
            .createWatermarkPolicy();

    mockStatic(Instant.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(500)); // returned when update is called
    Instant time2 =
        NOW.minus(
            Duration.standardSeconds(498)); // returned when getWatermark is called the first time
    Instant time3 = NOW; // returned when getWatermark is called the second time
    Instant arrivalTime = NOW.minus(Duration.standardSeconds(510));
    Duration watermarkIdleTimeThreshold =
        standardWatermarkParams.getWatermarkIdleDurationThreshold();

    when(Instant.now()).thenReturn(time1).thenReturn(time2).thenReturn(time3);

    KinesisRecord a = mock(KinesisRecord.class);
    when(a.getApproximateArrivalTimestamp()).thenReturn(arrivalTime);

    policy.update(a);

    // returns the latest event time when the watermark
    assertThat(policy.getWatermark()).isEqualTo(arrivalTime);
    // advance the watermark to [NOW - watermark idle time threshold]
    assertThat(policy.getWatermark()).isEqualTo(time3.minus(watermarkIdleTimeThreshold));
  }

  @Test
  public void shouldAdvanceWatermarkToNowWithProcessingTimePolicy() {
    WatermarkPolicy policy =
        WatermarkPolicyFactory.withProcessingTimePolicy().createWatermarkPolicy();

    mockStatic(Instant.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(5));
    Instant time2 = NOW.minus(Duration.standardSeconds(4));

    when(Instant.now()).thenReturn(time1).thenReturn(time2);

    assertThat(policy.getWatermark()).isEqualTo(time1);
    assertThat(policy.getWatermark()).isEqualTo(time2);
  }

  @Test
  public void shouldAdvanceWatermarkWithCustomTimePolicy() {
    SerializableFunction<KinesisRecord, Instant> timestampFn =
        (record) -> record.getApproximateArrivalTimestamp().plus(Duration.standardMinutes(1));

    WatermarkPolicy policy =
        WatermarkPolicyFactory.withCustomWatermarkPolicy(
                WatermarkParameters.create().withTimestampFn(timestampFn))
            .createWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(Duration.standardSeconds(30L));
    Instant time2 = NOW.minus(Duration.standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1.plus(Duration.standardMinutes(1)));
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2.plus(Duration.standardMinutes(1)));
  }
}
