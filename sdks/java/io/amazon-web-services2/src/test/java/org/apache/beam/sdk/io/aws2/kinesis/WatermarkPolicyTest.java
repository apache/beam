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
import static org.joda.time.Duration.standardSeconds;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Test;

/** Tests {@link WatermarkPolicy}. */
public class WatermarkPolicyTest {
  private static final Instant NOW = Instant.now();

  @After
  public void resetJodaTime() {
    // Some tests set the time source for joda time to a fixed timestamp.
    // This makes sure time is reset to the system time source afterwards.
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void shouldAdvanceWatermarkWithTheArrivalTimeFromKinesisRecords() {
    WatermarkPolicy policy = WatermarkPolicyFactory.withArrivalTimePolicy().createWatermarkPolicy();

    KinesisRecord a = mock(KinesisRecord.class);
    KinesisRecord b = mock(KinesisRecord.class);

    Instant time1 = NOW.minus(standardSeconds(30L));
    Instant time2 = NOW.minus(standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    assertThat(policy.getWatermark()).isEqualTo(BoundedWindow.TIMESTAMP_MIN_VALUE);
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

    Instant time1 = NOW.minus(standardSeconds(30L));
    Instant time2 = NOW.minus(standardSeconds(20L));
    Instant time3 = NOW.minus(standardSeconds(40L));
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

    Duration watermarkIdleTimeThreshold =
        standardWatermarkParams.getWatermarkIdleDurationThreshold();

    KinesisRecord a = mock(KinesisRecord.class);
    Instant arrivalTime = NOW.minus(standardSeconds(510));
    when(a.getApproximateArrivalTimestamp()).thenReturn(arrivalTime);

    DateTimeUtils.setCurrentMillisFixed(NOW.minus(standardSeconds(500)).getMillis());
    policy.update(a);

    // returns the latest event time when the watermark
    DateTimeUtils.setCurrentMillisFixed(NOW.minus(standardSeconds(498)).getMillis());
    assertThat(policy.getWatermark()).isEqualTo(arrivalTime);

    // advance the watermark to [NOW - watermark idle time threshold]
    DateTimeUtils.setCurrentMillisFixed(NOW.getMillis());
    assertThat(policy.getWatermark()).isEqualTo(NOW.minus(watermarkIdleTimeThreshold));
  }

  @Test
  public void shouldAdvanceWatermarkToNowWithProcessingTimePolicy() {
    WatermarkPolicy policy =
        WatermarkPolicyFactory.withProcessingTimePolicy().createWatermarkPolicy();

    Instant time1 = NOW.minus(standardSeconds(5));
    Instant time2 = NOW.minus(standardSeconds(4));

    DateTimeUtils.setCurrentMillisFixed(time1.getMillis());
    assertThat(policy.getWatermark()).isEqualTo(time1);

    DateTimeUtils.setCurrentMillisFixed(time2.getMillis());
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

    Instant time1 = NOW.minus(standardSeconds(30L));
    Instant time2 = NOW.minus(standardSeconds(20L));
    when(a.getApproximateArrivalTimestamp()).thenReturn(time1);
    when(b.getApproximateArrivalTimestamp()).thenReturn(time2);

    policy.update(a);
    assertThat(policy.getWatermark()).isEqualTo(time1.plus(Duration.standardMinutes(1)));
    policy.update(b);
    assertThat(policy.getWatermark()).isEqualTo(time2.plus(Duration.standardMinutes(1)));
  }

  @Test
  public void shouldUpdateWatermarkParameters() {
    SerializableFunction<KinesisRecord, Instant> fn = input -> Instant.now();
    Duration idleDurationThreshold = Duration.standardSeconds(30);

    WatermarkParameters parameters =
        WatermarkParameters.create()
            .withTimestampFn(fn)
            .withWatermarkIdleDurationThreshold(idleDurationThreshold);

    assertThat(parameters.getTimestampFn()).isEqualTo(fn);
    assertThat(parameters.getWatermarkIdleDurationThreshold()).isEqualTo(idleDurationThreshold);
  }
}
