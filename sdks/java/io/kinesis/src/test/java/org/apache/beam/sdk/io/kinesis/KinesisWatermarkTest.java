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
package org.apache.beam.sdk.io.kinesis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.BooleanSupplier;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/** Tests {@link KinesisWatermark}. */
@RunWith(MockitoJUnitRunner.class)
public class KinesisWatermarkTest {
  private static final BooleanSupplier SHARDS_UP_TO_DATE = () -> true;
  private static final BooleanSupplier SHARDS_NOT_UP_TO_DATE = () -> false;
  private static final BooleanSupplier SHARDS_IRRELEVANT =
      () -> {
        throw new AssertionError("Shard status should not be queried");
      };
  private final Instant now = Instant.now();
  private KinesisWatermark watermark;

  @Before
  public void setUp() {
    setCurrentTimeTo(now);
    watermark = new KinesisWatermark();
  }

  @After
  public void tearDown() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void watermarkStartsAtSamplePeriodBehindNowIfShardsUpToDate() {
    assertThat(watermark.getCurrent(SHARDS_UP_TO_DATE))
        .isEqualTo(now.minus(KinesisWatermark.SAMPLE_PERIOD));
  }

  @Test
  public void watermarkStartsWithMinIfShardsNotUpToDate() {
    Instant minKinesisWatermark = now.minus(KinesisWatermark.MAX_KINESIS_STREAM_RETENTION_PERIOD);

    assertThat(watermark.getCurrent(SHARDS_NOT_UP_TO_DATE)).isEqualTo(minKinesisWatermark);
  }

  @Test
  public void watermarkIsUpdatedToFirstRecordTimestamp() {
    Instant firstTimestamp = now.minus(Duration.standardHours(1));

    watermark.update(firstTimestamp);

    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);
  }

  @Test
  public void watermarkIsUpdatedToRecentRecordTimestampIfItIsOlderThanUpdateThreshold() {
    Instant firstTimestamp = now.minus(Duration.standardHours(1));
    watermark.update(firstTimestamp);
    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);

    Instant timeAfterWatermarkUpdateThreshold =
        now.plus(KinesisWatermark.UPDATE_THRESHOLD.plus(Duration.millis(1)));
    setCurrentTimeTo(timeAfterWatermarkUpdateThreshold);
    Instant nextTimestamp = timeAfterWatermarkUpdateThreshold.plus(Duration.millis(1));
    watermark.update(nextTimestamp);

    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(nextTimestamp);
  }

  @Test
  public void watermarkDoesNotChangeWhenTooFewSampleRecordsInSamplePeriod() {
    Instant firstTimestamp = now.minus(Duration.standardHours(1));
    watermark.update(firstTimestamp);
    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);

    setCurrentTimeTo(now.plus(KinesisWatermark.SAMPLE_PERIOD));
    watermark.update(firstTimestamp);
    for (int i = 1; i <= KinesisWatermark.MIN_MESSAGES / 2; ++i) {
      Instant plus = firstTimestamp.plus(Duration.millis(i));
      watermark.update(plus);
    }

    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);
  }

  @Test
  public void watermarkAdvancesWhenEnoughRecordsReadRecently() {
    Instant firstTimestamp = now.minus(Duration.standardHours(1));
    watermark.update(firstTimestamp);
    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);

    Instant newTimestamp = firstTimestamp.plus(Duration.millis(1));
    setCurrentTimeTo(now.plus(KinesisWatermark.SAMPLE_PERIOD));

    for (int i = 0; i < KinesisWatermark.MIN_MESSAGES - 1; ++i) {
      watermark.update(newTimestamp.plus(Duration.millis(i)));
      assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(firstTimestamp);
    }

    watermark.update(newTimestamp.plus(Duration.millis(KinesisWatermark.MIN_MESSAGES - 1)));
    assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(newTimestamp);
  }

  @Test
  public void watermarkDoesNotGoBackward() {
    watermark.update(now);
    for (int i = 0; i <= KinesisWatermark.MIN_MESSAGES * 2; ++i) {
      watermark.update(now.minus(Duration.millis(i)));
      assertThat(watermark.getCurrent(SHARDS_IRRELEVANT)).isEqualTo(now);
    }
  }

  private static void setCurrentTimeTo(Instant time) {
    DateTimeUtils.setCurrentMillisFixed(time.getMillis());
  }
}
