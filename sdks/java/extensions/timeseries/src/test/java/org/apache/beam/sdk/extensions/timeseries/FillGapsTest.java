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
package org.apache.beam.sdk.extensions.timeseries;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesLoopingTimer;
import org.apache.beam.sdk.testing.UsesStatefulParDo;
import org.apache.beam.sdk.testing.UsesStrictTimerOrdering;
import org.apache.beam.sdk.testing.UsesTimersInParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FillGapsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Message {
    abstract String getKey();

    abstract String getValue();

    abstract Instant getTimestamp();

    static Message update(FillGaps.InterpolateData<Message> interpolateData) {
      Message value = interpolateData.getValue().getValue();
      Instant nextWindowMax = interpolateData.getNextWindow().maxTimestamp();
      return value.toBuilder().setTimestamp(nextWindowMax).build();
    }

    static Message of(String key, String value, Instant timestamp) {
      return new AutoValue_FillGapsTest_Message.Builder()
          .setKey(key)
          .setValue(value)
          .setTimestamp(timestamp)
          .build();
    }

    static TimestampedValue<Message> ofTimestamped(String key, String value, Instant timestamp) {
      return TimestampedValue.of(of(key, value, timestamp), timestamp);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setKey(String key);

      abstract Builder setValue(String value);

      abstract Builder setTimestamp(Instant timestamp);

      abstract Message build();
    }

    abstract Builder toBuilder();
  }

  @Test
  public void testFillGaps() {
    List<TimestampedValue<Message>> values =
        ImmutableList.of(
            Message.ofTimestamped("key1", "value0<", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key2", "value0", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key1", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key1", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key2", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key2", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key1", "value3", Instant.ofEpochSecond(3)),
            Message.ofTimestamped("key2", "value3", Instant.ofEpochSecond(3)));

    PCollection<Message> input = pipeline.apply(Create.timestamped(values));
    PCollection<TimestampedValue<Message>> gapFilled =
        input
            .apply(
                FillGaps.<Message>of(Duration.standardSeconds(1), "key")
                    .withStopTime(Instant.ofEpochSecond(5)))
            .apply(Reify.timestamps());

    FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(1));
    PAssert.that(gapFilled)
        .containsInAnyOrder(
            Iterables.concat(
                values,
                ImmutableList.of(
                    TimestampedValue.of(
                        Message.of(
                            "key1", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of(
                            "key2", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key1", "value3", Instant.ofEpochSecond(3)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value3", Instant.ofEpochSecond(3)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()))));
    pipeline.run();
  }

  @Test
  public void testFillGapsKeepEarliest() {
    List<TimestampedValue<Message>> values =
        ImmutableList.of(
            Message.ofTimestamped("key1", "value0<", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key2", "value0", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key1", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key1", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key2", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key2", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key1", "value3", Instant.ofEpochSecond(3)),
            Message.ofTimestamped("key2", "value3", Instant.ofEpochSecond(3)));

    PCollection<Message> input = pipeline.apply(Create.timestamped(values));
    PCollection<TimestampedValue<Message>> gapFilled =
        input
            .apply(
                FillGaps.<Message>of(Duration.standardSeconds(1), "key")
                    .withMergeFunction(FillGaps.keepEarliest())
                    .withStopTime(Instant.ofEpochSecond(5)))
            .apply(Reify.timestamps());

    FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(1));
    PAssert.that(gapFilled)
        .containsInAnyOrder(
            Iterables.concat(
                values,
                ImmutableList.of(
                    TimestampedValue.of(
                        Message.of("key1", "value1<", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value1<", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key1", "value3", Instant.ofEpochSecond(3)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value3", Instant.ofEpochSecond(3)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()))));
    pipeline.run();
  }

  @Test
  public void testFillGapsMaxDuration() {
    List<TimestampedValue<Message>> values =
        ImmutableList.of(
            Message.ofTimestamped("key1", "value0", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key2", "value0", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key1", "value1", Instant.ofEpochSecond(1)),
            Message.ofTimestamped("key2", "value1", Instant.ofEpochSecond(1)),
            Message.ofTimestamped("key1", "value3", Instant.ofEpochSecond(10)),
            Message.ofTimestamped("key2", "value3", Instant.ofEpochSecond(10)));

    PCollection<Message> input = pipeline.apply(Create.timestamped(values));
    PCollection<TimestampedValue<Message>> gapFilled =
        input
            .apply(
                FillGaps.<Message>of(Duration.standardSeconds(1), "key")
                    .withMaxGapFillBuckets(4L)
                    .withStopTime(Instant.ofEpochSecond(11)))
            .apply(Reify.timestamps());

    FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(1));
    PAssert.that(gapFilled)
        .containsInAnyOrder(
            Iterables.concat(
                values,
                ImmutableList.of(
                    TimestampedValue.of(
                        Message.of("key1", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key1", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(3)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key1", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key1", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(5)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(3)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp()),
                    TimestampedValue.of(
                        Message.of("key2", "value1", Instant.ofEpochSecond(1)),
                        fixedWindows.assignWindow(Instant.ofEpochSecond(5)).maxTimestamp()))));
    pipeline.run();
  }

  @Test
  public void testFillGapsPropagateFunction() {
    List<TimestampedValue<Message>> values =
        ImmutableList.of(
            Message.ofTimestamped("key1", "value0<", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key2", "value0", Instant.ofEpochSecond(0)),
            Message.ofTimestamped("key1", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key1", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key2", "value1<", Instant.ofEpochSecond(1)),
            Message.ofTimestamped(
                "key2", "value1", Instant.ofEpochSecond(1).plus(Duration.millis(1))),
            Message.ofTimestamped("key1", "value3", Instant.ofEpochSecond(3)),
            Message.ofTimestamped("key2", "value3", Instant.ofEpochSecond(3)));

    PCollection<Message> input = pipeline.apply(Create.timestamped(values));
    PCollection<TimestampedValue<Message>> gapFilled =
        input
            .apply(
                FillGaps.<Message>of(Duration.standardSeconds(1), "key")
                    .withInterpolateFunction(Message::update)
                    .withStopTime(Instant.ofEpochSecond(5)))
            .apply(Reify.timestamps());

    FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(1));
    Instant bucketTwoMax = fixedWindows.assignWindow(Instant.ofEpochSecond(2)).maxTimestamp();
    Instant bucketFourMax = fixedWindows.assignWindow(Instant.ofEpochSecond(4)).maxTimestamp();

    PAssert.that(gapFilled)
        .containsInAnyOrder(
            Iterables.concat(
                values,
                ImmutableList.of(
                    Message.ofTimestamped("key1", "value1", bucketTwoMax),
                    Message.ofTimestamped("key2", "value1", bucketTwoMax),
                    Message.ofTimestamped("key1", "value3", bucketFourMax),
                    Message.ofTimestamped("key2", "value3", bucketFourMax))));
    pipeline.run();
  }

  // TODO: This test fails due to DirectRunner bugs. Uncomment once those bugs are fixed.
  @Test
  @Category({
    UsesTimersInParDo.class,
    UsesLoopingTimer.class,
    UsesStatefulParDo.class,
    UsesStrictTimerOrdering.class
  })
  public void testFillGapsFuzz() {
    for (int i = 0; i < 6; ++i) {
      fuzzTest(10, 500, 25, 20);
    }
  }

  public void fuzzTest(int numKeys, int numBuckets, int maxGapSizeToGenerate, long maxGapSize) {
    Pipeline p = Pipeline.create();
    List<TimestampedValue<Message>> values = Lists.newArrayList();
    List<TimestampedValue<Message>> expectedGaps = Lists.newArrayList();
    for (int i = 0; i < numKeys; ++i) {
      String key = "key" + i;
      generateFuzzTimerseries(
          key, numBuckets, maxGapSizeToGenerate, maxGapSize, values, expectedGaps);
    }

    PCollection<Message> input = p.apply(Create.timestamped(values));
    PCollection<TimestampedValue<Message>> gapFilled =
        input
            .apply(
                FillGaps.<Message>of(Duration.standardSeconds(1), "key")
                    .withInterpolateFunction(Message::update)
                    .withMaxGapFillBuckets(maxGapSize)
                    .withStopTime(Instant.ofEpochSecond(numBuckets)))
            .apply(Reify.timestamps());

    PAssert.that(gapFilled).containsInAnyOrder(Iterables.concat(values, expectedGaps));
    p.run();
  }

  void generateFuzzTimerseries(
      String key,
      int numBuckets,
      int maxGapSizeToGenerate,
      long maxGapSize,
      List<TimestampedValue<Message>> values,
      List<TimestampedValue<Message>> expectedGaps) {
    Random random = new Random();
    String lastValue = null;
    int currentGapSize = 0;
    for (int bucket = 0; bucket < numBuckets; ) {
      if (lastValue != null && currentGapSize < maxGapSizeToGenerate && random.nextInt(10) == 0) {
        // 10% chance of creating a gap.
        int gapSize = random.nextInt(maxGapSizeToGenerate) + 1;
        int lastGapBucket = Math.min(bucket + gapSize, numBuckets);

        for (; bucket < lastGapBucket; ++bucket) {
          if (currentGapSize < maxGapSize) {
            addBucketToTimeseries(key, lastValue, bucket, expectedGaps);
          }
          ++currentGapSize;
        }
      } else {
        lastValue = "bucket" + bucket;
        currentGapSize = 0;
        addBucketToTimeseries(key, lastValue, bucket, values);
        ++bucket;
      }
    }
  }

  void addBucketToTimeseries(
      String key, String value, int bucket, List<TimestampedValue<Message>> list) {
    FixedWindows fixedWindows = FixedWindows.of(Duration.standardSeconds(1));
    BoundedWindow currentBucket = fixedWindows.assignWindow(Instant.ofEpochSecond(bucket));
    TimestampedValue<Message> message =
        Message.ofTimestamped(key, value, currentBucket.maxTimestamp());
    list.add(message);
  }
}
