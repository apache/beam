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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.UsesTestStream;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Utility transforms for reifying implicit context into explicit fields. */
@RunWith(JUnit4.class)
public class ReifyTest implements Serializable {
  public static final WithTimestamps<KV<String, Integer>> TIMESTAMP_FROM_V =
      WithTimestamps.of(input -> new Instant(input.getValue().longValue()));
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void extractFromValuesSucceeds() {
    PCollection<KV<String, TimestampedValue<Integer>>> preified =
        pipeline.apply(
            Create.of(
                KV.of("foo", TimestampedValue.of(0, new Instant(0))),
                KV.of("foo", TimestampedValue.of(1, new Instant(1))),
                KV.of("bar", TimestampedValue.of(2, new Instant(2))),
                KV.of("baz", TimestampedValue.of(3, new Instant(3)))));

    PCollection<KV<String, Integer>> timestamped =
        preified.apply(Reify.extractTimestampsFromValues());

    PAssert.that(timestamped)
        .containsInAnyOrder(KV.of("foo", 0), KV.of("foo", 1), KV.of("bar", 2), KV.of("baz", 3));

    timestamped.apply(
        "AssertElementTimestamps",
        ParDo.of(
            new DoFn<KV<String, Integer>, Void>() {
              @ProcessElement
              public void verifyTimestampsEqualValue(ProcessContext context) {
                assertThat(
                    new Instant(context.element().getValue().longValue()),
                    equalTo(context.timestamp()));
              }
            }));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void extractFromValuesWhenValueTimestampedLaterSucceeds() {
    PCollection<KV<String, TimestampedValue<Integer>>> preified =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of(
                    KV.of("foo", TimestampedValue.of(0, new Instant(0))), new Instant(100)),
                TimestampedValue.of(
                    KV.of("foo", TimestampedValue.of(1, new Instant(1))), new Instant(101L)),
                TimestampedValue.of(
                    KV.of("bar", TimestampedValue.of(2, new Instant(2))), new Instant(102L)),
                TimestampedValue.of(
                    KV.of("baz", TimestampedValue.of(3, new Instant(3))), new Instant(103L))));

    PCollection<KV<String, Integer>> timestamped =
        preified.apply(ReifyTimestamps.extractFromValues());

    PAssert.that(timestamped)
        .containsInAnyOrder(KV.of("foo", 0), KV.of("foo", 1), KV.of("bar", 2), KV.of("baz", 3));

    timestamped.apply(
        "AssertElementTimestamps",
        ParDo.of(
            new DoFn<KV<String, Integer>, Void>() {
              @ProcessElement
              public void verifyTimestampsEqualValue(ProcessContext context) {
                assertThat(
                    new Instant(context.element().getValue().longValue()),
                    equalTo(context.timestamp()));
              }
            }));

    pipeline.run();
  }

  @Test
  @Category({NeedsRunner.class, UsesTestStream.class})
  public void globalWindowNoKeys() {
    PCollection<ValueInSingleWindow<String>> result =
        pipeline
            .apply(
                TestStream.create(StringUtf8Coder.of())
                    .addElements(TimestampedValue.of("dei", new Instant(123L)))
                    .advanceWatermarkToInfinity())
            .apply(Reify.windows());
    PAssert.that(result)
        .containsInAnyOrder(
            ValueInSingleWindow.of(
                "dei", new Instant(123L), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING));
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void timestampedValuesSucceeds() {
    PCollection<KV<String, Integer>> timestamped =
        pipeline
            .apply(Create.of(KV.of("foo", 0), KV.of("foo", 1), KV.of("bar", 2), KV.of("baz", 3)))
            .apply(TIMESTAMP_FROM_V);

    PCollection<KV<String, TimestampedValue<Integer>>> reified =
        timestamped.apply(Reify.timestampsInValue());

    PAssert.that(reified)
        .containsInAnyOrder(
            KV.of("foo", TimestampedValue.of(0, new Instant(0))),
            KV.of("foo", TimestampedValue.of(1, new Instant(1))),
            KV.of("bar", TimestampedValue.of(2, new Instant(2))),
            KV.of("baz", TimestampedValue.of(3, new Instant(3))));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void timestampsSucceeds() {
    PCollection<String> timestamped =
        pipeline.apply(
            Create.timestamped(
                TimestampedValue.of("foo", new Instant(0L)),
                TimestampedValue.of("bar", new Instant(1L))));

    PCollection<TimestampedValue<String>> reified = timestamped.apply(Reify.timestamps());

    PAssert.that(reified)
        .containsInAnyOrder(
            TimestampedValue.of("foo", new Instant(0)), TimestampedValue.of("bar", new Instant(1)));

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void windowsInValueSucceeds() {
    PCollection<KV<String, Integer>> timestamped =
        pipeline
            .apply(Create.of(KV.of("foo", 0), KV.of("foo", 1), KV.of("bar", 2), KV.of("baz", 3)))
            .apply(TIMESTAMP_FROM_V);

    PCollection<KV<String, ValueInSingleWindow<Integer>>> reified =
        timestamped.apply(Reify.windowsInValue());

    PAssert.that(reified)
        .containsInAnyOrder(
            KV.of(
                "foo",
                ValueInSingleWindow.of(
                    0, new Instant(0), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)),
            KV.of(
                "foo",
                ValueInSingleWindow.of(
                    1, new Instant(1), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)),
            KV.of(
                "bar",
                ValueInSingleWindow.of(
                    2, new Instant(2), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)),
            KV.of(
                "baz",
                ValueInSingleWindow.of(
                    3, new Instant(3), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING)));

    pipeline.run();
  }
}
