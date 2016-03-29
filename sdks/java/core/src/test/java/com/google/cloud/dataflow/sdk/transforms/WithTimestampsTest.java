/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.isA;

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link WithTimestamps}.
 */
@RunWith(JUnit4.class)
public class WithTimestampsTest implements Serializable {
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void withTimestampsShouldApplyTimestamps() {
    TestPipeline p = TestPipeline.create();

    SerializableFunction<String, Instant> timestampFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return new Instant(Long.valueOf(input));
          }
        };

    String yearTwoThousand = "946684800000";
    PCollection<String> timestamped =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
         .apply(WithTimestamps.of(timestampFn));

    PCollection<KV<String, Instant>> timestampedVals =
        timestamped.apply(ParDo.of(new DoFn<String, KV<String, Instant>>() {
          @Override
          public void processElement(DoFn<String, KV<String, Instant>>.ProcessContext c)
              throws Exception {
            c.output(KV.of(c.element(), c.timestamp()));
          }
        }));

    DataflowAssert.that(timestamped)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    DataflowAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0)),
            KV.of("1234", new Instant(1234L)),
            KV.of(Integer.toString(Integer.MAX_VALUE), new Instant(Integer.MAX_VALUE)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand))));

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void withTimestampsBackwardsInTimeShouldThrow() {
    TestPipeline p = TestPipeline.create();

    SerializableFunction<String, Instant> timestampFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return new Instant(Long.valueOf(input));
          }
        };
    SerializableFunction<String, Instant> backInTimeFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return new Instant(Long.valueOf(input)).minus(Duration.millis(1000L));
          }
        };


    String yearTwoThousand = "946684800000";

    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
     .apply("WithTimestamps", WithTimestamps.of(timestampFn))
     .apply("AddSkew", WithTimestamps.of(backInTimeFn));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalArgumentException.class));
    thrown.expectMessage("no earlier than the timestamp of the current input");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void withTimestampsBackwardsInTimeAndWithAllowedTimestampSkewShouldSucceed() {
    TestPipeline p = TestPipeline.create();

    SerializableFunction<String, Instant> timestampFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return new Instant(Long.valueOf(input));
          }
        };

    final Duration skew = Duration.millis(1000L);
    SerializableFunction<String, Instant> backInTimeFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return new Instant(Long.valueOf(input)).minus(skew);
          }
        };

    String yearTwoThousand = "946684800000";
    PCollection<String> timestampedWithSkew =
        p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
         .apply("FirstTimestamp", WithTimestamps.of(timestampFn))
         .apply(
             "WithSkew",
             WithTimestamps.of(backInTimeFn).withAllowedTimestampSkew(skew.plus(100L)));

    PCollection<KV<String, Instant>> timestampedVals =
        timestampedWithSkew.apply(ParDo.of(new DoFn<String, KV<String, Instant>>() {
          @Override
          public void processElement(DoFn<String, KV<String, Instant>>.ProcessContext c)
              throws Exception {
            c.output(KV.of(c.element(), c.timestamp()));
          }
        }));

    DataflowAssert.that(timestampedWithSkew)
        .containsInAnyOrder(yearTwoThousand, "0", "1234", Integer.toString(Integer.MAX_VALUE));
    DataflowAssert.that(timestampedVals)
        .containsInAnyOrder(
            KV.of("0", new Instant(0L).minus(skew)),
            KV.of("1234", new Instant(1234L).minus(skew)),
            KV.of(
                Integer.toString(Integer.MAX_VALUE),
                new Instant(Long.valueOf(Integer.MAX_VALUE)).minus(skew)),
            KV.of(yearTwoThousand, new Instant(Long.valueOf(yearTwoThousand)).minus(skew)));

    p.run();
  }

  @Test
  public void withTimestampsWithNullTimestampShouldThrow() {
    SerializableFunction<String, Instant> timestampFn =
        new SerializableFunction<String, Instant>() {
          @Override
          public Instant apply(String input) {
            return null;
          }
        };

    TestPipeline p = TestPipeline.create();
    String yearTwoThousand = "946684800000";
    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE), yearTwoThousand))
     .apply(WithTimestamps.of(timestampFn));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(NullPointerException.class));
    thrown.expectMessage("WithTimestamps");
    thrown.expectMessage("cannot be null");

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void withTimestampsWithNullFnShouldThrowOnConstruction() {
    TestPipeline p = TestPipeline.create();

    SerializableFunction<String, Instant> timestampFn = null;

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("WithTimestamps fn cannot be null");

    p.apply(Create.of("1234", "0", Integer.toString(Integer.MAX_VALUE)))
     .apply(WithTimestamps.of(timestampFn));

    p.run();
  }
}
