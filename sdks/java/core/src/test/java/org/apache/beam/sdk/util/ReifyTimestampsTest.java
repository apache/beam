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

package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ReifyTimestamps}.
 */
@RunWith(JUnit4.class)
public class ReifyTimestampsTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void inValuesSucceeds() {
    PCollection<KV<String, Integer>> timestamped =
        pipeline
            .apply(Create.of(KV.of("foo", 0), KV.of("foo", 1), KV.of("bar", 2), KV.of("baz", 3)))
            .apply(
                WithTimestamps.of(
                    new SerializableFunction<KV<String, Integer>, Instant>() {
                      @Override
                      public Instant apply(KV<String, Integer> input) {
                        return new Instant(input.getValue().longValue());
                      }
                    }));

    PCollection<KV<String, TimestampedValue<Integer>>> reified =
        timestamped.apply(ReifyTimestamps.<String, Integer>inValues());

    PAssert.that(reified)
        .containsInAnyOrder(
            KV.of("foo", TimestampedValue.of(0, new Instant(0))),
            KV.of("foo", TimestampedValue.of(1, new Instant(1))),
            KV.of("bar", TimestampedValue.of(2, new Instant(2))),
            KV.of("baz", TimestampedValue.of(3, new Instant(3))));

    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void extractFromValuesSucceeds() {
    PCollection<KV<String, TimestampedValue<Integer>>> preified =
        pipeline.apply(
            Create.of(
                KV.of("foo", TimestampedValue.of(0, new Instant((0)))),
                KV.of("foo", TimestampedValue.of(1, new Instant(1))),
                KV.of("bar", TimestampedValue.of(2, new Instant(2))),
                KV.of("baz", TimestampedValue.of(3, new Instant(3)))));

    PCollection<KV<String, Integer>> timestamped =
        preified.apply(ReifyTimestamps.<String, Integer>extractFromValues());

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
}
