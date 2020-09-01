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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Latest} {@link PTransform} and {@link Combine.CombineFn}. */
@RunWith(JUnit4.class)
public class LatestTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyEventTimestamp() {
    PCollection<String> output =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of("foo", new Instant(100)),
                    TimestampedValue.of("bar", new Instant(300)),
                    TimestampedValue.of("baz", new Instant(200))))
            .apply(Latest.globally());

    PAssert.that(output).containsInAnyOrder("bar");
    p.run();
  }

  @Test
  public void testGloballyOutputCoder() {
    p.enableAbandonedNodeEnforcement(false);

    BigEndianLongCoder inputCoder = BigEndianLongCoder.of();

    PCollection<Long> output =
        p.apply(Create.of(1L, 2L).withCoder(inputCoder)).apply(Latest.globally());

    Coder<Long> outputCoder = output.getCoder();
    assertThat(outputCoder, instanceOf(NullableCoder.class));
    assertEquals(inputCoder, ((NullableCoder<?>) outputCoder).getValueCoder());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGloballyEmptyCollection() {
    PCollection<Long> emptyInput = p.apply(Create.empty(VarLongCoder.of()));
    PCollection<Long> output = emptyInput.apply(Latest.globally());

    PAssert.that(output).containsInAnyOrder((Long) null);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPerKeyEventTimestamp() {
    PCollection<KV<String, String>> output =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("A", "foo"), new Instant(100)),
                    TimestampedValue.of(KV.of("B", "bar"), new Instant(300)),
                    TimestampedValue.of(KV.of("A", "baz"), new Instant(200))))
            .apply(Latest.perKey());

    PAssert.that(output).containsInAnyOrder(KV.of("B", "bar"), KV.of("A", "baz"));
    p.run();
  }

  @Test
  public void testPerKeyOutputCoder() {
    p.enableAbandonedNodeEnforcement(false);

    KvCoder<String, Long> inputCoder = KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of());

    PCollection<KV<String, Long>> output =
        p.apply(Create.of(KV.of("foo", 1L)).withCoder(inputCoder)).apply(Latest.perKey());

    assertEquals("Should use input coder for outputs", inputCoder, output.getCoder());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPerKeyEmptyCollection() {
    PCollection<KV<String, String>> output =
        p.apply(Create.empty(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(Latest.perKey());

    PAssert.that(output).empty();
    p.run();
  }

  /** Helper method to easily create a timestamped value. */
  private static TimestampedValue<Long> timestamped(Instant timestamp) {
    return TimestampedValue.of(uniqueLong.incrementAndGet(), timestamp);
  }

  private static final AtomicLong uniqueLong = new AtomicLong();
}
