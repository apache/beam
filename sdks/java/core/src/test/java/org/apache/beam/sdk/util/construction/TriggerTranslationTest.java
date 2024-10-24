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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterEach;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterSynchronizedProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for utilities in {@link TriggerTranslation}. */
@RunWith(Parameterized.class)
public class TriggerTranslationTest {

  @AutoValue
  abstract static class ToProtoAndBackSpec {
    abstract Trigger getTrigger();
  }

  private static ToProtoAndBackSpec toProtoAndBackSpec(Trigger trigger) {
    return new AutoValue_TriggerTranslationTest_ToProtoAndBackSpec(trigger);
  }

  @Parameters(name = "{index}: {0}")
  public static Iterable<ToProtoAndBackSpec> data() {
    return ImmutableList.of(
        // Atomic triggers
        toProtoAndBackSpec(AfterWatermark.pastEndOfWindow()),
        toProtoAndBackSpec(AfterPane.elementCountAtLeast(73)),
        toProtoAndBackSpec(AfterSynchronizedProcessingTime.ofFirstElement()),
        toProtoAndBackSpec(Never.ever()),
        toProtoAndBackSpec(DefaultTrigger.of()),
        toProtoAndBackSpec(AfterProcessingTime.pastFirstElementInPane()),
        toProtoAndBackSpec(
            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(23))),
        toProtoAndBackSpec(
            AfterProcessingTime.pastFirstElementInPane()
                .alignedTo(Duration.millis(5), new Instant(27))),
        toProtoAndBackSpec(
            AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardSeconds(3))
                .alignedTo(Duration.millis(5), new Instant(27))
                .plusDelayOf(Duration.millis(13))),

        // Composite triggers

        toProtoAndBackSpec(
            AfterAll.of(AfterPane.elementCountAtLeast(79), AfterWatermark.pastEndOfWindow())),
        toProtoAndBackSpec(
            AfterEach.inOrder(AfterPane.elementCountAtLeast(79), AfterPane.elementCountAtLeast(3))),
        toProtoAndBackSpec(
            AfterFirst.of(AfterWatermark.pastEndOfWindow(), AfterPane.elementCountAtLeast(3))),
        toProtoAndBackSpec(
            AfterWatermark.pastEndOfWindow().withEarlyFirings(AfterPane.elementCountAtLeast(3))),
        toProtoAndBackSpec(
            AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(3))),
        toProtoAndBackSpec(
            AfterWatermark.pastEndOfWindow()
                .withEarlyFirings(
                    AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.millis(42)))
                .withLateFirings(AfterPane.elementCountAtLeast(3))),
        toProtoAndBackSpec(Repeatedly.forever(AfterWatermark.pastEndOfWindow())),
        toProtoAndBackSpec(
            Repeatedly.forever(AfterPane.elementCountAtLeast(1))
                .orFinally(AfterWatermark.pastEndOfWindow())));
  }

  @Parameter(0)
  public ToProtoAndBackSpec toProtoAndBackSpec;

  @Test
  public void testToProtoAndBack() throws Exception {
    Trigger trigger = toProtoAndBackSpec.getTrigger();
    Trigger toProtoAndBackTrigger =
        TriggerTranslation.fromProto(TriggerTranslation.toProto(trigger));

    assertThat(toProtoAndBackTrigger, equalTo(trigger));
  }
}
