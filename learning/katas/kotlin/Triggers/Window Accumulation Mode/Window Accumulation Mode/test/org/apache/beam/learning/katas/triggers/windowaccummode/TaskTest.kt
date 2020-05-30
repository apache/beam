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
package org.apache.beam.learning.katas.triggers.windowaccummode

import org.apache.beam.learning.katas.triggers.windowaccummode.Task.applyTransform
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import java.io.Serializable

class TaskTest : Serializable {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun triggers_window_accumulation_mode_window_accumulation_mode() {
    val testStream = TestStream.create(SerializableCoder.of(String::class.java))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(0)))
      .advanceProcessingTime(Duration.standardSeconds(1))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(1)))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(2)))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(3)))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(4)))
      .advanceProcessingTime(Duration.standardSeconds(1))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(5)))
      .addElements(TimestampedValue.of("event", Instant.ofEpochSecond(6)))
      .advanceWatermarkToInfinity()

    val eventsPColl = testPipeline.apply(testStream)

    val results = applyTransform(eventsPColl)

    PAssert.that(results)
      .inEarlyPane(IntervalWindow(Instant.EPOCH, Instant.parse("1970-01-02T00:00:00+00:00")))
      .containsInAnyOrder(1L, 5L)
      .inFinalPane(IntervalWindow(Instant.EPOCH, Instant.parse("1970-01-02T00:00:00+00:00")))
      .containsInAnyOrder(7L)

    testPipeline.run().waitUntilFinish()
  }
}