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
package org.apache.beam.learning.katas.triggers.eventtimetriggers

import org.apache.beam.learning.katas.triggers.eventtimetriggers.Task.applyTransform
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test

class TaskTest {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun triggers_event_time_triggers_event_time_triggers() {
    val testStream = TestStream.create(SerializableCoder.of(String::class.java))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:01+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:02+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:03+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:04+00:00")))
      .advanceWatermarkTo(Instant.parse("2019-06-01T00:00:05+00:00"))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:05+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:06+00:00")))
      .addElements(TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:07+00:00")))
      .advanceWatermarkTo(Instant.parse("2019-06-01T00:00:10+00:00"))
      .advanceWatermarkToInfinity()

    val eventsPColl = testPipeline.apply(testStream)

    val results = applyTransform(eventsPColl)

    PAssert.that(results)
      .inWindow(createIntervalWindow("2019-06-01T00:00:00+00:00", "2019-06-01T00:00:05+00:00"))
      .containsInAnyOrder(5L)
      .inWindow(createIntervalWindow("2019-06-01T00:00:05+00:00", "2019-06-01T00:00:10+00:00"))
      .containsInAnyOrder(3L)

    testPipeline.run().waitUntilFinish()
  }

  private fun createIntervalWindow(startStr: String, endStr: String): IntervalWindow {
    return IntervalWindow(Instant.parse(startStr), Instant.parse(endStr))
  }
}