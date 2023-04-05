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
package org.apache.beam.learning.katas.windowing.fixedwindow

import org.apache.beam.learning.katas.windowing.fixedwindow.Task.applyTransform
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import java.io.Serializable

class TaskTest : Serializable {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun windowing_fixed_window_time_fixed_window_time() {
    val eventsPColl = testPipeline.apply(
      Create.timestamped(
        TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-01T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-05T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-05T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-08T00:00:00+00:00")),
        TimestampedValue.of("event", Instant.parse("2019-06-10T00:00:00+00:00"))
      )
    )

    val results = applyTransform(eventsPColl)

    val windowedResults = results.apply(
      "WindowedEvent",
      ParDo.of(object : DoFn<KV<String, Long>, WindowedEvent>() {
        @ProcessElement
        fun processElement(
          @Element element: KV<String, Long>,
          window: BoundedWindow, out: OutputReceiver<WindowedEvent>
        ) {

          out.output(WindowedEvent(element.key, element.value, window.toString()))
        }
      })
    )

    PAssert.that(windowedResults).containsInAnyOrder(
      WindowedEvent("event", 4L, "[2019-06-01T00:00:00.000Z..2019-06-02T00:00:00.000Z)"),
      WindowedEvent("event", 2L, "[2019-06-05T00:00:00.000Z..2019-06-06T00:00:00.000Z)"),
      WindowedEvent("event", 3L, "[2019-06-08T00:00:00.000Z..2019-06-09T00:00:00.000Z)"),
      WindowedEvent("event", 1L, "[2019-06-10T00:00:00.000Z..2019-06-11T00:00:00.000Z)")
    )

    testPipeline.run().waitUntilFinish()
  }
}