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
package org.apache.beam.learning.katas.windowing.addingtimestamp.withtimestamps

import org.apache.beam.learning.katas.windowing.addingtimestamp.withtimestamps.Task.applyTransform
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.joda.time.DateTime
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import java.io.Serializable

class TaskTest : Serializable {
  @get:Rule
  @Transient
  val testPipeline: TestPipeline = TestPipeline.create()

  @Test
  fun windowing_adding_timestamp_withtimestamps() {
    val events = listOf(
      Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
      Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
      Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
      Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
      Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
    )

    val eventsPColl = testPipeline.apply(Create.of(events))

    val results = applyTransform(eventsPColl)

    val timestampedResults = results.apply(
      "KV<Event, Instant>",
      ParDo.of(object : DoFn<Event, KV<Event, Instant>>() {
        @ProcessElement
        fun processElement(context: ProcessContext, out: OutputReceiver<KV<Event, Instant>>) {
          val event = context.element()
          out.output(KV.of(event, context.timestamp()))
        }
      })
    )

    PAssert.that(results).containsInAnyOrder(events)

    PAssert.that(timestampedResults).containsInAnyOrder(
      KV.of(events[0], events[0].date.toInstant()),
      KV.of(events[1], events[1].date.toInstant()),
      KV.of(events[2], events[2].date.toInstant()),
      KV.of(events[3], events[3].date.toInstant()),
      KV.of(events[4], events[4].date.toInstant())
    )

    testPipeline.run().waitUntilFinish()
  }
}