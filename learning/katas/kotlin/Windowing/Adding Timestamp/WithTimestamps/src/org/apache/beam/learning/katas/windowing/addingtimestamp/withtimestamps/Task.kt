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

import org.apache.beam.learning.katas.util.Log
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.WithTimestamps
import org.apache.beam.sdk.values.PCollection
import org.joda.time.DateTime

object Task {
  @JvmStatic
  fun main(args: Array<String>) {
    val options = PipelineOptionsFactory.fromArgs(*args).create()
    val pipeline = Pipeline.create(options)

    val events = pipeline.apply(
      Create.of(
        Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
        Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
        Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
        Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
        Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
      )
    )

    val output = applyTransform(events)

    output.apply(Log.ofElements())

    pipeline.run()
  }

  @JvmStatic
  fun applyTransform(events: PCollection<Event>): PCollection<Event> {
    return events.apply(WithTimestamps.of { event: Event -> event.date.toInstant() })
  }
}