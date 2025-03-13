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

package org.apache.beam.learning.katas.windowing.addingtimestamp.pardo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class TaskTest implements Serializable {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.create();

  @SuppressWarnings("unchecked")
  @Test
  public void windowing_addingTimeStamp_parDo() {
    List<Event> events = Arrays.asList(
        new Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
        new Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
        new Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
        new Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
        new Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
    );

    PCollection<Event> eventsPColl = testPipeline.apply(Create.of(events));

    PCollection<Event> results = Task.applyTransform(eventsPColl);

    PCollection<KV<Event, Instant>> timestampedResults =
        results.apply("KV<Event, Instant>",
            ParDo.of(new DoFn<Event, KV<Event, Instant>>() {

              @ProcessElement
              public void processElement(@Element Event event, ProcessContext context,
                  OutputReceiver<KV<Event, Instant>> out) {

                out.output(KV.of(event, context.timestamp()));
              }

            })
        );

    PAssert.that(results)
        .containsInAnyOrder(events);

    PAssert.that(timestampedResults)
        .containsInAnyOrder(
            KV.of(events.get(0), events.get(0).getDate().toInstant()),
            KV.of(events.get(1), events.get(1).getDate().toInstant()),
            KV.of(events.get(2), events.get(2).getDate().toInstant()),
            KV.of(events.get(3), events.get(3).getDate().toInstant()),
            KV.of(events.get(4), events.get(4).getDate().toInstant())
        );

    testPipeline.run().waitUntilFinish();
  }

}