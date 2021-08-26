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

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Event> events =
        pipeline.apply(
            Create.of(
                new Event("1", "book-order", DateTime.parse("2019-06-01T00:00:00+00:00")),
                new Event("2", "pencil-order", DateTime.parse("2019-06-02T00:00:00+00:00")),
                new Event("3", "paper-order", DateTime.parse("2019-06-03T00:00:00+00:00")),
                new Event("4", "pencil-order", DateTime.parse("2019-06-04T00:00:00+00:00")),
                new Event("5", "book-order", DateTime.parse("2019-06-05T00:00:00+00:00"))
            )
        );

    PCollection<Event> output = applyTransform(events);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Event> applyTransform(PCollection<Event> events) {
    return events.apply(ParDo.of(new DoFn<Event, Event>() {

      @ProcessElement
      public void processElement(@Element Event event, OutputReceiver<Event> out) {
        out.outputWithTimestamp(event, event.getDate().toInstant());
      }

    }));
  }

}