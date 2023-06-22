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

package org.apache.beam.learning.katas.triggers.eventtimetriggers;

// beam-playground:
//   name: EventTimeTriggers
//   description: Task from katas to count events with event time triggers
//   multifile: true
//   context_line: 49
//   categories:
//     - Streaming
//   complexity: MEDIUM
//   tags:
//     - count
//     - windowing
//     - triggers
//     - event

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> events =
        pipeline.apply(GenerateEvent.everySecond());

    PCollection<Long> output = applyTransform(events);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Long> applyTransform(PCollection<String> events) {
    return events
        .apply(
            Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .triggering(AfterWatermark.pastEndOfWindow())
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())

        .apply(Combine.globally(Count.<String>combineFn()).withoutDefaults());
  }

}