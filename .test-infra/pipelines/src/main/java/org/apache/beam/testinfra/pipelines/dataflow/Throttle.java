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
package org.apache.beam.testinfra.pipelines.dataflow;

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/** Controls the rate of elements. */
@Internal
class Throttle<T> extends PTransform<PCollection<T>, PCollection<T>> {

  /** Control the rate of elements, emitting each element per {@link Duration}. */
  static <T> Throttle<T> of(String name, Duration duration) {
    return new Throttle<>(name, duration);
  }

  private final String tagName;
  private final Duration duration;

  public Throttle(String tagName, Duration duration) {
    this.tagName = tagName;
    this.duration = duration;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    return input
        .apply(
            "Throttle/Window " + tagName,
            Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
        .apply(
            "Throttle/Assign To Bounded Window " + tagName,
            ParDo.of(new AssignToBoundedWindowFn<>()))
        .apply(
            "Throttle/GroupIntoBatches " + tagName,
            GroupIntoBatches.<Long, T>ofSize(1L).withMaxBufferingDuration(duration))
        .apply("Throttle/Extract Values " + tagName, Values.create())
        .apply("Throttle/Flatten " + tagName, Flatten.iterables());
  }

  private static class AssignToBoundedWindowFn<T> extends DoFn<T, KV<Long, T>> {
    @ProcessElement
    public void process(
        @Element T element, BoundedWindow boundedWindow, OutputReceiver<KV<Long, T>> receiver) {
      receiver.output(KV.of(boundedWindow.maxTimestamp().getMillis(), element));
    }
  }
}
