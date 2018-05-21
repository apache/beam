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
package org.apache.beam.sdk.nexmark.sources;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.generator.Generator;
import org.apache.beam.sdk.nexmark.sources.generator.GeneratorConfig;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test {@link BoundedEventSource}. */
@RunWith(JUnit4.class)
public class EventGeneratorDoFnTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public final transient TestPipeline sourcePipeline = TestPipeline.create();

  @Test
  public void testNumGeneratedEvents() {
    final int numEvents = 100;
    GeneratorConfig config =
        new GeneratorConfig(
            NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, numEvents, 0);
    PCollection<Event> events =
        p.apply(Create.of((Void) null)).apply(ParDo.of(new EventGeneratorDoFn(config)));
    PAssert.thatSingleton(events.apply("Count All", Count.globally())).isEqualTo((long) numEvents);
    p.run().waitUntilFinish();
  }

  @Test
  public void testGeneratesSameEvents() {
    final int numEvents = 100;
    GeneratorConfig config =
        new GeneratorConfig(
            NexmarkConfiguration.DEFAULT, System.currentTimeMillis(), 0, numEvents, 0);
    List<Event> eventsFromGenerator = generateEvents(config);

    PCollection<Event> eventsFromParDo =
        p.apply(Create.of((Void) null)).apply(ParDo.of(new EventGeneratorDoFn(config)));
    PAssert.that(eventsFromParDo).containsInAnyOrder(eventsFromGenerator);
    p.run().waitUntilFinish();

    PCollection<Event> eventsFromSource =
        sourcePipeline.apply(Read.from(new BoundedEventSource(config, 1)));
    PAssert.that(eventsFromSource).containsInAnyOrder(eventsFromGenerator);
    sourcePipeline.run().waitUntilFinish();

    p.run().waitUntilFinish();
  }

  private static List<Event> generateEvents(GeneratorConfig config) {
    Generator generator = new Generator(config);
    List<Event> events = new ArrayList<>();
    while (generator.hasNext()) {
      final TimestampedValue<Event> timestampedEvent = generator.next();
      events.add(timestampedEvent.getValue());
    }
    return events;
  }
}
