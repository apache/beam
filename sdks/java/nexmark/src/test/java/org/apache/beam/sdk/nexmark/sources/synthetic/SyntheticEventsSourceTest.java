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

package org.apache.beam.sdk.nexmark.sources.synthetic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.sources.synthetic.generator.GeneratorConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link SyntheticEventsSource}.
 */
public class SyntheticEventsSourceTest {

  private static final String QUERY_NAME = "queryName";
  private static final long EVENTS_COUNT = 5000L;
  private static final boolean STREAMING = true;
  private static final boolean NOT_STREAMING = false;

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test public void testCreatesSyntheticBatchSource() throws Exception {
    NexmarkConfiguration configuration = newConfig();
    NexmarkOptions options = newOptions(NOT_STREAMING);

    PTransform<PBegin, PCollection<Event>> eventSource = newEventSource(configuration, options);

    assertNotNull(eventSource);
    assertEquals(QUERY_NAME + ".ReadBounded", eventSource.getName());
    assertTrue(eventSource instanceof SyntheticEventsSource);
  }

  @Test public void testCreatesSyntheticStreamingSource() throws Exception {
    NexmarkConfiguration configuration = newConfig();
    NexmarkOptions options = newOptions(STREAMING);

    PTransform<PBegin, PCollection<Event>> eventSource = newEventSource(configuration, options);

    assertNotNull(eventSource);
    assertEquals(QUERY_NAME + ".ReadUnbounded", eventSource.getName());
    assertTrue(eventSource instanceof SyntheticEventsSource);
  }

  @Test public void testBatch() {
    NexmarkConfiguration configuration = newConfig();
    NexmarkOptions options = newOptions(NOT_STREAMING);

    PTransform<PBegin, PCollection<Event>> eventSource = newEventSource(configuration, options);
    PCollection<Event> generatedEvents = testPipeline.apply(eventSource);

    assertEquals(PCollection.IsBounded.BOUNDED, generatedEvents.isBounded());

    PCollection<Long> eventsCount = generatedEvents.apply(countEvents());
    PAssert.thatSingleton(eventsCount).isEqualTo(EVENTS_COUNT);

    testPipeline.run();
  }

  @Test public void testStreaming() {
    NexmarkConfiguration configuration = newConfig();
    NexmarkOptions options = newOptions(STREAMING);

    PTransform<PBegin, PCollection<Event>> eventSource = newEventSource(configuration, options);
    PCollection<Event> generatedEvents = testPipeline.apply(eventSource);
    assertEquals(PCollection.IsBounded.UNBOUNDED, generatedEvents.isBounded());

    PCollection<Long> eventsCount = generatedEvents
        .apply(windowByLastEventTime(configuration))
        .apply(countEvents());
    PAssert.that(eventsCount).containsInAnyOrder(EVENTS_COUNT);

    testPipeline.run();
  }

  private Combine.Globally<Event, Long> countEvents() {
    return Combine.globally(Count.<Event> combineFn()).withoutDefaults();
  }

  private Window<Event> windowByLastEventTime(NexmarkConfiguration configuration) {
    GeneratorConfig generatorConfig = NexmarkUtils.standardGeneratorConfig(configuration);

    long lastEventTime = generatorConfig
        .timestampAndInterEventDelayUsForEvent(EVENTS_COUNT - 1).getKey();

    Duration windowDuration = Duration.millis(lastEventTime - NexmarkUtils.BASE_TIME);
    Window<Event> allEventsWindow = Window.into(FixedWindows.of(windowDuration));
    return allEventsWindow;
  }

  private NexmarkOptions newOptions(boolean isStreaming) {
    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setStreaming(isStreaming);
    return options;
  }

  private PTransform<PBegin, PCollection<Event>> newEventSource(
      NexmarkConfiguration configuration,
      NexmarkOptions options) {

    return SyntheticEventsSource.create(configuration, options, QUERY_NAME);
  }

  private NexmarkConfiguration newConfig() {
    NexmarkConfiguration configuration = new NexmarkConfiguration();
    configuration.sourceType = NexmarkUtils.SourceType.DIRECT;
    configuration.numEvents = EVENTS_COUNT;
    return configuration;
  }
}
