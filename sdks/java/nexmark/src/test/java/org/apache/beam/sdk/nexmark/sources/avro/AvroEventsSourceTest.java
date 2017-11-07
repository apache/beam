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

package org.apache.beam.sdk.nexmark.sources.avro;

import static org.junit.Assert.assertNotNull;

import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link AvroEventsSource}.
 */
public class AvroEventsSourceTest {

  private static final String INPUT_PATH = "inputPath";
  private static final String QUERY_NAME = "queryName";

  private static final Person PERSON =
      new Person(1L, "personName", "email", "cc", "city", "state", 32124112L, "extra");

  private static final Auction AUCTION =
      new Auction(2L, "itemName", "descr", 4342L, 43234L, 8983423L, 9293849234L, 1, 2, "extra1");

  private static final Bid BID =
      new Bid(2L, 1L, 394234L, 2340982L, "extra3");

  private static final Event EVENT_PERSON = new Event(PERSON);
  private static final Event EVENT_AUCTION = new Event(AUCTION);
  private static final Event EVENT_BID = new Event(BID);

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Rule public TestPipeline testPipeline = TestPipeline.create();


  @Test public void testCreate() throws Exception {
    NexmarkOptions options = newOptions();

    PTransform<PBegin, PCollection<Event>> eventsSource = newEventSource(options);

    assertNotNull(eventsSource);
  }

  @Test public void testCreateThrowsForAbsentInputPath() throws Exception {
    thrown.expect(RuntimeException.class);

    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    newEventSource(options);
  }

  @Test public void testReadEvents() {
    NexmarkOptions options = newOptions();
    options.setInputPath("src/test/resources/");

    PTransform<PBegin, PCollection<Event>> eventSource = newEventSource(options);

    PCollection<Event> events = testPipeline.apply(eventSource);

    PAssert
        .that(events)
        .containsInAnyOrder(EVENT_PERSON, EVENT_AUCTION, EVENT_BID);

    testPipeline.run();
  }

  private PTransform<PBegin, PCollection<Event>> newEventSource(NexmarkOptions options) {
    return AvroEventsSource.createSource(options, QUERY_NAME);
  }

  private NexmarkOptions newOptions() {
    NexmarkOptions options = PipelineOptionsFactory.create().as(NexmarkOptions.class);
    options.setInputPath(INPUT_PATH);
    return options;
  }
}
