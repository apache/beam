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

package org.apache.beam.sdk.nexmark.sources.pubsub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link MessageTransforms}.
 */
public class MessageTransformsTest {

  private static final Person PERSON =
      new Person(1L, "personName", "email", "cc", "city", "state", 0L, "extra");
  private static final Event EVENT_PERSON = new Event(PERSON);
  private static final PubsubMessage PUBSUB_MESSAGE = newPubsubMessage(EVENT_PERSON);

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  /**
   * Equality check for PubsubMessages.
   */
  public static class PubsubsMessageEquals
      implements SerializableFunction<Iterable<PubsubMessage>, Void> {

    private byte[] expectedMessagePayload;
    private Map<String, String> expectedMessageAttributes;

    public PubsubsMessageEquals(PubsubMessage message) {
      this.expectedMessagePayload = message.getPayload();
      this.expectedMessageAttributes = message.getAttributeMap();
    }

    @Override
    public Void apply(Iterable<PubsubMessage> input) {
      PubsubMessage generatedMessage = Iterables.getOnlyElement(input);
      assertTrue(Arrays.equals(expectedMessagePayload, generatedMessage.getPayload()));
      assertEquals(expectedMessageAttributes, generatedMessage.getAttributeMap());
      return null;
    }
  }

  @Test public void testEventToMessage() throws Exception {
    PCollection<Event> events = testPipeline.apply(testEventStream());

    ParDo.SingleOutput<Event, PubsubMessage> transformToPubsub = MessageTransforms.eventToMessage();

    PCollection<PubsubMessage> pubsubMessages = events
        .apply(transformToPubsub)
        .apply(Window.<PubsubMessage> into(FixedWindows.of(Duration.millis(10))));

    PAssert
      .that(pubsubMessages)
      .inWindow(intervalWindowOfMs(10L))
      .satisfies(messageEqualsTo(PUBSUB_MESSAGE));

    testPipeline.run();
  }

  @Test public void testMessageToEvent() throws Exception {
    PCollection<PubsubMessage> messages = testPipeline.apply(testPubsubStream());

    ParDo.SingleOutput<PubsubMessage, Event> transfrmToMessage = MessageTransforms.messageToEvent();

    PCollection<Event> pubsubMessages = messages
        .apply(transfrmToMessage)
        .apply(Window.<Event> into(FixedWindows.of(Duration.millis(10))));

    PAssert
        .that(pubsubMessages)
        .inWindow(intervalWindowOfMs(10L))
        .containsInAnyOrder(EVENT_PERSON);

    testPipeline.run();
  }

  private PubsubsMessageEquals messageEqualsTo(PubsubMessage expectedPubsubMessage) {
    return new PubsubsMessageEquals(expectedPubsubMessage);
  }

  private BoundedWindow intervalWindowOfMs(long l) {
    return new IntervalWindow(new Instant(0), new Instant(l));
  }

  private TestStream<Event> testEventStream() {
    return TestStream
        .create(Event.CODER)
        .addElements(TimestampedValue.of(EVENT_PERSON, new Instant(0L)))
        .advanceWatermarkToInfinity();
  }

  private TestStream<PubsubMessage> testPubsubStream() {
    return TestStream
        .create(PubsubMessageWithAttributesCoder.of())
        .addElements(TimestampedValue.of(PUBSUB_MESSAGE, new Instant(0L)))
        .advanceWatermarkToInfinity();
  }

  private static PubsubMessage newPubsubMessage(Event eventPerson) {
    try {
      return new PubsubMessage(
          CoderUtils.encodeToByteArray(Event.CODER, eventPerson),
          new HashMap<String, String>());
    } catch (CoderException e) {
      throw new RuntimeException("Coder exception", e);
    }
  }
}
