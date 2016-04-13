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
package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import com.google.api.client.testing.http.FixedClock;
import com.google.api.client.util.Clock;
import com.google.api.services.pubsub.model.PubsubMessage;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;

import javax.annotation.Nullable;

/**
 * Tests for PubsubIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class PubsubIOTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPubsubIOGetName() {
    assertEquals("PubsubIO.Read",
        PubsubIO.Read.topic("projects/myproject/topics/mytopic").getName());
    assertEquals("PubsubIO.Write",
        PubsubIO.Write.topic("projects/myproject/topics/mytopic").getName());
    assertEquals("ReadMyTopic",
        PubsubIO.Read.named("ReadMyTopic").topic("projects/myproject/topics/mytopic").getName());
    assertEquals("WriteMyTopic",
        PubsubIO.Write.named("WriteMyTopic").topic("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.Read.topic("projects/my-project/topics/abc");
    PubsubIO.Read.topic("projects/my-project/topics/ABC");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-DeF");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234");
    PubsubIO.Read.topic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("11111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.Read.topic(new StringBuilder().append("projects/my-project/topics/A-really-long-one-")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("111111111111111111111111111111111111111111111111111111111111111111111111111111111")
        .append("1111111111111111111111111111111111111111111111111111111111111111111111111111")
        .toString());
  }

  /**
   * Helper function that creates a {@link PubsubMessage} with the given timestamp registered as
   * an attribute with the specified label.
   *
   * <p>If {@code label} is {@code null}, then the attributes are {@code null}.
   *
   * <p>Else, if {@code timestamp} is {@code null}, then attributes are present but have no key for
   * the label.
   */
  private static PubsubMessage messageWithTimestamp(
      @Nullable String label, @Nullable String timestamp) {
    PubsubMessage message = new PubsubMessage();
    if (label == null) {
      message.setAttributes(null);
      return message;
    }

    message.setAttributes(new HashMap<String, String>());

    if (timestamp == null) {
      return message;
    }

    message.getAttributes().put(label, timestamp);
    return message;
  }

  /**
   * Helper function that parses the given string to a timestamp through the PubSubIO plumbing.
   */
  private static Instant parseTimestamp(@Nullable String timestamp) {
    PubsubMessage message = messageWithTimestamp("mylabel", timestamp);
    return PubsubIO.assignMessageTimestamp(message, "mylabel", Clock.SYSTEM);
  }

  @Test
  public void noTimestampLabelReturnsNow() {
    final long time = 987654321L;
    Instant timestamp = PubsubIO.assignMessageTimestamp(
        messageWithTimestamp(null, null), null, new FixedClock(time));

    assertEquals(new Instant(time), timestamp);
  }

  @Test
  public void timestampLabelWithNullAttributesThrowsError() {
    PubsubMessage message = messageWithTimestamp(null, null);
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a timestamp in label: myLabel");

    PubsubIO.assignMessageTimestamp(message, "myLabel", Clock.SYSTEM);
  }

  @Test
  public void timestampLabelSetWithMissingAttributeThrowsError() {
    PubsubMessage message = messageWithTimestamp("notMyLabel", "ignored");
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a timestamp in label: myLabel");

    PubsubIO.assignMessageTimestamp(message, "myLabel", Clock.SYSTEM);
  }

  @Test
  public void timestampLabelParsesMillisecondsSinceEpoch() {
    Long millis = 1446162101123L;
    assertEquals(new Instant(millis), parseTimestamp(millis.toString()));
  }

  @Test
  public void timestampLabelParsesRfc3339Seconds() {
    String rfc3339 = "2015-10-29T23:41:41Z";
    assertEquals(Instant.parse(rfc3339), parseTimestamp(rfc3339));
  }

  @Test
  public void timestampLabelParsesRfc3339Tenths() {
    String rfc3339tenths = "2015-10-29T23:41:41.1Z";
    assertEquals(Instant.parse(rfc3339tenths), parseTimestamp(rfc3339tenths));
  }

  @Test
  public void timestampLabelParsesRfc3339Hundredths() {
    String rfc3339hundredths = "2015-10-29T23:41:41.12Z";
    assertEquals(Instant.parse(rfc3339hundredths), parseTimestamp(rfc3339hundredths));
  }

  @Test
  public void timestampLabelParsesRfc3339Millis() {
    String rfc3339millis = "2015-10-29T23:41:41.123Z";
    assertEquals(Instant.parse(rfc3339millis), parseTimestamp(rfc3339millis));
  }

  @Test
  public void timestampLabelParsesRfc3339Micros() {
    String rfc3339micros = "2015-10-29T23:41:41.123456Z";
    assertEquals(Instant.parse(rfc3339micros), parseTimestamp(rfc3339micros));
    // Note: micros part 456/1000 is dropped.
    assertEquals(Instant.parse("2015-10-29T23:41:41.123Z"), parseTimestamp(rfc3339micros));
  }

  @Test
  public void timestampLabelParsesRfc3339MicrosRounding() {
    String rfc3339micros = "2015-10-29T23:41:41.123999Z";
    assertEquals(Instant.parse(rfc3339micros), parseTimestamp(rfc3339micros));
    // Note: micros part 999/1000 is dropped, not rounded up.
    assertEquals(Instant.parse("2015-10-29T23:41:41.123Z"), parseTimestamp(rfc3339micros));
  }

  @Test
  public void timestampLabelWithInvalidFormatThrowsError() {
    thrown.expect(NumberFormatException.class);
    parseTimestamp("not-a-timestamp");
  }

  @Test
  public void timestampLabelWithInvalidFormat2ThrowsError() {
    thrown.expect(NumberFormatException.class);
    parseTimestamp("null");
  }

  @Test
  public void timestampLabelWithInvalidFormat3ThrowsError() {
    thrown.expect(NumberFormatException.class);
    parseTimestamp("2015-10");
  }

  @Test
  public void timestampLabelParsesRfc3339WithSmallYear() {
    // Google and JodaTime agree on dates after 1582-10-15, when the Gregorian Calendar was adopted
    // This is therefore a "small year" until this difference is reconciled.
    String rfc3339SmallYear = "1582-10-15T01:23:45.123Z";
    assertEquals(Instant.parse(rfc3339SmallYear), parseTimestamp(rfc3339SmallYear));
  }

  @Test
  public void timestampLabelParsesRfc3339WithLargeYear() {
    // Year 9999 in range.
    String rfc3339LargeYear = "9999-10-29T23:41:41.123999Z";
    assertEquals(Instant.parse(rfc3339LargeYear), parseTimestamp(rfc3339LargeYear));
  }

  @Test
  public void timestampLabelRfc3339WithTooLargeYearThrowsError() {
    thrown.expect(NumberFormatException.class);
    // Year 10000 out of range.
    parseTimestamp("10000-10-29T23:41:41.123999Z");
  }
}
