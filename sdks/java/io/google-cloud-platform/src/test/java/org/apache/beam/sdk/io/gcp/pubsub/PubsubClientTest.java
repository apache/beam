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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for helper classes and methods in PubsubClient. */
@RunWith(JUnit4.class)
public class PubsubClientTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  //
  // Timestamp handling
  //

  private long parse(String timestamp) {
    Map<String, String> map = ImmutableMap.of("myAttribute", timestamp);
    return PubsubClient.extractTimestamp("myAttribute", null, map);
  }

  private void roundTripRfc339(String timestamp) {
    assertEquals(Instant.parse(timestamp).getMillis(), parse(timestamp));
  }

  private void truncatedRfc339(String timestamp, String truncatedTimestmap) {
    assertEquals(Instant.parse(truncatedTimestmap).getMillis(), parse(timestamp));
  }

  @Test
  public void noTimestampAttributeReturnsPubsubPublish() {
    final long time = 987654321L;
    long timestamp = PubsubClient.extractTimestamp(null, String.valueOf(time), null);
    assertEquals(time, timestamp);
  }

  @Test
  public void noTimestampAttributeAndInvalidPubsubPublishThrowsError() {
    thrown.expect(NumberFormatException.class);
    PubsubClient.extractTimestamp(null, "not-a-date", null);
  }

  @Test
  public void timestampAttributeWithNullAttributesThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    PubsubClient.extractTimestamp("myAttribute", null, null);
  }

  @Test
  public void timestampAttributeSetWithMissingAttributeThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    Map<String, String> map = ImmutableMap.of("otherLabel", "whatever");
    PubsubClient.extractTimestamp("myAttribute", null, map);
  }

  @Test
  public void timestampAttributeParsesMillisecondsSinceEpoch() {
    long time = 1446162101123L;
    Map<String, String> map = ImmutableMap.of("myAttribute", String.valueOf(time));
    long timestamp = PubsubClient.extractTimestamp("myAttribute", null, map);
    assertEquals(time, timestamp);
  }

  @Test
  public void timestampAttributeParsesRfc3339Seconds() {
    roundTripRfc339("2015-10-29T23:41:41Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339Tenths() {
    roundTripRfc339("2015-10-29T23:41:41.1Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339Hundredths() {
    roundTripRfc339("2015-10-29T23:41:41.12Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339Millis() {
    roundTripRfc339("2015-10-29T23:41:41.123Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339Micros() {
    // Note: micros part 456/1000 is dropped.
    truncatedRfc339("2015-10-29T23:41:41.123456Z", "2015-10-29T23:41:41.123Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339MicrosRounding() {
    // Note: micros part 999/1000 is dropped, not rounded up.
    truncatedRfc339("2015-10-29T23:41:41.123999Z", "2015-10-29T23:41:41.123Z");
  }

  @Test
  public void timestampAttributeWithInvalidFormatThrowsError() {
    thrown.expect(NumberFormatException.class);
    parse("not-a-timestamp");
  }

  @Test
  public void timestampAttributeWithInvalidFormat2ThrowsError() {
    thrown.expect(NumberFormatException.class);
    parse("null");
  }

  @Test
  public void timestampAttributeWithInvalidFormat3ThrowsError() {
    thrown.expect(NumberFormatException.class);
    parse("2015-10");
  }

  @Test
  public void timestampAttributeParsesRfc3339WithSmallYear() {
    // Google and JodaTime agree on dates after 1582-10-15, when the Gregorian Calendar was adopted
    // This is therefore a "small year" until this difference is reconciled.
    roundTripRfc339("1582-10-15T01:23:45.123Z");
  }

  @Test
  public void timestampAttributeParsesRfc3339WithLargeYear() {
    // Year 9999 in range.
    roundTripRfc339("9999-10-29T23:41:41.123999Z");
  }

  @Test
  public void timestampAttributeRfc3339WithTooLargeYearThrowsError() {
    thrown.expect(NumberFormatException.class);
    // Year 10000 out of range.
    parse("10000-10-29T23:41:41.123999Z");
  }

  //
  // Paths
  //

  @Test
  public void projectPathFromIdWellFormed() {
    ProjectPath path = PubsubClient.projectPathFromId("test");
    assertEquals("projects/test", path.getPath());
  }

  @Test
  public void subscriptionPathFromNameWellFormed() {
    SubscriptionPath path = PubsubClient.subscriptionPathFromName("test", "something");
    assertEquals("projects/test/subscriptions/something", path.getPath());
    assertEquals("/subscriptions/test/something", path.getV1Beta1Path());
  }

  @Test
  public void topicPathFromNameWellFormed() {
    TopicPath path = PubsubClient.topicPathFromName("test", "something");
    assertEquals("projects/test/topics/something", path.getPath());
    assertEquals("/topics/test/something", path.getV1Beta1Path());
  }
}
