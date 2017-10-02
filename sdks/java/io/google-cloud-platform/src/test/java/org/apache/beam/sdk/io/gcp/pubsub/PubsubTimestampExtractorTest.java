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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTimestampExtractor.TimestampExtractorFn;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for timestamp methods in PubsubTimestampExtractor.
 */
@RunWith(JUnit4.class)
public class PubsubTimestampExtractorTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  //
  // Timestamp handling
  //

  private long parse(String timestamp) {
    Map<String, String> map = ImmutableMap.of("myAttribute", timestamp);
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor("myAttribute");
    return timestampExtractor.extractTimestamp(null, null, map);
  }

  private void roundTripRfc339(String timestamp) {
    assertEquals(Instant.parse(timestamp).getMillis(), parse(timestamp));
  }

  private void truncatedRfc339(String timestamp, String truncatedTimestmap) {
    assertEquals(Instant.parse(truncatedTimestmap).getMillis(), parse(timestamp));
  }

  @Test
  public void noTimestampAttributeOrFunctionExtractorReturnsPubsubPublish() {
    final long time = 987654321L;
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor();
    long timestamp = timestampExtractor.extractTimestamp(null, String.valueOf(time), null);
    assertEquals(time, timestamp);
  }

  @Test
  public void noTimestampAttributeOrFunctionExtractorAndInvalidPubsubPublishThrowsError() {
    thrown.expect(NumberFormatException.class);
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor();
    timestampExtractor.extractTimestamp(null, "not-a-date", null);
  }

  @Test
  public void timestampAttributeWithNullAttributesThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor("myAttribute");
    timestampExtractor.extractTimestamp(null, null, null);
  }

  @Test
  public void timestampAttributeSetWithMissingAttributeThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    Map<String, String> map = ImmutableMap.of("otherLabel", "whatever");
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor("myAttribute");
    timestampExtractor.extractTimestamp(null, null, map);
  }

  @Test
  public void timestampAttributeParsesMillisecondsSinceEpoch() {
    long time = 1446162101123L;
    Map<String, String> map = ImmutableMap.of("myAttribute", String.valueOf(time));
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor("myAttribute");
    long timestamp = timestampExtractor.extractTimestamp(null, null, map);
    assertEquals(time, timestamp);
  }

  @Test
  public void timestampFunctionExtractorReturnedNullThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Cannot interpret timestamp from function extractor: null");
    final long time = 987654321L;
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor(
        new TimestampExtractorFn() {
          @Override public String extractTimestamp(String messagePaylod) {
            return null;
          }
        });
    long timestamp = timestampExtractor.extractTimestamp("2015-10-29T23:45:41Z", null, null);
  }

  @Test
  public void timestampFunctionExtractorReturnedInvalidTimeThrowsError() {
    thrown.expect(NumberFormatException.class);
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor(
        new TimestampExtractorFn() {
          @Override public String extractTimestamp(String messagePaylod) {
            return "invalid-time";
          }
        });
    long timestamp = timestampExtractor.extractTimestamp("2015-10-29T23:45:41Z", null, null);
  }

  @Test
  public void timestampFunctionExtractorParsesMillisecondsSinceEpoch() {
    final long time = 1446162341000L;
    PubsubTimestampExtractor timestampExtractor = new PubsubTimestampExtractor(
        new TimestampExtractorFn() {
          @Override public String extractTimestamp(String messagePaylod) {
            return messagePaylod;
          }
        });
    long timestamp = timestampExtractor.extractTimestamp("2015-10-29T23:45:41Z", null, null);
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

}
