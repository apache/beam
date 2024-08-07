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
import static org.junit.Assert.assertThrows;

import com.google.pubsub.v1.Schema;
import java.util.Map;
import org.apache.avro.SchemaParseException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SchemaPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
    return PubsubClient.extractTimestampAttribute("myAttribute", map);
  }

  private void roundTripRfc339(String timestamp) {
    assertEquals(Instant.parse(timestamp).getMillis(), parse(timestamp));
  }

  private void truncatedRfc339(String timestamp, String truncatedTimestmap) {
    assertEquals(Instant.parse(truncatedTimestmap).getMillis(), parse(timestamp));
  }

  @Test
  public void noTimestampAttributeAndInvalidPubsubPublishThrowsError() {
    thrown.expect(NumberFormatException.class);
    PubsubClient.parseTimestampAsMsSinceEpoch("not-a-date");
  }

  @Test
  public void timestampAttributeWithNullAttributesThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    PubsubClient.extractTimestampAttribute("myAttribute", null);
  }

  @Test
  public void timestampAttributeSetWithMissingAttributeThrowsError() {
    thrown.expect(RuntimeException.class);
    thrown.expectMessage("PubSub message is missing a value for timestamp attribute myAttribute");
    Map<String, String> map = ImmutableMap.of("otherLabel", "whatever");
    PubsubClient.extractTimestampAttribute("myAttribute", map);
  }

  @Test
  public void timestampAttributeParsesMillisecondsSinceEpoch() {
    long time = 1446162101123L;
    Map<String, String> map = ImmutableMap.of("myAttribute", String.valueOf(time));
    long timestamp = PubsubClient.extractTimestampAttribute("myAttribute", map);
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
    assertEquals("/subscriptions/test/something", path.getFullPath());
    assertEquals(ImmutableList.of("test", "something"), path.getDataCatalogSegments());
  }

  @Test
  public void topicPathFromNameWellFormed() {
    TopicPath path = PubsubClient.topicPathFromName("test", "something");
    assertEquals("projects/test/topics/something", path.getPath());
    assertEquals("/topics/test/something", path.getFullPath());
    assertEquals(ImmutableList.of("test", "something"), path.getDataCatalogSegments());
  }

  @Test
  public void schemaPathFromIdPathWellFormed() {
    SchemaPath path = PubsubClient.schemaPathFromId("projectId", "schemaId");
    assertEquals("projects/projectId/schemas/schemaId", path.getPath());
    assertEquals("schemaId", path.getId());
  }

  @Test
  public void schemaPathFromPathWellFormed() {
    SchemaPath path = PubsubClient.schemaPathFromPath("projects/projectId/schemas/schemaId");
    assertEquals("projects/projectId/schemas/schemaId", path.getPath());
    assertEquals("schemaId", path.getId());
  }

  @Test
  public void fromPubsubSchema() {
    assertThrows(
        "null definition should throw an exception",
        NullPointerException.class,
        () ->
            PubsubClient.fromPubsubSchema(
                new com.google.api.services.pubsub.model.Schema().setType("AVRO")));

    assertThrows(
        "null definition should throw an exception",
        SchemaParseException.class,
        () ->
            PubsubClient.fromPubsubSchema(
                com.google.pubsub.v1.Schema.newBuilder().setType(Schema.Type.AVRO).build()));

    String badSchema =
        "{\"type\": \"record\", \"name\": \"Avro\",\"fields\": [{\"name\": \"bad\", \"type\": \"notatype\"}]}";
    String goodSchema =
        "{"
            + " \"type\" : \"record\","
            + " \"name\" : \"Avro\","
            + " \"fields\" : ["
            + "   {"
            + "     \"name\" : \"StringField\","
            + "     \"type\" : \"string\""
            + "   },"
            + "   {"
            + "     \"name\" : \"FloatField\","
            + "     \"type\" : \"float\""
            + "   },"
            + "   {"
            + "     \"name\" : \"IntField\","
            + "     \"type\" : \"int\""
            + "   },"
            + "   {"
            + "     \"name\" : \"LongField\","
            + "     \"type\" : \"long\""
            + "   },"
            + "   {"
            + "     \"name\" : \"DoubleField\","
            + "     \"type\" : \"double\""
            + "   },"
            + "   {"
            + "     \"name\" : \"BytesField\","
            + "     \"type\" : \"bytes\""
            + "   },"
            + "   {"
            + "     \"name\" : \"BooleanField\","
            + "     \"type\" : \"boolean\""
            + "   }"
            + " ]"
            + "}";

    assertThrows(
        "unsupported Schema type should throw an exception",
        IllegalArgumentException.class,
        () ->
            PubsubClient.fromPubsubSchema(
                new com.google.api.services.pubsub.model.Schema()
                    .setType("PROTOCOL_BUFFER")
                    .setDefinition(goodSchema)));

    assertThrows(
        "'notatype' Avro type should throw an exception",
        SchemaParseException.class,
        () ->
            PubsubClient.fromPubsubSchema(
                new com.google.api.services.pubsub.model.Schema()
                    .setType("AVRO")
                    .setDefinition(badSchema)));

    assertEquals(
        org.apache.beam.sdk.schemas.Schema.of(
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "StringField", org.apache.beam.sdk.schemas.Schema.FieldType.STRING),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "FloatField", org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "IntField", org.apache.beam.sdk.schemas.Schema.FieldType.INT32),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "LongField", org.apache.beam.sdk.schemas.Schema.FieldType.INT64),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "DoubleField", org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "BytesField", org.apache.beam.sdk.schemas.Schema.FieldType.BYTES),
            org.apache.beam.sdk.schemas.Schema.Field.of(
                "BooleanField", org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN)),
        PubsubClient.fromPubsubSchema(
            new com.google.api.services.pubsub.model.Schema()
                .setType("AVRO")
                .setDefinition(goodSchema)));
  }
}
