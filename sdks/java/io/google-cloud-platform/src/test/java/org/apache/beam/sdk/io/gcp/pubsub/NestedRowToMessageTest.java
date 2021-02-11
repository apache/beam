package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_ARRAY_ENTRY_SCHEMA;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_ARRAY_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_MAP_FIELD_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class NestedRowToMessageTest {
  private final PayloadSerializer SERIALIZER = mock(PayloadSerializer.class);
  private final NestedRowToMessage TRANSFORM = new NestedRowToMessage(SERIALIZER);
  private final Map<String, String> ATTRIBUTES = ImmutableMap.of("k1", "v1", "k2", "v2");

  @Test
  public void mapAttributesTransformed() {
    Row row = Row.withSchema(Schema.builder().addByteArrayField(PAYLOAD_FIELD).addField(ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE).build())
        .attachValues("abc".getBytes(UTF_8), ATTRIBUTES);
    PubsubMessage message = new PubsubMessage("abc".getBytes(UTF_8), ATTRIBUTES);
    assertEquals(message, TRANSFORM.apply(row));
  }

  @Test
  public void entriesAttributesTransformed() {
    Row row = Row.withSchema(Schema.builder().addByteArrayField(PAYLOAD_FIELD).addField(ATTRIBUTES_FIELD, ATTRIBUTE_ARRAY_FIELD_TYPE).build())
        .attachValues("abc".getBytes(UTF_8), ImmutableList.of(Row.withSchema(ATTRIBUTE_ARRAY_ENTRY_SCHEMA).attachValues("k1", "v1"), Row.withSchema(ATTRIBUTE_ARRAY_ENTRY_SCHEMA).attachValues("k2", "v2")));
    PubsubMessage message = new PubsubMessage("abc".getBytes(UTF_8), ATTRIBUTES);
    assertEquals(message, TRANSFORM.apply(row));
  }

  @Test
  public void rowPayloadTransformed() {
    Schema payloadSchema = Schema.builder().addStringField("fieldName").build();
    Row payload = Row.withSchema(payloadSchema).attachValues("abc");
    Row row = Row.withSchema(Schema.builder().addRowField(PAYLOAD_FIELD, payloadSchema).addField(ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE).build())
        .attachValues(payload, ATTRIBUTES);
    when(SERIALIZER.serialize(payload)).thenReturn("abc".getBytes(UTF_8));
    PubsubMessage message = new PubsubMessage("abc".getBytes(UTF_8), ATTRIBUTES);
    assertEquals(message, TRANSFORM.apply(row));
  }

  @Test
  public void rowPayloadTransformFailure() {
    Schema payloadSchema = Schema.builder().addStringField("fieldName").build();
    Row payload = Row.withSchema(payloadSchema).attachValues("abc");
    Row row = Row.withSchema(Schema.builder().addRowField(PAYLOAD_FIELD, payloadSchema).addField(ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE).build())
        .attachValues(payload, ATTRIBUTES);
    when(SERIALIZER.serialize(payload)).thenThrow(new IllegalArgumentException());
    assertThrows(IllegalArgumentException.class, () -> TRANSFORM.apply(row));
  }
}
