package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.sdk.extensions.sql.meta.provider.kafka.Schemas.*;
import static org.junit.Assert.assertThrows;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
public class NestedPayloadKafkaTableTest {
  private static final Schema FULL_WRITE_SCHEMA =
      Schema.builder()
          .addByteArrayField(MESSAGE_KEY_FIELD)
          .addField(EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
          .addArrayField(HEADERS_FIELD, FieldType.row(HEADERS_ENTRY_SCHEMA))
          .addByteArrayField(PAYLOAD_FIELD)
          .build();
  private static final Schema FULL_READ_SCHEMA =
      Schema.builder()
          .addByteArrayField(MESSAGE_KEY_FIELD)
          .addDateTimeField(EVENT_TIMESTAMP_FIELD)
          .addArrayField(HEADERS_FIELD, FieldType.row(HEADERS_ENTRY_SCHEMA))
          .addByteArrayField(PAYLOAD_FIELD)
          .build();

  @Mock
  public PayloadSerializer serializer;

  @Before
  public void setUp() {
    openMocks(this);
  }
  
  private NestedPayloadKafkaTable newTable(Schema schema, Optional<PayloadSerializer> serializer) {
    return new NestedPayloadKafkaTable(schema, "abc.bootstrap", ImmutableList.of("mytopic"), serializer);
  }
  
  @Test
  public void constructionFailures() {
    // Row payload without serializer
    assertThrows(
        IllegalArgumentException.class,
        () -> newTable(
                Schema.builder()
                    .addRowField(PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                    .build(), Optional.empty()));
    // Bytes payload with serializer
    assertThrows(
        IllegalArgumentException.class,
        () -> newTable(
                Schema.builder().addByteArrayField(PAYLOAD_FIELD).build(), Optional.of(serializer)));
  }
}
