package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link NullableKeyKafkaRecordCoder}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
    "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
public class NullableKeyKafkaRecordCoderTest {

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        NullableKeyKafkaRecordCoder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testKafkaNullableRecordSerializableWithHeaders() throws IOException {
    RecordHeaders headers = new RecordHeaders();
    headers.add("headerKey", "headerVal".getBytes(StandardCharsets.UTF_8));
    verifySerialization(headers);
  }

  @Test
  public void testKafkaNullableRecordSerializableWithoutHeaders() throws IOException {
    ConsumerRecord consumerRecord = new ConsumerRecord<>("", 0, 0L, null, "");
    verifySerialization(consumerRecord.headers());
  }

  private void verifySerialization(Headers headers) throws IOException {
    KafkaRecord<String, String> kafkaRecord =
        new KafkaRecord<>(
            "topic", 0, 0, 0, KafkaTimestampType.CREATE_TIME, headers, null, "value");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    NullableKeyKafkaRecordCoder nullableKeyKafkaRecordCoder =
        NullableKeyKafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    nullableKeyKafkaRecordCoder.encode(kafkaRecord, outputStream);
    KafkaRecord<String, String> decodedRecord =
        nullableKeyKafkaRecordCoder.decode(new ByteArrayInputStream(outputStream.toByteArray()));

    assertEquals(kafkaRecord, decodedRecord);
  }
}
