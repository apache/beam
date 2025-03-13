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
package org.apache.beam.sdk.io.aws2.schemas;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.util.CoderUtils.decodeFromByteArray;
import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.SerializableUtils.ensureSerializableRoundTrip;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Condition;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class AwsSchemaProviderTest {
  private final SchemaRegistry registry = SchemaRegistry.createDefault();
  private final Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);

  private interface Schemas {
    Schema MESSAGE_ATTRIBUTES =
        Schema.builder()
            .addNullableField("stringValue", FieldType.STRING)
            .addNullableField("binaryValue", FieldType.BYTES)
            .addNullableField("stringListValues", FieldType.array(FieldType.STRING))
            .addNullableField("binaryListValues", FieldType.array(FieldType.BYTES))
            .addNullableField("dataType", FieldType.STRING)
            .build();

    Schema SEND_MESSAGE_REQUEST =
        Schema.builder()
            .addNullableField("queueUrl", FieldType.STRING)
            .addNullableField("messageBody", FieldType.STRING)
            .addNullableField("delaySeconds", FieldType.INT32)
            .addNullableField(
                "messageAttributes",
                FieldType.map(FieldType.STRING, FieldType.row(MESSAGE_ATTRIBUTES)))
            .addNullableField(
                "messageSystemAttributes",
                FieldType.map(FieldType.STRING, FieldType.row(MESSAGE_ATTRIBUTES)))
            .addNullableField("messageDeduplicationId", FieldType.STRING)
            .addNullableField("messageGroupId", FieldType.STRING)
            .build();

    Schema SAMPLE =
        Schema.builder()
            .addNullableField("string", FieldType.STRING)
            .addNullableField("short", FieldType.INT16)
            .addNullableField("integer", FieldType.INT32)
            .addNullableField("long", FieldType.INT64)
            .addNullableField("float", FieldType.FLOAT)
            .addNullableField("double", FieldType.DOUBLE)
            .addNullableField("decimal", FieldType.DECIMAL)
            .addNullableField("boolean", FieldType.BOOLEAN)
            .addNullableField("instant", FieldType.DATETIME)
            .addNullableField("bytes", FieldType.BYTES)
            .addNullableField("list", FieldType.array(FieldType.STRING))
            .addNullableField("map", FieldType.map(FieldType.STRING, FieldType.STRING))
            .build();
  }

  @Test
  public void testSampleSchema() throws NoSuchSchemaException {
    Schema schema = registry.getSchema(Sample.class);
    SchemaTestUtils.assertSchemaEquivalent(Schemas.SAMPLE, schema);
  }

  @Test
  public void testAwsExampleSchema() throws NoSuchSchemaException {
    Schema schema = registry.getSchema(SendMessageRequest.class);
    SchemaTestUtils.assertSchemaEquivalent(Schemas.SEND_MESSAGE_REQUEST, schema);
  }

  @Test
  public void testRecursiveSchema() {
    assertThatThrownBy(() -> registry.getSchema(AttributeValue.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Self-recursive types are not supported: " + AttributeValue.class);
  }

  @Test
  public void testToRowSerializable() throws NoSuchSchemaException {
    ensureSerializableRoundTrip(registry.getToRowFunction(Sample.class));
    ensureSerializableRoundTrip(registry.getToRowFunction(SendMessageRequest.class));
  }

  @Test
  public void testFromRowSerializable() throws NoSuchSchemaException {
    ensureSerializableRoundTrip(registry.getFromRowFunction(Sample.class));
    ensureSerializableRoundTrip(registry.getFromRowFunction(SendMessageRequest.class));
  }

  @Test
  public void testSampleToRow() throws NoSuchSchemaException {
    Sample sample =
        Sample.builder()
            .stringField("0")
            .shortField((short) 1)
            .integerField(2)
            .longField(3L)
            .floatField((float) 4.0)
            .doubleField(5.0)
            .decimalField(BigDecimal.valueOf(6L))
            .instantField(now)
            .bytesField(SdkBytes.fromUtf8String("7"))
            .listField(ImmutableList.of("8"))
            .mapField(ImmutableMap.of("9", "9"))
            .build();

    Row row = registry.getToRowFunction(Sample.class).apply(sample);

    assertThat(row)
        .has(field("string", "0"))
        .has(field("short", (short) 1))
        .has(field("integer", 2))
        .has(field("long", 3L))
        .has(field("float", (float) 4.0))
        .has(field("double", 5.0))
        .has(field("decimal", BigDecimal.valueOf(6L)))
        .has(field("instant", org.joda.time.Instant.ofEpochMilli(now.toEpochMilli())))
        .has(field("bytes", "7".getBytes(UTF_8)))
        .has(field("list", ImmutableList.of("8")))
        .has(field("map", ImmutableMap.of("9", "9")));
  }

  @Test
  public void testAwsExampleToRow() throws NoSuchSchemaException {
    SendMessageRequest request =
        SendMessageRequest.builder()
            .queueUrl("queue")
            .messageBody("body")
            .delaySeconds(100)
            .messageDeduplicationId("dedupId")
            .messageGroupId("groupId")
            .messageAttributes(
                ImmutableMap.of(
                    "string",
                    attribute(b -> b.stringValue("v").dataType("String")),
                    "binary",
                    attribute(b -> b.binaryValue(sdkBytes("v")).dataType("Binary")),
                    "stringList",
                    attribute(b -> b.stringListValues("v1", "v2")),
                    "binaryList",
                    attribute(b -> b.binaryListValues(sdkBytes("v1"), sdkBytes("v2")))))
            .build();

    Row row = registry.getToRowFunction(SendMessageRequest.class).apply(request);

    assertThat(row)
        .has(field("queueUrl", "queue"))
        .has(field("messageBody", "body"))
        .has(field("delaySeconds", 100))
        .has(field("messageDeduplicationId", "dedupId"))
        .has(field("messageGroupId", "groupId"));

    assertThat((Row) row.getMap("messageAttributes").get("string"))
        .has(field("dataType", "String"))
        .has(field("stringValue", "v"))
        .has(field("binaryValue", null))
        .has(field("stringListValues", null))
        .has(field("binaryListValues", null));

    assertThat((Row) row.getMap("messageAttributes").get("binary"))
        .has(field("dataType", "Binary"))
        .has(field("stringValue", null))
        .has(field("binaryValue", bytes("v")))
        .has(field("stringListValues", null))
        .has(field("binaryListValues", null));

    assertThat((Row) row.getMap("messageAttributes").get("stringList"))
        .has(field("dataType", null))
        .has(field("stringValue", null))
        .has(field("binaryValue", null))
        .has(field("stringListValues", ImmutableList.of("v1", "v2")))
        .has(field("binaryListValues", null));

    assertThat((Row) row.getMap("messageAttributes").get("binaryList"))
        .has(field("dataType", null))
        .has(field("stringValue", null))
        .has(field("binaryValue", null))
        .has(field("stringListValues", null))
        .has(field("binaryListValues", ImmutableList.of(bytes("v1"), bytes("v2"))));
  }

  @Test
  public void testSampleFromRow() throws NoSuchSchemaException, CoderException {
    Sample sample =
        Sample.builder()
            .stringField("0")
            .shortField((short) 1)
            .integerField(2)
            .longField(3L)
            .floatField((float) 4.0)
            .doubleField(5.0)
            .decimalField(BigDecimal.valueOf(6L))
            .instantField(now)
            .bytesField(SdkBytes.fromUtf8String("7"))
            .listField(ImmutableList.of("8"))
            .mapField(ImmutableMap.of("9", "9"))
            .build();

    SchemaCoder<Sample> coder = registry.getSchemaCoder(Sample.class);

    Row row = coder.getToRowFunction().apply(sample);
    assertThat(coder.getFromRowFunction().apply(row)).isEqualTo(sample);

    byte[] sampleBytes = encodeToByteArray(coder, sample);
    Sample sampleFromBytes = decodeFromByteArray(coder, sampleBytes);
    assertThat(sampleFromBytes).isEqualTo(sample);

    // verify still serializable after use
    ensureSerializableRoundTrip(coder.getToRowFunction());
    ensureSerializableRoundTrip(coder.getFromRowFunction());
  }

  @Test
  public void testAwsExampleFromRow() throws NoSuchSchemaException, CoderException {
    SendMessageRequest request =
        SendMessageRequest.builder()
            .queueUrl("queue")
            .messageBody("body")
            .delaySeconds(100)
            .messageDeduplicationId("dedupId")
            .messageGroupId("groupId")
            .messageAttributes(
                ImmutableMap.of(
                    "string",
                    attribute(b -> b.stringValue("v").dataType("String")),
                    "binary",
                    attribute(b -> b.binaryValue(sdkBytes("v")).dataType("Binary")),
                    "stringList",
                    attribute(b -> b.stringListValues("v1", "v2")),
                    "binaryList",
                    attribute(b -> b.binaryListValues(sdkBytes("v1"), sdkBytes("v2")))))
            .build();

    SchemaCoder<SendMessageRequest> coder = registry.getSchemaCoder(SendMessageRequest.class);

    Row row = coder.getToRowFunction().apply(request);
    assertThat(coder.getFromRowFunction().apply(row)).isEqualTo(request);

    byte[] requestBytes = encodeToByteArray(coder, request);
    SendMessageRequest requestFromBytes = decodeFromByteArray(coder, requestBytes);
    assertThat(requestFromBytes).isEqualTo(request);

    // verify still serializable after use
    ensureSerializableRoundTrip(coder.getToRowFunction());
    ensureSerializableRoundTrip(coder.getFromRowFunction());
  }

  @Test
  public void testFromRowWithPartialSchema() throws NoSuchSchemaException {
    SerializableFunction<Row, SendMessageRequest> fromRow =
        registry.getFromRowFunction(SendMessageRequest.class);

    Schema partialSchema =
        Schema.builder()
            .addNullableField("queueUrl", FieldType.STRING)
            .addNullableField("messageBody", FieldType.STRING)
            .addNullableField("delaySeconds", FieldType.INT32)
            .build();

    SendMessageRequest request =
        SendMessageRequest.builder()
            .queueUrl("queue")
            .messageBody("body")
            .delaySeconds(100)
            .build();

    Row row = Row.withSchema(partialSchema).addValues("queue", "body", 100).build();

    assertThat(fromRow.apply(row)).isEqualTo(request);
  }

  @Test
  public void testFailFromRowOnUnknownField() throws NoSuchSchemaException {
    SerializableFunction<Row, SendMessageRequest> fromRow =
        registry.getFromRowFunction(SendMessageRequest.class);

    Schema partialSchema =
        Schema.builder()
            .addNullableField("queueUrl", FieldType.STRING)
            .addNullableField("messageBody", FieldType.STRING)
            .addNullableField("unknownField", FieldType.INT32)
            .build();

    Row row = Row.withSchema(partialSchema).addValues("queue", "body", 100).build();

    assertThatThrownBy(() -> fromRow.apply(row))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Row schema contains unknown fields: [unknownField]");
  }

  private static MessageAttributeValue attribute(
      Function<MessageAttributeValue.Builder, MessageAttributeValue.Builder> b) {
    return b.apply(MessageAttributeValue.builder()).build();
  }

  private static SdkBytes sdkBytes(String str) {
    return SdkBytes.fromByteArrayUnsafe(bytes(str));
  }

  private static byte[] bytes(String str) {
    return str.getBytes(UTF_8);
  }

  private <T> Condition<Row> field(String name, T value) {
    return new Condition<>(
        row -> {
          FieldType type = row.getSchema().getField(name).getType();
          SoftAssertions soft = new SoftAssertions();
          Object actual = row.getValue(name);
          if (type.getTypeName() == ARRAY && value != null && value instanceof List) {
            soft.assertThat((List<Object>) actual).containsExactlyElementsOf((List<Object>) value);
          } else {
            soft.assertThat((T) actual).isEqualTo(value);
          }
          soft.errorsCollected().forEach(System.out::println);
          return soft.errorsCollected().isEmpty();
        },
        "field %s of: %s",
        name,
        value);
  }
}
