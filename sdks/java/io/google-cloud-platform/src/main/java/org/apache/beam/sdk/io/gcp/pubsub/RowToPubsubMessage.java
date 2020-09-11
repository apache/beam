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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A {@link PTransform} to convert {@link Row} to {@link PubsubMessage} with JSON/AVRO payload.
 *
 * <p>Currently only supports writing a flat schema into a JSON/AVRO payload. This means that all
 * Row field values are written to the {@link PubsubMessage} payload, except for {@code
 * event_timestamp}, which is either ignored or written to the message attributes, depending on
 * whether config.getValue("timestampAttributeKey") is set.
 */
class RowToPubsubMessage extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {
  private final boolean useTimestampAttribute;
  private final PayloadFormat payloadFormat;

  private RowToPubsubMessage(boolean useTimestampAttribute, PayloadFormat payloadFormat) {
    this.useTimestampAttribute = useTimestampAttribute;
    this.payloadFormat = payloadFormat;
  }

  public static RowToPubsubMessage of(boolean useTimestampAttribute, PayloadFormat payloadFormat) {
    return new RowToPubsubMessage(useTimestampAttribute, payloadFormat);
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<Row> input) {
    PCollection<Row> withTimestamp =
        useTimestampAttribute
            ? input.apply(WithTimestamps.of((row) -> row.getDateTime(TIMESTAMP_FIELD).toInstant()))
            : input;

    withTimestamp = withTimestamp.apply(DropFields.fields(TIMESTAMP_FIELD));
    switch (payloadFormat) {
      case JSON:
        return withTimestamp
            .apply(ToJson.of())
            .apply(
                MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                    .via(
                        json ->
                            new PubsubMessage(
                                json.getBytes(StandardCharsets.ISO_8859_1), ImmutableMap.of())));
      case AVRO:
        return withTimestamp.apply(
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(row -> new PubsubMessage(AvroUtils.rowToAvroBytes(row), ImmutableMap.of())));
      case PROTO:
        return withTimestamp.apply(
            MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                .via(RowToPubsubMessage::encodeToProtoPubsubMessage));
      default:
        throw new IllegalArgumentException("Unsupported payload format: " + payloadFormat);
    }
  }

  private static PubsubMessage encodeToProtoPubsubMessage(Row row) {
    RowCoder rowCoder = RowCoder.of(row.getSchema());
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      rowCoder.encode(row, outputStream);
      return new PubsubMessage(outputStream.toByteArray(), ImmutableMap.of());
    } catch (IOException e) {
      throw new RuntimeException(String.format("Could not encode row %s to proto.", row), e);
    }
  }
}
