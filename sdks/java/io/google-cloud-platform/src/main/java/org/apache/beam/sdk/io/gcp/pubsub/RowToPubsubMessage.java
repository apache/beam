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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.PayloadFormat;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link PTransform} to convert {@link Row} to {@link PubsubMessage} with JSON/AVRO payload.
 *
 * <p>Currently only supports writing a flat schema into a JSON/AVRO payload. This means that all
 * Row field values are written to the {@link PubsubMessage} payload, except for {@code
 * event_timestamp}, which is either ignored or written to the message attributes, depending on
 * whether config.getValue("timestampAttributeKey") is set.
 */
@SuppressWarnings("nullness") // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
class RowToPubsubMessage extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {
  private final boolean useTimestampAttribute;
  private final PayloadFormat payloadFormat;
  @Nullable private final Schema payloadSchema;

  private RowToPubsubMessage(
      boolean useTimestampAttribute, PayloadFormat payloadFormat, @Nullable Schema schema) {
    this.useTimestampAttribute = useTimestampAttribute;
    this.payloadFormat = payloadFormat;
    this.payloadSchema = schema == null ? null : stripFromTimestampField(schema);
  }

  public static RowToPubsubMessage of(boolean useTimestampAttribute, PayloadFormat payloadFormat) {
    return new RowToPubsubMessage(useTimestampAttribute, payloadFormat, null);
  }

  public static RowToPubsubMessage of(
      boolean useTimestampAttribute, PayloadFormat payloadFormat, Schema schema) {
    return new RowToPubsubMessage(useTimestampAttribute, payloadFormat, schema);
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
            .apply("MapRowToJsonString", ToJson.of())
            .apply("MapToJsonBytes", MapElements.via(new StringToBytes()))
            .apply("MapToPubsubMessage", MapElements.via(new ToPubsubMessage()));
      case AVRO:
        return withTimestamp
            .apply(
                "MapRowToAvroBytes",
                MapElements.via(AvroUtils.getRowToAvroBytesFunction(payloadSchema)))
            .apply("MapToPubsubMessage", MapElements.via(new ToPubsubMessage()));
      default:
        throw new IllegalArgumentException("Unsupported payload format: " + payloadFormat);
    }
  }

  private static class StringToBytes extends SimpleFunction<String, byte[]> {
    @Override
    public byte[] apply(String s) {
      return s.getBytes(ISO_8859_1);
    }
  }

  private static class ToPubsubMessage extends SimpleFunction<byte[], PubsubMessage> {
    @Override
    public PubsubMessage apply(byte[] bytes) {
      return new PubsubMessage(bytes, ImmutableMap.of());
    }
  }

  private Schema stripFromTimestampField(Schema schema) {
    List<Schema.Field> selectedFields =
        schema.getFields().stream()
            .filter(field -> !TIMESTAMP_FIELD.equals(field.getName()))
            .collect(toList());
    return Schema.of(selectedFields.toArray(new Schema.Field[0]));
  }
}
