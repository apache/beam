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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

class RowHandler implements Serializable {
  private static final long serialVersionUID = 6827681678454156L;

  static final String PUBLISH_TIMESTAMP_FIELD = "publish_timestamp";
  static final String MESSAGE_KEY_FIELD = "message_key";
  static final String EVENT_TIMESTAMP_FIELD = "event_timestamp";
  static final String ATTRIBUTES_FIELD = "attributes";
  static final String PAYLOAD_FIELD = "payload";

  static final String ATTRIBUTES_KEY_FIELD = "key";
  static final String ATTRIBUTES_VALUES_FIELD = "values";

  static final Schema ATTRIBUTES_ENTRY_SCHEMA =
      Schema.builder()
          .addStringField(ATTRIBUTES_KEY_FIELD)
          .addArrayField(ATTRIBUTES_VALUES_FIELD, FieldType.BYTES)
          .build();
  static final Schema.FieldType ATTRIBUTES_FIELD_TYPE =
      Schema.FieldType.array(FieldType.row(ATTRIBUTES_ENTRY_SCHEMA));

  private final Schema schema;
  private final @Nullable PayloadSerializer payloadSerializer;

  RowHandler(Schema schema) {
    checkArgument(schema.getField(PAYLOAD_FIELD).getType().equals(FieldType.BYTES));
    this.schema = schema;
    this.payloadSerializer = null;
  }

  RowHandler(Schema schema, @Nonnull PayloadSerializer payloadSerializer) {
    this.schema = schema;
    this.payloadSerializer = payloadSerializer;
    checkArgument(schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
  }

  /* Convert a message to a row. If Schema payload field is a Row type, payloadSerializer is required. */
  Row messageToRow(SequencedMessage message) {
    // Transform this to a FieldValueBuilder, because otherwise individual withFieldValue calls will
    // not mutate the original object.
    Row.FieldValueBuilder builder = Row.withSchema(schema).withFieldValues(ImmutableMap.of());
    if (schema.hasField(PUBLISH_TIMESTAMP_FIELD)) {
      builder.withFieldValue(
          PUBLISH_TIMESTAMP_FIELD,
          Instant.ofEpochMilli(Timestamps.toMillis(message.getPublishTime())));
    }
    if (schema.hasField(MESSAGE_KEY_FIELD)) {
      builder.withFieldValue(MESSAGE_KEY_FIELD, message.getMessage().getKey().toByteArray());
    }
    if (schema.hasField(EVENT_TIMESTAMP_FIELD) && message.getMessage().hasEventTime()) {
      builder.withFieldValue(
          EVENT_TIMESTAMP_FIELD,
          Instant.ofEpochMilli(Timestamps.toMillis(message.getMessage().getEventTime())));
    }
    if (schema.hasField(ATTRIBUTES_FIELD)) {
      ImmutableList.Builder<Row> listBuilder = ImmutableList.builder();
      message
          .getMessage()
          .getAttributesMap()
          .forEach(
              (key, values) -> {
                Row entry =
                    Row.withSchema(ATTRIBUTES_ENTRY_SCHEMA)
                        .withFieldValue(ATTRIBUTES_KEY_FIELD, key)
                        .withFieldValue(
                            ATTRIBUTES_VALUES_FIELD,
                            values.getValuesList().stream()
                                .map(ByteString::toByteArray)
                                .collect(Collectors.toList()))
                        .build();
                listBuilder.add(entry);
              });
      builder.withFieldValue(ATTRIBUTES_FIELD, listBuilder.build());
    }
    if (payloadSerializer == null) {
      builder.withFieldValue(PAYLOAD_FIELD, message.getMessage().getData().toByteArray());
    } else {
      builder.withFieldValue(
          PAYLOAD_FIELD,
          payloadSerializer.deserialize(message.getMessage().getData().toByteArray()));
    }
    return builder.build();
  }

  /* Convert a row to a message. If Schema payload field is a Row type, payloadSerializer is required. */
  PubSubMessage rowToMessage(Row row) {
    row = castRow(row, row.getSchema(), schema);
    PubSubMessage.Builder builder = PubSubMessage.newBuilder();
    if (schema.hasField(MESSAGE_KEY_FIELD)) {
      byte[] bytes = row.getBytes(MESSAGE_KEY_FIELD);
      if (bytes != null) {
        builder.setKey(ByteString.copyFrom(bytes));
      }
    }
    if (schema.hasField(EVENT_TIMESTAMP_FIELD)) {
      ReadableDateTime time = row.getDateTime(EVENT_TIMESTAMP_FIELD);
      if (time != null) {
        builder.setEventTime(Timestamps.fromMillis(time.getMillis()));
      }
    }
    if (schema.hasField(ATTRIBUTES_FIELD)) {
      Collection<Row> attributes = row.getArray(ATTRIBUTES_FIELD);
      if (attributes != null) {
        attributes.forEach(
            entry -> {
              AttributeValues.Builder valuesBuilder = AttributeValues.newBuilder();
              Collection<byte[]> values =
                  checkArgumentNotNull(entry.getArray(ATTRIBUTES_VALUES_FIELD));
              values.forEach(bytes -> valuesBuilder.addValues(ByteString.copyFrom(bytes)));
              builder.putAttributes(
                  checkArgumentNotNull(entry.getString(ATTRIBUTES_KEY_FIELD)),
                  valuesBuilder.build());
            });
      }
    }
    if (payloadSerializer == null) {
      byte[] payload = row.getBytes(PAYLOAD_FIELD);
      if (payload != null) {
        builder.setData(ByteString.copyFrom(payload));
      }
    } else {
      Row payload = row.getRow(PAYLOAD_FIELD);
      if (payload != null) {
        builder.setData(ByteString.copyFrom(payloadSerializer.serialize(payload)));
      }
    }
    return builder.build();
  }
}
