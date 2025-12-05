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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Provides a {@link SchemaCoder} for {@link PubsubMessage}, including the topic and all fields of a
 * PubSub message from server.
 *
 * <p>{@link SchemaCoder} is used so that fields can be added in the future without breaking update
 * compatibility. Maintainers should prefer this coder when adding new features to {@link PubsubIO}.
 */
public class PubsubMessageSchemaCoder {
  // NOTE: Fields must not be reordered.
  private static final Schema PUBSUB_MESSAGE_SCHEMA =
      Schema.builder()
          .addByteArrayField("payload")
          .addNullableStringField("topic")
          .addNullableMapField("attributes", Schema.FieldType.STRING, Schema.FieldType.STRING)
          .addNullableStringField("message_id")
          .addNullableStringField("ordering_key")
          .build();

  private static final SerializableFunction<PubsubMessage, Row> TO_ROW =
      (PubsubMessage message) -> {
        // NOTE: The row's value attachment order must match the schema's definition order.
        return Row.withSchema(PUBSUB_MESSAGE_SCHEMA)
            .attachValues(
                message.getPayload(),
                message.getTopic(),
                message.getAttributeMap(),
                message.getMessageId(),
                message.getOrderingKey());
      };

  private static final SerializableFunction<Row, PubsubMessage> FROM_ROW =
      (Row row) -> {
        PubsubMessage message =
            new PubsubMessage(
                Preconditions.checkNotNull(row.getBytes("payload")),
                row.getMap("attributes"),
                row.getString("message_id"),
                row.getString("ordering_key"));

        @Nullable String topic = row.getString("topic");
        if (topic != null) {
          message = message.withTopic(topic);
        }
        return message;
      };

  public static SchemaCoder<PubsubMessage> getSchemaCoder() {
    return SchemaCoder.of(
        PUBSUB_MESSAGE_SCHEMA, TypeDescriptor.of(PubsubMessage.class), TO_ROW, FROM_ROW);
  }
}
