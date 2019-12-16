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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.util.RowJsonUtils.newObjectMapperWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.UnsupportedRowJsonException;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** A {@link DoFn} to convert {@link PubsubMessage} with JSON payload to {@link Row}. */
@Internal
@Experimental
@AutoValue
public abstract class PubsubMessageToRow extends DoFn<PubsubMessage, Row> {
  static final String TIMESTAMP_FIELD = "event_timestamp";
  static final String ATTRIBUTES_FIELD = "attributes";
  static final String PAYLOAD_FIELD = "payload";
  static final TupleTag<PubsubMessage> DLQ_TAG = new TupleTag<PubsubMessage>() {};
  static final TupleTag<Row> MAIN_TAG = new TupleTag<Row>() {};

  private transient volatile @Nullable ObjectMapper objectMapper;

  /**
   * Schema of the Pubsub message.
   *
   * <p>Required to have at least 'event_timestamp' field of type {@link Schema.FieldType#DATETIME}.
   *
   * <p>If {@code useFlatSchema()} is set every other field is assumed to be part of the payload.
   * Otherwise, the schema must contain exactly:
   *
   * <ul>
   *   <li>'attributes' of type {@link TypeName#MAP MAP&lt;VARCHAR,VARCHAR&gt;}
   *   <li>'payload' of type {@link TypeName#ROW ROW&lt;...&gt;}
   * </ul>
   *
   * <p>Only UTF-8 JSON objects are supported.
   */
  public abstract Schema messageSchema();

  public abstract boolean useDlq();

  public abstract boolean useFlatSchema();

  private Schema payloadSchema() {
    if (!useFlatSchema()) {
      return messageSchema().getField(PAYLOAD_FIELD).getType().getRowSchema();
    } else {
      // The payload contains every field in the schema except event_timestamp
      return new Schema(
          messageSchema().getFields().stream()
              .filter(f -> !f.getName().equals(TIMESTAMP_FIELD))
              .collect(Collectors.toList()));
    }
  }

  public static Builder builder() {
    return new AutoValue_PubsubMessageToRow.Builder();
  }

  @DoFn.ProcessElement
  public void processElement(ProcessContext context) {
    try {
      List<Object> values = getFieldValues(context);
      context.output(Row.withSchema(messageSchema()).addValues(values).build());
    } catch (UnsupportedRowJsonException jsonException) {
      if (useDlq()) {
        context.output(DLQ_TAG, context.element());
      } else {
        throw new RuntimeException("Error parsing message", jsonException);
      }
    }
  }

  /**
   * Get values for fields in the same order they're specified in schema, including timestamp,
   * payload, and attributes.
   */
  private List<Object> getFieldValues(ProcessContext context) {
    Row payload = parsePayloadJsonRow(context.element());
    return messageSchema().getFields().stream()
        .map(
            field ->
                getValueForField(
                    field, context.timestamp(), context.element().getAttributeMap(), payload))
        .collect(toList());
  }

  private Object getValueForField(
      Schema.Field field, Instant timestamp, Map<String, String> attributeMap, Row payload) {
    // TODO(BEAM-8801): do this check once at construction time, rather than for every element.
    if (useFlatSchema()) {
      if (field.getName().equals(TIMESTAMP_FIELD)) {
        return timestamp;
      } else {
        return payload.getValue(field.getName());
      }
    } else {
      switch (field.getName()) {
        case TIMESTAMP_FIELD:
          return timestamp;
        case ATTRIBUTES_FIELD:
          return attributeMap;
        case PAYLOAD_FIELD:
          return payload;
        default:
          throw new IllegalArgumentException(
              "Unexpected field '"
                  + field.getName()
                  + "' in top level schema"
                  + " for Pubsub message. Top level schema should only contain "
                  + "'timestamp', 'attributes', and 'payload' fields");
      }
    }
  }

  private Row parsePayloadJsonRow(PubsubMessage pubsubMessage) {
    String payloadJson = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);

    if (objectMapper == null) {
      objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(payloadSchema()));
    }

    return RowJsonUtils.jsonToRow(objectMapper, payloadJson);
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder messageSchema(Schema messageSchema);

    public abstract Builder useDlq(boolean useDlq);

    public abstract Builder useFlatSchema(boolean useFlatSchema);

    public abstract PubsubMessageToRow build();
  }
}
