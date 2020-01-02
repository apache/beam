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
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

/** Read side converter for {@link PubsubMessage} with JSON payload. */
@Internal
@Experimental
@AutoValue
public abstract class JsonPubsubMessageToRow extends PubsubMessageToRow implements Serializable {

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple rows =
        input.apply(
            ParDo.of(
                    useFlatSchema()
                        ? new FlatSchemaJsonPubsubMessageToRoW(messageSchema(), useDlq())
                        : new NestedSchemaPubsubMessageToRow(messageSchema(), useDlq()))
                .withOutputTags(
                    MAIN_TAG, useDlq() ? TupleTagList.of(DLQ_TAG) : TupleTagList.empty()));
    rows.get(MAIN_TAG).setRowSchema(messageSchema());
    return rows;
  }

  /**
   * A {@link DoFn} to convert a flat schema{@link PubsubMessage} with JSON payload to {@link Row}.
   */
  @Internal
  private static class FlatSchemaJsonPubsubMessageToRoW extends FlatSchemaPubsubMessageToRow {

    private final Schema messageSchema;

    private final boolean useDlq;

    private transient volatile @Nullable ObjectMapper objectMapper;

    protected FlatSchemaJsonPubsubMessageToRoW(Schema messageSchema, boolean useDlq) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
    }

    @Override
    public Schema getMessageSchema() {
      return messageSchema;
    }

    @Override
    public boolean getUseDlq() {
      return useDlq;
    }

    @Override
    protected final Row parsePayload(PubsubMessage pubsubMessage) {
      String payloadJson = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
      // Construct flat payload schema.
      Schema payloadSchema =
          new Schema(
              messageSchema.getFields().stream()
                  .filter(f -> !f.getName().equals(TIMESTAMP_FIELD))
                  .collect(Collectors.toList()));

      if (objectMapper == null) {
        objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(payloadSchema));
      }

      return RowJsonUtils.jsonToRow(objectMapper, payloadJson);
    }
  }

  /**
   * A {@link DoFn} to convert a nested schema {@link PubsubMessage} with JSON payload to {@link
   * Row}.
   */
  @Internal
  private static class NestedSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    private final Schema messageSchema;

    private final boolean useDlq;

    private transient volatile @Nullable ObjectMapper objectMapper;

    protected NestedSchemaPubsubMessageToRow(Schema messageSchema, boolean useDlq) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
    }

    /** Get the value for a field int the order they're specified in the nested schema. */
    private Object getValueForFieldNestedSchema(
        Schema.Field field, Instant timestamp, Map<String, String> attributeMap, Row payload) {
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

    private Row parsePayload(PubsubMessage pubsubMessage) {
      String payloadJson = new String(pubsubMessage.getPayload(), StandardCharsets.UTF_8);
      // Retrieve nested payload schema.
      Schema payloadSchema = messageSchema.getField(PAYLOAD_FIELD).getType().getRowSchema();

      if (objectMapper == null) {
        objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(payloadSchema));
      }

      return RowJsonUtils.jsonToRow(objectMapper, payloadJson);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        Row payload = parsePayload(context.element());
        List<Object> values =
            messageSchema.getFields().stream()
                .map(
                    field ->
                        getValueForFieldNestedSchema(
                            field,
                            context.timestamp(),
                            context.element().getAttributeMap(),
                            payload))
                .collect(toList());
        context.output(Row.withSchema(messageSchema).addValues(values).build());
      } catch (UnsupportedRowJsonException jsonException) {
        if (useDlq) {
          context.output(DLQ_TAG, context.element());
        } else {
          throw new RuntimeException("Error parsing message", jsonException);
        }
      }
    }
  }

  public static Builder builder() {
    return new AutoValue_JsonPubsubMessageToRow.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder messageSchema(Schema messageSchema);

    public abstract Builder useDlq(boolean useDlq);

    public abstract Builder useFlatSchema(boolean useFlatSchema);

    public abstract JsonPubsubMessageToRow build();
  }
}
