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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_ARRAY_ENTRY_SCHEMA;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;

/** Read side converter for {@link PubsubMessage} with JSON/AVRO payload. */
@Internal
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class PubsubMessageToRow extends PTransform<PCollection<PubsubMessage>, PCollectionTuple>
    implements Serializable {
  interface SerializerProvider extends SerializableFunction<Schema, PayloadSerializer> {}

  static final String TIMESTAMP_FIELD = "event_timestamp";
  static final String ATTRIBUTES_FIELD = "attributes";
  static final String PAYLOAD_FIELD = "payload";
  static final TupleTag<PubsubMessage> DLQ_TAG = new TupleTag<PubsubMessage>() {};
  static final TupleTag<Row> MAIN_TAG = new TupleTag<Row>() {};

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
   * <p>Only UTF-8 JSON and AVRO objects are supported.
   */
  public abstract Schema messageSchema();

  public abstract boolean useDlq();

  public abstract boolean useFlatSchema();

  // A provider for a PayloadSerializer given the expected payload schema.
  public abstract @Nullable SerializerProvider serializerProvider();

  public static Builder builder() {
    return new AutoValue_PubsubMessageToRow.Builder();
  }

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple rows =
        input.apply(
            ParDo.of(
                    useFlatSchema()
                        ? new FlatSchemaPubsubMessageToRow(
                            messageSchema(), useDlq(), serializerProvider())
                        : new NestedSchemaPubsubMessageToRow(
                            messageSchema(), useDlq(), serializerProvider()))
                .withOutputTags(
                    MAIN_TAG, useDlq() ? TupleTagList.of(DLQ_TAG) : TupleTagList.empty()));
    rows.get(MAIN_TAG).setRowSchema(messageSchema());
    return rows;
  }

  /**
   * A {@link DoFn} to convert a flat schema{@link PubsubMessage} with JSON/AVRO payload to {@link
   * Row}.
   */
  @Internal
  private static class FlatSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    private final Schema messageSchema;

    private final boolean useDlq;

    private final PayloadSerializer payloadSerializer;

    protected FlatSchemaPubsubMessageToRow(
        Schema messageSchema, boolean useDlq, SerializerProvider serializerProvider) {
      this.messageSchema = messageSchema;
      // Construct flat payload schema.
      Schema payloadSchema =
          new Schema(
              messageSchema.getFields().stream()
                  .filter(f -> !f.getName().equals(TIMESTAMP_FIELD))
                  .collect(Collectors.toList()));
      this.useDlq = useDlq;
      this.payloadSerializer = serializerProvider.apply(payloadSchema);
    }

    /**
     * Get the value for a field from a given payload in the order they're specified in the flat
     * schema.
     */
    private Object getValueForFieldFlatSchema(Schema.Field field, Instant timestamp, Row payload) {
      String fieldName = field.getName();
      if (TIMESTAMP_FIELD.equals(fieldName)) {
        return timestamp;
      } else {
        return payload.getValue(fieldName);
      }
    }

    @ProcessElement
    public void processElement(
        @Element PubsubMessage element, @Timestamp Instant timestamp, MultiOutputReceiver o) {
      try {
        Row payload = payloadSerializer.deserialize(element.getPayload());
        List<Object> values =
            messageSchema.getFields().stream()
                .map(field -> getValueForFieldFlatSchema(field, timestamp, payload))
                .collect(toList());
        o.get(MAIN_TAG).output(Row.withSchema(messageSchema).addValues(values).build());
      } catch (Exception e) {
        if (useDlq) {
          o.get(DLQ_TAG).output(element);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A {@link DoFn} to convert a nested schema {@link PubsubMessage} with JSON/AVRO payload to
   * {@link Row}.
   */
  @Internal
  private static class NestedSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    private final Schema messageSchema;

    private final boolean useDlq;

    private final @Nullable PayloadSerializer payloadSerializer;

    private NestedSchemaPubsubMessageToRow(
        Schema messageSchema, boolean useDlq, @Nullable SerializerProvider serializerProvider) {
      this.messageSchema = messageSchema;
      this.useDlq = useDlq;
      if (serializerProvider == null) {
        checkArgument(
            messageSchema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.BYTES));
        this.payloadSerializer = null;
      } else {
        checkArgument(
            messageSchema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
        Schema payloadSchema = messageSchema.getField(PAYLOAD_FIELD).getType().getRowSchema();
        this.payloadSerializer = serializerProvider.apply(payloadSchema);
      }
    }

    private Object maybeDeserialize(byte[] payload) {
      if (payloadSerializer == null) {
        return payload;
      }
      return payloadSerializer.deserialize(payload);
    }

    private Object handleAttributes(Map<String, String> attributeMap) {
      if (messageSchema.getField(ATTRIBUTES_FIELD).getType().getTypeName().isMapType()) {
        return attributeMap;
      }
      ImmutableList.Builder<Row> rows = ImmutableList.builder();
      attributeMap.forEach(
          (k, v) -> rows.add(Row.withSchema(ATTRIBUTE_ARRAY_ENTRY_SCHEMA).attachValues(k, v)));
      return rows.build();
    }

    /** Get the value for a field int the order they're specified in the nested schema. */
    private Object getValueForFieldNestedSchema(
        Schema.Field field, Instant timestamp, Map<String, String> attributeMap, byte[] payload) {
      switch (field.getName()) {
        case TIMESTAMP_FIELD:
          return timestamp;
        case ATTRIBUTES_FIELD:
          return handleAttributes(attributeMap);
        case PAYLOAD_FIELD:
          return maybeDeserialize(payload);
        default:
          throw new IllegalArgumentException(
              "Unexpected field '"
                  + field.getName()
                  + "' in top level schema"
                  + " for Pubsub message. Top level schema should only contain "
                  + "'timestamp', 'attributes', and 'payload' fields");
      }
    }

    @ProcessElement
    public void processElement(
        @Element PubsubMessage element, @Timestamp Instant timestamp, MultiOutputReceiver o) {
      try {
        List<Object> values =
            messageSchema.getFields().stream()
                .map(
                    field ->
                        getValueForFieldNestedSchema(
                            field, timestamp, element.getAttributeMap(), element.getPayload()))
                .collect(toList());
        o.get(MAIN_TAG).output(Row.withSchema(messageSchema).addValues(values).build());
      } catch (Exception e) {
        if (useDlq) {
          o.get(DLQ_TAG).output(element);
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder messageSchema(Schema messageSchema);

    public abstract Builder useDlq(boolean useDlq);

    public abstract Builder useFlatSchema(boolean useFlatSchema);

    public abstract Builder serializerProvider(SerializerProvider serializerProvider);

    public abstract PubsubMessageToRow build();
  }

  public static class ParseException extends RuntimeException {
    ParseException(Throwable cause) {
      super("Error parsing message", cause);
    }
  }
}
