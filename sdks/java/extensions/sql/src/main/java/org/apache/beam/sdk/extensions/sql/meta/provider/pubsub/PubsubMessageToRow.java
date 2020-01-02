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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/** Read side converter template for {@link PubsubMessage}. */
@Internal
@Experimental
public abstract class PubsubMessageToRow
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {
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
   * <p>UTF-8 JSON and Avro objects are supported via {@link JsonPubsubMessageToRow} and {@link
   * AvroPubsubMessageToRow} respectively.
   */
  public abstract Schema messageSchema();

  public abstract boolean useDlq();

  public abstract boolean useFlatSchema();

  protected abstract static class FlatSchemaPubsubMessageToRow extends DoFn<PubsubMessage, Row> {

    protected abstract Row parsePayload(PubsubMessage message);

    protected abstract Schema getMessageSchema();

    protected abstract boolean getUseDlq();

    /**
     * Get the value for a field from a given payload in the order they're specified in the flat
     * schema.
     */
    protected Object getValueForFieldFlatSchema(Field field, Instant timestamp, Row payload) {
      String fieldName = field.getName();
      if (TIMESTAMP_FIELD.equals(fieldName)) {
        return timestamp;
      } else {
        return payload.getValue(fieldName);
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        Row payload = parsePayload(context.element());
        List<Object> values =
            getMessageSchema().getFields().stream()
                .map(field -> getValueForFieldFlatSchema(field, context.timestamp(), payload))
                .collect(Collectors.toList());
        context.output(Row.withSchema(getMessageSchema()).addValues(values).build());
      } catch (Exception exception) {
        if (getUseDlq()) {
          context.output(DLQ_TAG, context.element());
        } else {
          throw new RuntimeException("Error parsing message", exception);
        }
      }
    }
  }
}
