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

import static com.google.api.client.util.DateTime.parseRfc3339;
import static org.apache.beam.sdk.schemas.Schema.TypeName.DATETIME;
import static org.apache.beam.sdk.util.JsonToRowUtils.newObjectMapperWith;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.JsonToRowUtils;
import org.apache.beam.sdk.util.RowJsonDeserializer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

/**
 * <i>Experimental</i>
 *
 * <p>Wraps the {@link PubsubIO} with JSON messages into {@link BeamSqlTable}.
 *
 * <p>This enables {@link PubsubIO} registration in Beam SQL environment as a table, including DDL
 * support.
 *
 * <p>For example:
 * <pre>
 *
 *  CREATE TABLE topic (name VARCHAR, age INTEGER)
 *     TYPE 'pubsub'
 *     LOCATION projects/&lt;GCP project id&gt;/topics/&lt;topic name&gt;
 *     TBLPROPERTIES '{ \"timestampAttributeKey\" : &lt;timestamp attribute&gt; }';
 *
 *   SELECT name, age FROM topic;
 *
 * </pre>
 */
@AutoValue
@Internal
@Experimental
abstract class PubsubIOJsonTable implements BeamSqlTable, Serializable {

  /**
   * Schema of the pubsubs message payload.
   *
   * <p>Only UTF-8 flat JSON objects are supported at the moment.
   */
  abstract Schema getPayloadSchema();

  /**
   * Attribute key of the Pubsub message from which to extract the event timestamp.
   *
   * <p>This attribute has to conform to the same requirements as in {@link
   * PubsubIO.Read.Builder#withTimestampAttribute}.
   *
   * <p>Short version: it has to be either millis since epoch or string in RFC 3339 format.
   */
  abstract String getTimestampAttribute();

  /**
   * Pubsub topic name.
   *
   * <p>Topic is the only way to specify the Pubsub source. Explicitly specifying the subscription
   * is not supported at the moment. Subscriptions are automatically created an managed.
   */
  abstract String getTopic();

  static Builder builder() {
    return new AutoValue_PubsubIOJsonTable.Builder();
  }

  /**
   * Table schema.
   *
   * <p>Inherited from {@link BeamSqlTable}. Different from {@link #getPayloadSchema()},
   * includes timestamp attribute.
   */
   public abstract Schema getSchema();

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(Pipeline pipeline) {
    return
        PBegin
            .in(pipeline)
            .apply("readFromPubsub", readMessagesWithAttributes())
            .apply("parseTimestampAttribute", parseTimestampAttribute())
            .apply("parseJsonPayload", parseJsonPayload())
            .setCoder(getSchema().getRowCoder());
  }

  private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
    return
        PubsubIO
            .readMessagesWithAttributes()
            .fromTopic(getTopic())
            .withTimestampAttribute(getTimestampAttribute());
  }

  private ParDo.SingleOutput<PubsubMessage, KV<DateTime, PubsubMessage>> parseTimestampAttribute() {
    return
        ParDo.of(new DoFn<PubsubMessage, KV<DateTime, PubsubMessage>>() {
          @ProcessElement
          public void processElement(ProcessContext context) {
            PubsubMessage pubsubMessage = context.element();
            long msSinceEpoch = asMsSinceEpoch(pubsubMessage.getAttribute(getTimestampAttribute()));
            context.output(KV.of(new DateTime(msSinceEpoch), pubsubMessage));
          }
        });
  }

  private static long asMsSinceEpoch(String timestamp) {
    try {
      return Long.parseLong(timestamp);
    } catch (IllegalArgumentException e1) {
      return parseRfc3339(timestamp).getValue();
    }
  }

  private ParDo.SingleOutput<KV<DateTime, PubsubMessage>, Row> parseJsonPayload() {
    return ParDo.of(
        new DoFn<KV<DateTime, PubsubMessage>, Row>() {
          private transient volatile @Nullable ObjectMapper objectMapper;

          @ProcessElement
          public void processElement(ProcessContext context) {
            DateTime timestamp = context.element().getKey();
            byte[] payloadBytes = context.element().getValue().getPayload();
            String payloadJson = new String(payloadBytes, StandardCharsets.UTF_8);
            Row payloadRow = parseRow(payloadJson);

            context.output(
                Row
                    .withSchema(getSchema())
                    .addValues(payloadRow.getValues())
                    .addValue(timestamp)
                    .build());
          }

          private Row parseRow(String json) {
            if (objectMapper == null) {
              objectMapper = newObjectMapperWith(RowJsonDeserializer.forSchema(getPayloadSchema()));
            }

            return JsonToRowUtils.jsonToRow(objectMapper, json);
          }
        });
  }

  @Override
  public PTransform<? super PCollection<Row>, POutput> buildIOWriter() {
    throw new UnsupportedOperationException("Writing to a Pubsub topic is not supported");
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setPayloadSchema(Schema payloadSchema);
    abstract Builder setSchema(Schema schema);
    abstract Builder setTimestampAttribute(String timestampAttribute);
    abstract Builder setTopic(String topic);

    abstract PubsubIOJsonTable autoBuild();

    abstract Schema getPayloadSchema();
    abstract String getTimestampAttribute();

    public PubsubIOJsonTable build() {
      return
          this
              .setSchema(addTimestampField(getPayloadSchema(), getTimestampAttribute()))
              .autoBuild();
    }

    private Schema addTimestampField(Schema schema, String timestampField) {
      return
          Schema
              .builder()
              .addFields(schema.getFields())
              .addField(Schema.Field.of(timestampField, DATETIME.type()))
              .build();
    }
  }
}
