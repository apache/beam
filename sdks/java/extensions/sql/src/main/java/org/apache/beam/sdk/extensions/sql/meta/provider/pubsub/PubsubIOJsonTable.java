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

import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.MAIN_TAG;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * <i>Experimental</i>
 *
 * <p>Wraps the {@link PubsubIO} with JSON messages into {@link BeamSqlTable}.
 *
 * <p>This enables {@link PubsubIO} registration in Beam SQL environment as a table, including DDL
 * support.
 *
 * <p>Pubsub messages include metadata along with the payload, and it has to be explicitly specified
 * in the schema to make sure it is available to the queries.
 *
 * <p>The fields included in the Pubsub message model are: 'event_timestamp', 'attributes', and
 * 'payload'.
 *
 * <p>For example:
 *
 * <p>If the messages have JSON messages in the payload that look like this:
 *
 * <pre>
 *  {
 *    "id" : 5,
 *    "name" : "foo"
 *  }
 * </pre>
 *
 * <p>Then SQL statements to declare and query such topic will look like this:
 *
 * <pre>
 *  CREATE TABLE topic_table (
 *        event_timestamp TIMESTAMP,
 *        attributes MAP&lt;VARCHAR, VARCHAR&gt;,
 *        payload ROW&lt;name VARCHAR, age INTEGER&gt;
 *      )
 *     TYPE 'pubsub'
 *     LOCATION projects/&lt;GCP project id&gt;/topics/&lt;topic name&gt;
 *     TBLPROPERTIES '{ \"timestampAttributeKey\" : &lt;timestamp attribute&gt; }';
 *
 *  SELECT event_timestamp, topic_table.payload.name FROM topic_table;
 * </pre>
 *
 * <p>Note, 'payload' field is defined as ROW with schema matching the JSON payload of the message.
 * If 'timestampAttributeKey' is specified in TBLPROPERTIES then 'event_timestamp' will be set to
 * the value of that attribute. If it is not specified, then message publish time will be used as
 * event timestamp. 'attributes' map contains Pubsub message attributes map unchanged and can be
 * referenced in the queries as well.
 */
@AutoValue
@Internal
@Experimental
abstract class PubsubIOJsonTable implements BeamSqlTable, Serializable {

  /**
   * Optional attribute key of the Pubsub message from which to extract the event timestamp.
   *
   * <p>This attribute has to conform to the same requirements as in {@link
   * PubsubIO.Read.Builder#withTimestampAttribute}.
   *
   * <p>Short version: it has to be either millis since epoch or string in RFC 3339 format.
   *
   * <p>If the attribute is specified then event timestamps will be extracted from the specified
   * attribute. If it is not specified then message publish timestamp will be used.
   */
  @Nullable
  abstract String getTimestampAttribute();

  /**
   * Optional topic path which will be used as a dead letter queue.
   *
   * <p>Messages that cannot be processed will be sent to this topic. If it is not specified then
   * exception will be thrown for errors during processing causing the pipeline to crash.
   */
  @Nullable
  abstract String getDeadLetterQueue();

  private boolean useDlq() {
    return getDeadLetterQueue() != null;
  }

  /**
   * Pubsub topic name.
   *
   * <p>Topic is the only way to specify the Pubsub source. Explicitly specifying the subscription
   * is not supported at the moment. Subscriptions are automatically created (but not deleted).
   */
  abstract String getTopic();

  static Builder builder() {
    return new AutoValue_PubsubIOJsonTable.Builder();
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  /**
   * Table schema, describes Pubsub message schema.
   *
   * <p>Includes fields 'event_timestamp', 'attributes, and 'payload'. See {@link
   * PubsubMessageToRow}.
   */
  @Override
  public abstract Schema getSchema();

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    PCollectionTuple rowsWithDlq =
        begin
            .apply("readFromPubsub", readMessagesWithAttributes())
            .apply("parseMessageToRow", createParserParDo());
    rowsWithDlq.get(MAIN_TAG).setRowSchema(getSchema());

    if (useDlq()) {
      rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
    }

    return rowsWithDlq.get(MAIN_TAG);
  }

  private ParDo.MultiOutput<PubsubMessage, Row> createParserParDo() {
    return ParDo.of(
            PubsubMessageToRow.builder()
                .messageSchema(getSchema())
                .useDlq(getDeadLetterQueue() != null)
                .build())
        .withOutputTags(MAIN_TAG, useDlq() ? TupleTagList.of(DLQ_TAG) : TupleTagList.empty());
  }

  private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
    PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributes().fromTopic(getTopic());

    return (getTimestampAttribute() == null)
        ? read
        : read.withTimestampAttribute(getTimestampAttribute());
  }

  private PubsubIO.Write<PubsubMessage> writeMessagesToDlq() {
    PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(getDeadLetterQueue());

    return (getTimestampAttribute() == null)
        ? write
        : write.withTimestampAttribute(getTimestampAttribute());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("Writing to a Pubsub topic is not supported");
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setSchema(Schema schema);

    abstract Builder setTimestampAttribute(String timestampAttribute);

    abstract Builder setDeadLetterQueue(String deadLetterQueue);

    abstract Builder setTopic(String topic);

    abstract PubsubIOJsonTable build();
  }
}
