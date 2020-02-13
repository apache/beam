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

import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.TIMESTAMP;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.InvalidTableException;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.Schema;

/**
 * {@link TableProvider} for {@link PubsubIOJsonTable} which wraps {@link PubsubIO} for consumption
 * by Beam SQL.
 */
@Internal
@Experimental
@AutoService(TableProvider.class)
public class PubsubJsonTableProvider extends InMemoryMetaTableProvider {

  @Override
  public String getTableType() {
    return "pubsub";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table tableDefintion) {
    JSONObject tableProperties = tableDefintion.getProperties();
    String timestampAttributeKey = tableProperties.getString("timestampAttributeKey");
    String deadLetterQueue = tableProperties.getString("deadLetterQueue");
    validateDlq(deadLetterQueue);

    Schema schema = tableDefintion.getSchema();
    validateEventTimestamp(schema);

    PubsubIOTableConfiguration config =
        PubsubIOTableConfiguration.builder()
            .setSchema(schema)
            .setTimestampAttribute(timestampAttributeKey)
            .setDeadLetterQueue(deadLetterQueue)
            .setTopic(tableDefintion.getLocation())
            .setUseFlatSchema(!definesAttributeAndPayload(schema))
            .build();

    return PubsubIOJsonTable.withConfiguration(config);
  }

  private void validateEventTimestamp(Schema schema) {
    if (!fieldPresent(schema, TIMESTAMP_FIELD, TIMESTAMP)) {
      throw new InvalidTableException(
          "Unsupported schema specified for Pubsub source in CREATE TABLE."
              + "CREATE TABLE for Pubsub topic must include at least 'event_timestamp' field of "
              + "type 'TIMESTAMP'");
    }
  }

  private boolean definesAttributeAndPayload(Schema schema) {
    return fieldPresent(
            schema, ATTRIBUTES_FIELD, Schema.FieldType.map(VARCHAR.withNullable(false), VARCHAR))
        && (schema.hasField(PAYLOAD_FIELD)
            && ROW.equals(schema.getField(PAYLOAD_FIELD).getType().getTypeName()));
  }

  private boolean fieldPresent(Schema schema, String field, Schema.FieldType expectedType) {
    return schema.hasField(field)
        && expectedType.equivalent(
            schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
  }

  private void validateDlq(String deadLetterQueue) {
    if (deadLetterQueue != null && deadLetterQueue.isEmpty()) {
      throw new InvalidTableException("Dead letter queue topic name is not specified");
    }
  }

  @AutoValue
  public abstract static class PubsubIOTableConfiguration implements Serializable {
    public boolean useDlq() {
      return getDeadLetterQueue() != null;
    }

    public boolean useTimestampAttribute() {
      return getTimestampAttribute() != null;
    }

    /** Determines whether or not the messages should be represented with a flattened schema. */
    abstract boolean getUseFlatSchema();

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

    /**
     * Pubsub topic name.
     *
     * <p>Topic is the only way to specify the Pubsub source. Explicitly specifying the subscription
     * is not supported at the moment. Subscriptions are automatically created (but not deleted).
     */
    abstract String getTopic();

    /**
     * Table schema, describes Pubsub message schema.
     *
     * <p>If {@link #getUseFlatSchema()} is not set, schema must contain exactly fields
     * 'event_timestamp', 'attributes, and 'payload'. Else, it must contain just 'event_timestamp'.
     * See {@linkA PubsubMessageToRow} for details.
     */
    public abstract Schema getSchema();

    static Builder builder() {
      return new AutoValue_PubsubJsonTableProvider_PubsubIOTableConfiguration.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUseFlatSchema(boolean useFlatSchema);

      abstract Builder setSchema(Schema schema);

      abstract Builder setTimestampAttribute(String timestampAttribute);

      abstract Builder setDeadLetterQueue(String deadLetterQueue);

      abstract Builder setTopic(String topic);

      abstract PubsubIOTableConfiguration build();
    }
  }
}
