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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
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
    validatePubsubMessageSchema(tableDefintion);

    JSONObject tableProperties = tableDefintion.getProperties();
    String timestampAttributeKey = tableProperties.getString("timestampAttributeKey");
    String deadLetterQueue = tableProperties.getString("deadLetterQueue");
    validateDlq(deadLetterQueue);

    return PubsubIOJsonTable.builder()
        .setSchema(tableDefintion.getSchema())
        .setTimestampAttribute(timestampAttributeKey)
        .setDeadLetterQueue(deadLetterQueue)
        .setTopic(tableDefintion.getLocation())
        .build();
  }

  private void validatePubsubMessageSchema(Table tableDefinition) {
    Schema schema = tableDefinition.getSchema();

    if (schema.getFieldCount() != 3
        || !fieldPresent(schema, TIMESTAMP_FIELD, TIMESTAMP)
        || !fieldPresent(
            schema, ATTRIBUTES_FIELD, Schema.FieldType.map(VARCHAR.withNullable(false), VARCHAR))
        || !(schema.hasField(PAYLOAD_FIELD)
            && ROW.equals(schema.getField(PAYLOAD_FIELD).getType().getTypeName()))) {

      throw new IllegalArgumentException(
          "Unsupported schema specified for Pubsub source in CREATE TABLE. "
              + "CREATE TABLE for Pubsub topic should define exactly the following fields: "
              + "'event_timestamp' field of type 'TIMESTAMP', 'attributes' field of type "
              + "MAP<VARCHAR, VARCHAR>, and 'payload' field of type 'ROW<...>' which matches the "
              + "payload JSON format.");
    }
  }

  private boolean fieldPresent(Schema schema, String field, Schema.FieldType expectedType) {
    return schema.hasField(field)
        && expectedType.equivalent(
            schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
  }

  private void validateDlq(String deadLetterQueue) {
    if (deadLetterQueue != null && deadLetterQueue.isEmpty()) {
      throw new IllegalArgumentException("Dead letter queue topic name is not specified");
    }
  }
}
