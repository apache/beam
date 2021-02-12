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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_ARRAY_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_MAP_FIELD_TYPE;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

class NestedRowToMessage extends SimpleFunction<Row, PubsubMessage> {
  private static final long serialVersionUID = 65176815766314684L;

  private final PayloadSerializer serializer;

  NestedRowToMessage(PayloadSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public PubsubMessage apply(Row row) {
    Schema schema = row.getSchema();
    ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder();
    if (schema.getField(ATTRIBUTES_FIELD).getType().equals(ATTRIBUTE_MAP_FIELD_TYPE)) {
      attributes.putAll(checkArgumentNotNull(row.getMap(ATTRIBUTES_FIELD)));
    } else {
      checkArgument(schema.getField(ATTRIBUTES_FIELD).getType().equals(ATTRIBUTE_ARRAY_FIELD_TYPE));
      Collection<Row> attributeEntries = checkArgumentNotNull(row.getArray(ATTRIBUTES_FIELD));
      for (Row entry : attributeEntries) {
        attributes.put(
            checkArgumentNotNull(entry.getString("key")),
            checkArgumentNotNull(entry.getString("value")));
      }
    }
    @Nonnull byte[] payload;
    if (schema.getField(PAYLOAD_FIELD).getType().equals(FieldType.BYTES)) {
      payload = checkArgumentNotNull(row.getBytes(PAYLOAD_FIELD));
    } else {
      checkArgument(schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
      payload = serializer.serialize(checkArgumentNotNull(row.getRow(PAYLOAD_FIELD)));
    }
    return new PubsubMessage(payload, attributes.build());
  }
}
