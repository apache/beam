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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

class NestedRowToMessage extends SimpleFunction<Row, PubsubMessage> {
  private static final long serialVersionUID = 65176815766314684L;

  private final PayloadSerializer serializer;
  private final SerializableFunction<Row, Map<String, String>> attributesExtractor;
  private final SerializableFunction<Row, byte[]> payloadExtractor;

  @SuppressWarnings("methodref.receiver.bound")
  NestedRowToMessage(PayloadSerializer serializer, Schema schema) {
    this.serializer = serializer;
    if (schema.getField(ATTRIBUTES_FIELD).getType().equals(ATTRIBUTE_MAP_FIELD_TYPE)) {
      attributesExtractor = NestedRowToMessage::getAttributesFromMap;
    } else {
      checkArgument(schema.getField(ATTRIBUTES_FIELD).getType().equals(ATTRIBUTE_ARRAY_FIELD_TYPE));
      attributesExtractor = NestedRowToMessage::getAttributesFromArray;
    }
    if (schema.getField(PAYLOAD_FIELD).getType().equals(FieldType.BYTES)) {
      payloadExtractor = NestedRowToMessage::getPayloadFromBytes;
    } else {
      checkArgument(schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
      payloadExtractor = this::getPayloadFromNested;
    }
  }

  private static Map<String, String> getAttributesFromMap(Row row) {
    return ImmutableMap.<String, String>builder()
        .putAll(checkArgumentNotNull(row.getMap(ATTRIBUTES_FIELD)))
        .build();
  }

  private static Map<String, String> getAttributesFromArray(Row row) {
    ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder();
    Collection<Row> attributeEntries = checkArgumentNotNull(row.getArray(ATTRIBUTES_FIELD));
    for (Row entry : attributeEntries) {
      attributes.put(
          checkArgumentNotNull(entry.getString("key")),
          checkArgumentNotNull(entry.getString("value")));
    }
    return attributes.build();
  }

  private static byte[] getPayloadFromBytes(Row row) {
    return checkArgumentNotNull(row.getBytes(PAYLOAD_FIELD));
  }

  private byte[] getPayloadFromNested(Row row) {
    return serializer.serialize(checkArgumentNotNull(row.getRow(PAYLOAD_FIELD)));
  }

  @Override
  public PubsubMessage apply(Row row) {
    return new PubsubMessage(payloadExtractor.apply(row), attributesExtractor.apply(row));
  }
}
