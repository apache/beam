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
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;

/**
 * Builds a {@link PubsubMessageToRow} from a {@link PubsubReadSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
@Experimental(Kind.SCHEMAS)
class PubsubSchemaTransformMessageToRowFactory {
  private static final String DEFAULT_FORMAT = "json";

  private static final Schema.FieldType ATTRIBUTE_MAP_FIELD_TYPE =
      Schema.FieldType.map(Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING);
  private static final Schema ATTRIBUTE_ARRAY_ENTRY_SCHEMA =
      Schema.builder().addStringField("key").addStringField("value").build();
  private static final Schema.FieldType ATTRIBUTE_ARRAY_FIELD_TYPE =
      Schema.FieldType.array(Schema.FieldType.row(ATTRIBUTE_ARRAY_ENTRY_SCHEMA));

  private static final String THRIFT_CLASS_KEY = "thriftClass";
  private static final String THRIFT_PROTOCOL_FACTORY_CLASS_KEY = "thriftProtocolFactoryClass";
  private static final String PROTO_CLASS_KEY = "protoClass";

  /**
   * Instantiate a {@link PubsubSchemaTransformMessageToRowFactory} from a {@link
   * PubsubReadSchemaTransformConfiguration}.
   */
  static PubsubSchemaTransformMessageToRowFactory from(
      PubsubReadSchemaTransformConfiguration configuration) {
    return new PubsubSchemaTransformMessageToRowFactory(configuration);
  }

  /** Build the {@link PubsubMessageToRow}. */
  PubsubMessageToRow buildMessageToRow() {
    PubsubMessageToRow.Builder builder =
        PubsubMessageToRow.builder()
            .messageSchema(configuration.getDataSchema())
            .useDlq(
                configuration.getDeadLetterQueue() != null
                    && !configuration.getDeadLetterQueue().isEmpty())
            .useFlatSchema(!shouldUseNestedSchema());

    if (needsSerializer()) {
      builder = builder.serializerProvider(serializer());
    }

    return builder.build();
  }

  private final PubsubReadSchemaTransformConfiguration configuration;

  private PubsubSchemaTransformMessageToRowFactory(
      PubsubReadSchemaTransformConfiguration configuration) {
    this.configuration = configuration;
  }

  private PayloadSerializer payloadSerializer() {
    Schema schema = configuration.getDataSchema();
    String format = DEFAULT_FORMAT;

    if (configuration.getFormat() != null && !configuration.getFormat().isEmpty()) {
      format = configuration.getFormat();
    }

    Map<String, Object> params = new HashMap<>();

    if (configuration.getThriftClass() != null && !configuration.getThriftClass().isEmpty()) {
      params.put(THRIFT_CLASS_KEY, configuration.getThriftClass());
    }

    if (configuration.getThriftProtocolFactoryClass() != null
        && !configuration.getThriftProtocolFactoryClass().isEmpty()) {
      params.put(THRIFT_PROTOCOL_FACTORY_CLASS_KEY, configuration.getThriftProtocolFactoryClass());
    }

    if (configuration.getProtoClass() != null && !configuration.getProtoClass().isEmpty()) {
      params.put(PROTO_CLASS_KEY, configuration.getProtoClass());
    }

    return PayloadSerializers.getSerializer(format, schema, params);
  }

  PubsubMessageToRow.SerializerProvider serializer() {
    return input -> payloadSerializer();
  }

  /**
   * Determines whether the {@link PubsubMessageToRow} needs a {@link
   * PubsubMessageToRow.SerializerProvider}.
   *
   * <p>The determination is based on {@link #shouldUseNestedSchema()} is false or if the {@link
   * PubsubMessageToRow#PAYLOAD_FIELD} is not present.
   */
  boolean needsSerializer() {
    return !shouldUseNestedSchema() || !fieldPresent(PAYLOAD_FIELD, Schema.FieldType.BYTES);
  }

  /**
   * Determines whether a nested schema should be used for {@link
   * PubsubReadSchemaTransformConfiguration#getDataSchema()}.
   *
   * <p>The determination is based on {@link #schemaHasValidPayloadField()} and {@link
   * #schemaHasValidAttributesField()}}
   */
  boolean shouldUseNestedSchema() {
    return schemaHasValidPayloadField() && schemaHasValidAttributesField();
  }

  /**
   * Determines whether {@link PubsubReadSchemaTransformConfiguration#getDataSchema()} has a valid
   * {@link PubsubMessageToRow#PAYLOAD_FIELD}.
   */
  boolean schemaHasValidPayloadField() {
    Schema schema = configuration.getDataSchema();
    if (!schema.hasField(PAYLOAD_FIELD)) {
      return false;
    }
    if (fieldPresent(PAYLOAD_FIELD, Schema.FieldType.BYTES)) {
      return true;
    }
    return schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(ROW);
  }

  /**
   * Determines whether {@link PubsubReadSchemaTransformConfiguration#getDataSchema()} has a valid
   * {@link PubsubMessageToRow#ATTRIBUTES_FIELD} field.
   *
   * <p>The determination is based on whether {@link #fieldPresent(String, Schema.FieldType)} for
   * {@link PubsubMessageToRow#ATTRIBUTES_FIELD} is true for either {@link
   * #ATTRIBUTE_MAP_FIELD_TYPE} or {@link #ATTRIBUTE_ARRAY_FIELD_TYPE} {@link Schema.FieldType}s.
   */
  boolean schemaHasValidAttributesField() {
    return fieldPresent(ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE)
        || fieldPresent(ATTRIBUTES_FIELD, ATTRIBUTE_ARRAY_FIELD_TYPE);
  }

  /**
   * Determines whether {@link PubsubReadSchemaTransformConfiguration#getDataSchema()} contains the
   * field and whether that field is an expectedType {@link Schema.FieldType}.
   */
  boolean fieldPresent(String field, Schema.FieldType expectedType) {
    Schema schema = configuration.getDataSchema();
    return schema.hasField(field)
        && expectedType.equivalent(
            schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
  }
}
