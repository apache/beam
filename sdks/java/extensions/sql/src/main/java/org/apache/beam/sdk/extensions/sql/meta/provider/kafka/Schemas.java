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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.EquivalenceNullablePolicy;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;

final class Schemas {
  private Schemas() {}

  static final String MESSAGE_KEY_FIELD = "message_key";
  static final String EVENT_TIMESTAMP_FIELD = "event_timestamp";
  static final String HEADERS_FIELD = "headers";
  static final String PAYLOAD_FIELD = "payload";

  static final String HEADERS_KEY_FIELD = "key";
  static final String HEADERS_VALUES_FIELD = "values";
  static final Schema HEADERS_ENTRY_SCHEMA =
      Schema.builder()
          .addStringField(HEADERS_KEY_FIELD)
          .addArrayField(HEADERS_VALUES_FIELD, FieldType.BYTES)
          .build();
  static final Schema.FieldType HEADERS_FIELD_TYPE =
      Schema.FieldType.array(FieldType.row(HEADERS_ENTRY_SCHEMA));

  private static boolean hasNestedPayloadField(Schema schema) {
    if (!schema.hasField(PAYLOAD_FIELD)) {
      return false;
    }
    Field field = schema.getField(PAYLOAD_FIELD);
    if (fieldHasType(field, FieldType.BYTES)) {
      return true;
    }
    return field.getType().getTypeName().equals(TypeName.ROW);
  }

  private static boolean hasNestedHeadersField(Schema schema) {
    if (!schema.hasField(HEADERS_FIELD)) {
      return false;
    }
    return fieldHasType(schema.getField(HEADERS_FIELD), HEADERS_FIELD_TYPE);
  }

  static boolean isNestedSchema(Schema schema) {
    return hasNestedPayloadField(schema) && hasNestedHeadersField(schema);
  }

  private static boolean fieldHasType(Field field, FieldType type) {
    return type.equivalent(field.getType(), EquivalenceNullablePolicy.WEAKEN);
  }

  private static void checkFieldHasType(Field field, FieldType type) {
    checkArgument(
        fieldHasType(field, type),
        String.format("'%s' field must have schema matching '%s'.", field.getName(), type));
  }

  static void validateNestedSchema(Schema schema) {
    checkArgument(schema.hasField(PAYLOAD_FIELD), "Must provide a 'payload' field for Kafka.");
    for (Field field : schema.getFields()) {
      switch (field.getName()) {
        case HEADERS_FIELD:
          checkFieldHasType(field, HEADERS_FIELD_TYPE);
          break;
        case EVENT_TIMESTAMP_FIELD:
          checkFieldHasType(field, FieldType.DATETIME);
          break;
        case MESSAGE_KEY_FIELD:
          checkFieldHasType(field, FieldType.BYTES);
          break;
        case PAYLOAD_FIELD:
          checkArgument(
              fieldHasType(field, FieldType.BYTES)
                  || field.getType().getTypeName().equals(TypeName.ROW),
              String.format(
                  "'%s' field must either have a 'BYTES NOT NULL' or 'ROW' schema.",
                  field.getName()));
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "'%s' field is invalid at the top level for Kafka in the nested schema.",
                  field.getName()));
      }
    }
  }
}
