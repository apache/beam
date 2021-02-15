package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import org.apache.beam.sdk.schemas.Schema;
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
    if (field.getType().equals(FieldType.BYTES)) {
      return true;
    }
    return field.getType().getTypeName().equals(TypeName.ROW);
  }

  private static boolean hasNestedHeadersField(Schema schema) {
    if (!schema.hasField(HEADERS_FIELD)) {
      return false;
    }
    Field field = schema.getField(HEADERS_FIELD);
    return field.getType().equals(HEADERS_FIELD_TYPE);
  }

  static boolean isNestedSchema(Schema schema) {
    return hasNestedPayloadField(schema) && hasNestedHeadersField(schema);
  }
}
