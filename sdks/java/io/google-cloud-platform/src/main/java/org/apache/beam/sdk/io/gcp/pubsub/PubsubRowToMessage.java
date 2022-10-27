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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** Write side {@link Row} to {@link PubsubMessage} converter. */
@Internal
@Experimental
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class PubsubRowToMessage extends PTransform<PCollection<Row>, PCollectionTuple> {
  static TupleTag<PubsubMessage> OUTPUT = new TupleTag<PubsubMessage>() {};
  static TupleTag<Row> ERROR = new TupleTag<Row>() {};

  private static final String DEFAULT_KEY_PREFIX = "$";
  private static final String ATTRIBUTES_KEY_NAME = "pubsub_attributes";
  private static final FieldType ATTRIBUTES_FIELD_TYPE =
      FieldType.map(FieldType.STRING, FieldType.STRING);

  private static final String EVENT_TIMESTAMP_KEY_NAME = "pubsub_event_timestamp";
  private static final FieldType EVENT_TIMESTAMP_FIELD_TYPE = FieldType.DATETIME;

  private static final String PAYLOAD_KEY_NAME = "pubsub_payload";
  private static final TypeName PAYLOAD_BYTES_TYPE_NAME = TypeName.BYTES;
  private static final TypeName PAYLOAD_ROW_TYPE_NAME = TypeName.ROW;

  abstract String getKeyPrefix();

  @Nullable
  abstract PayloadSerializer getPayloadSerializer();

  @Nullable
  abstract String getTimestampAttributeName();

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    return null;
  }

  String getAttributesKeyName() {
    return getKeyPrefix() + ATTRIBUTES_KEY_NAME;
  }

  String getEventTimestampKeyName() {
    return getKeyPrefix() + EVENT_TIMESTAMP_KEY_NAME;
  }

  String getPayloadKeyName() {
    return getKeyPrefix() + PAYLOAD_KEY_NAME;
  }

  void validate(Schema schema) {
    validateAttributesField(schema);
    validateEventTimeStampField(schema);
    validatePayloadField(schema);
  }

  void validateAttributesField(Schema schema) {
    String attributesKeyName = getAttributesKeyName();
    if (!schema.hasField(attributesKeyName)) {
      return;
    }
    checkArgument(SchemaReflection.of(schema).matchesAll(FieldMatcher.of(attributesKeyName, ATTRIBUTES_FIELD_TYPE)));
  }

  void validateEventTimeStampField(Schema schema) {
    String eventTimestampKeyName = getEventTimestampKeyName();
    if (!schema.hasField(eventTimestampKeyName)) {
      return;
    }
    checkArgument(SchemaReflection.of(schema).matchesAll(FieldMatcher.of(eventTimestampKeyName, EVENT_TIMESTAMP_FIELD_TYPE)));
  }

  void validatePayloadField(Schema schema) {
    String attributesKeyName = getAttributesKeyName();
    String eventTimestampKeyName = getEventTimestampKeyName();
    String payloadKeyName = getPayloadKeyName();
    Schema withUserFieldsOnly = removeFields(schema, attributesKeyName, eventTimestampKeyName, payloadKeyName);
    boolean hasUserFields = withUserFieldsOnly.getFieldCount() > 0;
    String withUserFieldsList = String.join(", ", withUserFieldsOnly.getFieldNames());
    SchemaReflection schemaReflection = SchemaReflection.of(schema);
    boolean hasPayloadField = schemaReflection.matchesAll(FieldMatcher.of(name));
    boolean hasPayloadRowField = schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName,
        PAYLOAD_ROW_TYPE_NAME));
    boolean hasBothUserFieldsAndPayloadField = hasUserFields && hasPayloadField;

    checkArgument(!hasBothUserFieldsAndPayloadField,
        String.format("schema field: %s incompatible with %s fields", payloadKeyName, withUserFieldsList)
    );
    checkArgument(hasPayloadRowField && getPayloadSerializer() != null, String.format("schema field: %s of type: %s requires a %s", payloadKeyName, PAYLOAD_ROW_TYPE_NAME, PayloadSerializer.class.getName()));
    checkArgument(hasUserFields && getPayloadSerializer() != null, String.format("specifying schema fields: %s requires a %s", withUserFieldsList, PayloadSerializer.class.getName()));
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setKeyPrefix(String value);

    abstract Optional<String> getKeyPrefix();

    abstract Builder setPayloadSerializer(PayloadSerializer value);

    abstract Builder setTimestampAttributeName(String value);

    abstract PubsubRowToMessage autoBuild();

    final PubsubRowToMessage build() {
      if (!getKeyPrefix().isPresent()) {
        setKeyPrefix(DEFAULT_KEY_PREFIX);
      }
      return autoBuild();
    }
  }

  static class SchemaReflection {
    static SchemaReflection of(Schema schema) {
      return new SchemaReflection(schema);
    }
    private final Schema schema;

    private SchemaReflection(Schema schema) {
      this.schema = schema;
    }

    boolean matchesAll(FieldMatcher ...fieldMatchers) {
      for (FieldMatcher fieldMatcher : fieldMatchers) {
        if (!fieldMatcher.match(schema)) {
          return false;
        }
      }
      return true;
    }
  }

  static class FieldMatcher {
    static FieldMatcher of(String name) {
      return new FieldMatcher(name);
    }
    static FieldMatcher of(String name, TypeName typeName) {
      return new FieldMatcher(name, typeName);
    }
    static FieldMatcher of(String name, FieldType fieldType) {
      return new FieldMatcher(name, fieldType);
    }
    private final String name;

    @Nullable
    private final TypeName typeName;

    @Nullable
    private final FieldType fieldType;

    private FieldMatcher(String name, @Nullable TypeName typeName,
        @Nullable FieldType fieldType) {
      this.name = name;
      this.typeName = typeName;
      this.fieldType = fieldType;
    }

    private FieldMatcher(String name) {
      this(name, null, null);
    }

    private FieldMatcher(String name, TypeName typeName) {
      this(name, typeName, null);
    }

    private FieldMatcher(String name, FieldType fieldType) {
      this(name, null, fieldType);
    }

    boolean match(Schema schema) {
      if (typeName == null && fieldType == null) {
        return schema.hasField(name);
      }
      Field field = schema.getField(name);
      if (typeName != null) {
        return field.getType().getTypeName().equals(typeName);
      }
      return fieldType.equals(field.getType());
    }
  }

  static Schema removeFields(Schema schema, String ...fields) {
    List<String> exclude = Arrays.stream(fields).collect(Collectors.toList());
    Schema.Builder builder = Schema.builder();
    for (Field field : schema.getFields()) {
      if (exclude.contains(field.getName())) {
        continue;
      }
      builder.addField(field);
    }
    return builder.build();
  }

  static class PubsubRowToMessageDoFn extends DoFn<Row, PubsubMessage> {
    PubsubRowToMessageDoFn from(PubsubRowToMessage spec) {
      return new PubsubRowToMessageDoFn(spec.getAttributesKeyName(), spec.getEventTimestampKeyName(), spec.getPayloadKeyName(), spec.getPayloadSerializer());
    }
    private final String attributesKeyName;
    private final String eventTimestampKeyName;
    private final String payloadKeyName;

    @Nullable
    private PayloadSerializer payloadSerializer;

    PubsubRowToMessageDoFn(String attributesKeyName, String eventTimestampKeyName,
        String payloadKeyName,
        @Nullable PayloadSerializer payloadSerializer) {
      this.attributesKeyName = attributesKeyName;
      this.eventTimestampKeyName = eventTimestampKeyName;
      this.payloadKeyName = payloadKeyName;
      this.payloadSerializer = payloadSerializer;
    }

    @ProcessElement
    public void process(@Element Row row, MultiOutputReceiver receiver) {

    }

    Map<String, String> attributes(Row row) {
      if (!row.getSchema().hasField(attributesKeyName)) {
        return ImmutableMap.of();
      }
      return row.getMap(attributesKeyName);
    }

    byte[] payload(Row row) {
      if (SchemaReflection.of(row.getSchema()).matchesAll(FieldMatcher.of(payloadKeyName,
          PAYLOAD_BYTES_TYPE_NAME))) {
        return row.getBytes(payloadKeyName);
      }
      return Objects.requireNonNull(payloadSerializer).serialize(serializableRow(row));
    }

    Row serializableRow(Row row) {
      SchemaReflection schemaReflection = SchemaReflection.of(row.getSchema());
      if (schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_ROW_TYPE_NAME))) {
        return row.getRow(payloadKeyName);
      }
      Schema withUserFieldsOnly = removeFields(row.getSchema(), attributesKeyName, eventTimestampKeyName);
      Map<String, Object> values = new HashMap<>();
      for (String name : withUserFieldsOnly.getFieldNames()) {
        values.put(name, row.getValue(name));
      }
      return Row.withSchema(withUserFieldsOnly).withFieldValues(values).build();
    }
  }
}
