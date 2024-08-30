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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.joda.time.format.ISODateTimeFormat;

/** Write side {@link Row} to {@link PubsubMessage} converter. */
@Internal
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class PubsubRowToMessage extends PTransform<PCollection<Row>, PCollectionTuple> {

  static Builder builder() {
    return new AutoValue_PubsubRowToMessage.Builder();
  }

  static final TupleTag<PubsubMessage> OUTPUT = new TupleTag<PubsubMessage>() {};
  static final TupleTag<Row> ERROR = new TupleTag<Row>() {};
  static final String ERROR_DATA_FIELD_NAME = "data";
  static final Field ERROR_MESSAGE_FIELD = Field.of("error_message", FieldType.STRING);
  static final Field ERROR_STACK_TRACE_FIELD = Field.of("error_stack_trace", FieldType.STRING);

  static final String DEFAULT_KEY_PREFIX = "$";
  static final String ATTRIBUTES_KEY_NAME = "pubsub_attributes";
  static final FieldType ATTRIBUTES_FIELD_TYPE = FieldType.map(FieldType.STRING, FieldType.STRING);

  static final String EVENT_TIMESTAMP_KEY_NAME = "pubsub_event_timestamp";
  static final FieldType EVENT_TIMESTAMP_FIELD_TYPE = FieldType.DATETIME;

  static final String PAYLOAD_KEY_NAME = "pubsub_payload";
  static final TypeName PAYLOAD_BYTES_TYPE_NAME = TypeName.BYTES;
  static final TypeName PAYLOAD_ROW_TYPE_NAME = TypeName.ROW;
  static final String DEFAULT_ATTRIBUTES_KEY_NAME = DEFAULT_KEY_PREFIX + ATTRIBUTES_KEY_NAME;
  static final String DEFAULT_EVENT_TIMESTAMP_KEY_NAME =
      DEFAULT_KEY_PREFIX + EVENT_TIMESTAMP_KEY_NAME;
  static final String DEFAULT_PAYLOAD_KEY_NAME = DEFAULT_KEY_PREFIX + PAYLOAD_KEY_NAME;

  /** The prefix for all non-user fields. Defaults to {@link #DEFAULT_KEY_PREFIX}. */
  abstract String getKeyPrefix();

  /**
   * The {@link PayloadSerializer} to {@link PayloadSerializer#serialize(Row)} the payload or user
   * fields row.
   */
  @Nullable
  abstract PayloadSerializer getPayloadSerializer();

  /** The name of the attribute to apply to the {@link PubsubMessage}. */
  @Nullable
  abstract String getTargetTimestampAttributeName();

  /**
   * Use for testing, simplify assertions of generated timestamp when input lacks a timestamp field.
   */
  @Nullable
  abstract Instant getMockInstant();

  /** Generates {@link Schema} of the {@link #ERROR} {@link PCollection}. */
  static Schema errorSchema(Schema inputSchema) {
    Field dataField = Field.of(ERROR_DATA_FIELD_NAME, FieldType.row(inputSchema));
    return Schema.of(dataField, ERROR_MESSAGE_FIELD, ERROR_STACK_TRACE_FIELD);
  }

  /**
   * As a convenience method, generates {@link InputSchemaFactory} for expected {@link Schema} for
   * {@link Row} input into {@link PubsubRowToMessage}, excluding {@link Field} for {@link
   * #getPayloadKeyName()}. See {@link InputSchemaFactory#buildSchema(Field...)} for details on how
   * to add additional fields.
   */
  InputSchemaFactory inputSchemaFactory() {
    return inputSchemaFactory(null);
  }

  /**
   * As a convenience method, generates {@link InputSchemaFactory} for expected {@link Schema} for
   * {@link Row} input into {@link PubsubRowToMessage}. The {@link Field} for {@link
   * #getPayloadKeyName()} is excluded for null {@param payloadFieldType}. See {@link
   * InputSchemaFactory#buildSchema(Field...)} for details on how to add additional fields.
   */
  InputSchemaFactory inputSchemaFactory(@Nullable FieldType payloadFieldType) {
    InputSchemaFactory.Builder builder =
        InputSchemaFactory.builder()
            .setAttributesField(Field.of(getAttributesKeyName(), ATTRIBUTES_FIELD_TYPE))
            .setTimestampField(
                Field.of(getSourceEventTimestampKeyName(), EVENT_TIMESTAMP_FIELD_TYPE));

    if (payloadFieldType != null) {
      builder = builder.setPayloadField(Field.of(getPayloadKeyName(), payloadFieldType));
    }

    return builder.build();
  }

  @Override
  public PCollectionTuple expand(PCollection<Row> input) {
    Schema schema = input.getSchema();
    validate(schema);
    Schema errorSchema = errorSchema(schema);
    PCollectionTuple pct =
        input.apply(
            PubsubRowToMessage.class.getSimpleName(),
            ParDo.of(
                    new PubsubRowToMessageDoFn(
                        getAttributesKeyName(),
                        getSourceEventTimestampKeyName(),
                        getPayloadKeyName(),
                        errorSchema,
                        getTargetTimestampAttributeName(),
                        getMockInstant(),
                        getPayloadSerializer()))
                .withOutputTags(OUTPUT, TupleTagList.of(ERROR)));

    PCollection<PubsubMessage> output = pct.get(OUTPUT);
    PCollection<Row> error = pct.get(ERROR).setRowSchema(errorSchema);
    return PCollectionTuple.of(OUTPUT, output).and(ERROR, error);
  }

  /** Returns the name of the source attributes key, prefixed with {@link #getKeyPrefix()}. */
  String getAttributesKeyName() {
    return getKeyPrefix() + ATTRIBUTES_KEY_NAME;
  }

  /** Returns the name of the source timestamp key, prefixed with {@link #getKeyPrefix()}. */
  String getSourceEventTimestampKeyName() {
    return getKeyPrefix() + EVENT_TIMESTAMP_KEY_NAME;
  }

  /** Returns the name of the source payload key, prefixed with {@link #getKeyPrefix()}. */
  String getPayloadKeyName() {
    return getKeyPrefix() + PAYLOAD_KEY_NAME;
  }

  /** Validates an input's {@link Schema} for correctness. */
  void validate(Schema schema) {

    if (schema.getFieldCount() == 0) {
      throw new IllegalArgumentException(
          String.format("Schema must contain at least one field. Schema: %s", schema));
    }

    validateAttributesField(schema);
    validateSourceEventTimeStampField(schema);
    validateSerializableFields(schema);
  }

  /**
   * Validates an input's {@link Schema} for its {@link #getAttributesKeyName()} field correctness,
   * if exists.
   */
  void validateAttributesField(Schema schema) {
    String attributesKeyName = getAttributesKeyName();
    if (!schema.hasField(attributesKeyName)) {
      return;
    }
    checkArgument(
        SchemaReflection.of(schema)
            .matchesAll(FieldMatcher.of(attributesKeyName, ATTRIBUTES_FIELD_TYPE)));
  }

  /**
   * Validates an input's {@link Schema} for its {@link #getSourceEventTimestampKeyName()} ()} field
   * correctness, if exists.
   */
  void validateSourceEventTimeStampField(Schema schema) {
    String eventTimestampKeyName = getSourceEventTimestampKeyName();
    if (!schema.hasField(eventTimestampKeyName)) {
      return;
    }
    checkArgument(
        SchemaReflection.of(schema)
            .matchesAll(FieldMatcher.of(eventTimestampKeyName, EVENT_TIMESTAMP_FIELD_TYPE)));
  }

  /**
   * Validates an input's {@link Schema} for either {@link #getPayloadKeyName()} or user fields
   * correctness. Additionally, it validates the {@link #getPayloadSerializer()} null state based on
   * the {@link #getPayloadKeyName()} {@link FieldType} or the presence of user fields.
   */
  void validateSerializableFields(Schema schema) {
    String attributesKeyName = getAttributesKeyName();
    String eventTimestampKeyName = getSourceEventTimestampKeyName();
    String payloadKeyName = getPayloadKeyName();
    Schema withUserFieldsOnly =
        removeFields(schema, attributesKeyName, eventTimestampKeyName, payloadKeyName);
    boolean hasUserFields = withUserFieldsOnly.getFieldCount() > 0;
    String withUserFieldsList = String.join(", ", withUserFieldsOnly.getFieldNames());
    SchemaReflection schemaReflection = SchemaReflection.of(schema);
    boolean hasPayloadField = schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName));
    boolean hasPayloadRowField =
        schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_ROW_TYPE_NAME));
    boolean hasPayloadBytesField =
        schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_BYTES_TYPE_NAME));
    boolean hasBothUserFieldsAndPayloadField = hasUserFields && hasPayloadField;

    checkArgument(
        hasUserFields || hasPayloadField,
        String.format(
            "schema must have either a %s field or user fields i.e. not %s, %s or %s",
            payloadKeyName, attributesKeyName, eventTimestampKeyName, payloadKeyName));

    checkArgument(
        !hasBothUserFieldsAndPayloadField,
        String.format(
            "schema field: %s incompatible with %s fields", payloadKeyName, withUserFieldsList));

    if (hasPayloadBytesField) {
      checkArgument(
          getPayloadSerializer() == null,
          String.format(
              "schema field: %s of type: %s with a %s is incompatible",
              payloadKeyName, PAYLOAD_BYTES_TYPE_NAME, PayloadSerializer.class.getName()));
    }

    if (hasPayloadRowField) {
      checkArgument(
          getPayloadSerializer() != null,
          String.format(
              "schema field: %s of type: %s requires a %s",
              payloadKeyName, PAYLOAD_ROW_TYPE_NAME, PayloadSerializer.class.getName()));
    }

    if (hasUserFields) {
      checkArgument(
          getPayloadSerializer() != null,
          String.format(
              "specifying schema fields: %s requires a %s",
              withUserFieldsList, PayloadSerializer.class.getName()));
    }
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setKeyPrefix(String value);

    abstract Optional<String> getKeyPrefix();

    abstract Builder setPayloadSerializer(PayloadSerializer value);

    abstract Builder setTargetTimestampAttributeName(String value);

    abstract Builder setMockInstant(Instant value);

    abstract PubsubRowToMessage autoBuild();

    final PubsubRowToMessage build() {
      if (!getKeyPrefix().isPresent()) {
        setKeyPrefix(DEFAULT_KEY_PREFIX);
      }
      return autoBuild();
    }
  }

  /** {@link SchemaReflection} is a helper class for reflecting fields of a {@link Schema}. */
  static class SchemaReflection {
    static SchemaReflection of(Schema schema) {
      return new SchemaReflection(schema);
    }

    private final Schema schema;

    private SchemaReflection(Schema schema) {
      this.schema = schema;
    }

    /** Returns true of all {@param fieldMatchers} {@link FieldMatcher#match(Schema)}. */
    boolean matchesAll(FieldMatcher... fieldMatchers) {
      for (FieldMatcher fieldMatcher : fieldMatchers) {
        if (!fieldMatcher.match(schema)) {
          return false;
        }
      }
      return true;
    }

    /** Returns true of any {@param fieldMatchers} {@link FieldMatcher#match(Schema)}. */
    boolean matchesAny(FieldMatcher... fieldMatchers) {
      for (FieldMatcher fieldMatcher : fieldMatchers) {
        if (fieldMatcher.match(schema)) {
          return true;
        }
      }
      return false;
    }
  }

  /** {@link FieldMatcher} matches fields in a {@link Schema}. */
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

    @Nullable private final TypeName typeName;

    @Nullable private final FieldType fieldType;

    private FieldMatcher(String name, @Nullable TypeName typeName, @Nullable FieldType fieldType) {
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

    /** Returns true when the {@link Field} in a {@link Schema} matches its search criteria. */
    boolean match(Schema schema) {
      if (!schema.hasField(name)) {
        return false;
      }
      if (typeName == null && fieldType == null) {
        return true;
      }
      Field field = schema.getField(name);
      if (typeName != null) {
        return field.getType().getTypeName().equals(typeName);
      }
      return fieldType.equals(field.getType());
    }
  }

  /** Removes fields from a {@link Schema}. */
  static Schema removeFields(Schema schema, String... fields) {
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

  /** A {@link DoFn} that converts a {@link Row} to a {@link PubsubMessage}. */
  static class PubsubRowToMessageDoFn extends DoFn<Row, PubsubMessage> {

    private final String attributesKeyName;
    private final String sourceTimestampKeyName;
    private final String payloadKeyName;
    private final Schema errorSchema;

    @Nullable private final String targetTimestampKeyName;
    @Nullable private final PayloadSerializer payloadSerializer;

    @Nullable private final Instant mockInstant;

    PubsubRowToMessageDoFn(
        String attributesKeyName,
        String sourceTimestampKeyName,
        String payloadKeyName,
        Schema errorSchema,
        @Nullable String targetTimestampKeyName,
        @Nullable Instant mockInstant,
        @Nullable PayloadSerializer payloadSerializer) {
      this.attributesKeyName = attributesKeyName;
      this.sourceTimestampKeyName = sourceTimestampKeyName;
      this.payloadKeyName = payloadKeyName;
      this.errorSchema = errorSchema;
      this.targetTimestampKeyName = targetTimestampKeyName;
      this.payloadSerializer = payloadSerializer;
      this.mockInstant = mockInstant;
    }

    @ProcessElement
    public void process(@Element Row row, MultiOutputReceiver receiver) {
      try {

        Map<String, String> attributesWithoutTimestamp = this.attributesWithoutTimestamp(row);
        String timestampAsString = this.timestampAsString(row);
        String timestampKeyName = sourceTimestampKeyName;
        if (targetTimestampKeyName != null) {
          timestampKeyName = targetTimestampKeyName;
        }
        byte[] payload = this.payload(row);
        HashMap<String, String> attributes = new HashMap<>(attributesWithoutTimestamp);
        attributes.put(timestampKeyName, timestampAsString);
        PubsubMessage message = new PubsubMessage(payload, attributes);
        receiver.get(OUTPUT).output(message);

      } catch (Exception e) {

        String message = e.getMessage();
        String stackTrace = Throwables.getStackTraceAsString(e);
        Row error =
            Row.withSchema(errorSchema)
                .withFieldValue(ERROR_DATA_FIELD_NAME, row)
                .withFieldValue(ERROR_MESSAGE_FIELD.getName(), message)
                .withFieldValue(ERROR_STACK_TRACE_FIELD.getName(), stackTrace)
                .build();

        receiver.get(ERROR).output(error);
      }
    }

    /**
     * Extracts the {@code Map<String, String>} attributes from a {@link Row} that contains the
     * {@link #attributesKeyName}.
     */
    Map<String, String> attributesWithoutTimestamp(Row row) {
      if (!row.getSchema().hasField(attributesKeyName)) {
        return new HashMap<>();
      }
      return row.getMap(attributesKeyName);
    }

    /**
     * Outputs the {@link #timestamp(Row)} as a String in RFC 3339 format. For example, {@code
     * 2015-10-29T23:41:41.123Z}.
     */
    String timestampAsString(Row row) {
      return ISODateTimeFormat.dateTime().print(timestamp(row));
    }

    /**
     * Extracts a {@link ReadableDateTime} from a {@link Row} containing a {@link
     * #sourceTimestampKeyName}. If the {@link Field} is missing, returns an {@link Instant#now()}.
     */
    ReadableDateTime timestamp(Row row) {
      if (row.getSchema().hasField(sourceTimestampKeyName)) {
        return row.getDateTime(sourceTimestampKeyName);
      }
      Instant instant = Instant.now();
      if (mockInstant != null) {
        instant = mockInstant;
      }
      return new DateTime(instant).withZone(instant.getZone());
    }

    /**
     * Extracts a {@code byte[]} payload from a {@link Row}, from either of the following mutually
     * exclusive sources. <br>
     * - {@link #payloadKeyName} {@link Field} with {@link FieldType#BYTES} <br>
     * - serialized {@link #payloadKeyName} {@link Field} with {@link TypeName#ROW} using the {@link
     * #payloadSerializer} <br>
     * - serialized user fields provided that are not {@link #attributesKeyName} and {@link
     * #sourceTimestampKeyName}
     */
    byte[] payload(Row row) {
      if (SchemaReflection.of(row.getSchema())
          .matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_BYTES_TYPE_NAME))) {
        return row.getBytes(payloadKeyName);
      }
      return Objects.requireNonNull(payloadSerializer).serialize(serializableRow(row));
    }

    /**
     * Extracts the serializable part of a {@link Row} from the following mutually exclusive
     * sources. <br>
     * - serialized {@link #payloadKeyName} {@link Field} with {@link TypeName#ROW} using the {@link
     * #payloadSerializer} <br>
     * - serialized user fields provided that are not {@link #attributesKeyName} and {@link
     * #sourceTimestampKeyName}
     */
    Row serializableRow(Row row) {
      SchemaReflection schemaReflection = SchemaReflection.of(row.getSchema());

      if (schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_BYTES_TYPE_NAME))) {
        throw new IllegalArgumentException(
            String.format(
                "serializable Row does not exist for payload of type: %s",
                PAYLOAD_BYTES_TYPE_NAME));
      }

      if (schemaReflection.matchesAll(FieldMatcher.of(payloadKeyName, PAYLOAD_ROW_TYPE_NAME))) {
        return row.getRow(payloadKeyName);
      }
      Schema withUserFieldsOnly =
          removeFields(row.getSchema(), attributesKeyName, sourceTimestampKeyName);
      Map<String, Object> values = new HashMap<>();
      for (String name : withUserFieldsOnly.getFieldNames()) {
        values.put(name, row.getValue(name));
      }
      return Row.withSchema(withUserFieldsOnly).withFieldValues(values).build();
    }
  }

  /**
   * A convenience class for generating the expected {@link Schema} for {@link Row} input of {@link
   * PubsubRowToMessage}.
   */
  @AutoValue
  abstract static class InputSchemaFactory {
    static Builder builder() {
      return new AutoValue_PubsubRowToMessage_InputSchemaFactory.Builder();
    }

    abstract Field getAttributesField();

    abstract Field getTimestampField();

    @Nullable
    abstract Field getPayloadField();

    /**
     * Builds a {@link Schema} from {@link #getAttributesField()} and {@link #getTimestampField()}
     * and {@param additionalFields}. Users are encouraged to use the {@link #removeFields(Schema,
     * String...)} method to customize the resulting {@link Schema}.
     */
    Schema buildSchema(Field... additionalFields) {
      Schema.Builder builder =
          Schema.builder().addField(getAttributesField()).addField(getTimestampField());

      if (getPayloadField() != null) {
        builder = builder.addField(getPayloadField());
      }

      for (Field field : additionalFields) {
        builder = builder.addField(field);
      }

      return builder.build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAttributesField(Field value);

      abstract Builder setTimestampField(Field value);

      abstract Builder setPayloadField(Field value);

      abstract InputSchemaFactory build();
    }
  }
}
