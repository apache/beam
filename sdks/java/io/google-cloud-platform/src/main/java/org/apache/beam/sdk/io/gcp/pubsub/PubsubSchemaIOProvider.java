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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.MAIN_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.InvalidSchemaException;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.io.SchemaIOProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link SchemaIOProvider} for reading and writing JSON/AVRO payloads with
 * {@link PubsubIO}.
 *
 * <h2>Schema</h2>
 *
 * <p>The data schema passed to {@link #from(String, Row, Schema)} must either be of the nested or
 * flat style.
 *
 * <h3>Nested style</h3>
 *
 * <p>If nested structure is used, the required fields included in the Pubsub message model are
 * 'event_timestamp', 'attributes', and 'payload'.
 *
 * <h3>Flat style</h3>
 *
 * <p>If flat structure is used, the required fields include just 'event_timestamp'. Every other
 * field is assumed part of the payload. See {@link PubsubMessageToRow} for details.
 *
 * <h2>Configuration</h2>
 *
 * <p>{@link #configurationSchema()} consists of two attributes, timestampAttributeKey and
 * deadLetterQueue.
 *
 * <h3>timestampAttributeKey</h3>
 *
 * <p>An optional attribute key of the Pubsub message from which to extract the event timestamp. If
 * not specified, the message publish time will be used as event timestamp.
 *
 * <p>This attribute has to conform to the same requirements as in {@link
 * PubsubIO.Read.Builder#withTimestampAttribute(String)}
 *
 * <p>Short version: it has to be either millis since epoch or string in RFC 3339 format.
 *
 * <p>If the attribute is specified then event timestamps will be extracted from the specified
 * attribute. If it is not specified then message publish timestamp will be used.
 *
 * <h3>deadLetterQueue</h3>
 *
 * <p>deadLetterQueue is an optional topic path which will be used as a dead letter queue.
 *
 * <p>Messages that cannot be processed will be sent to this topic. If it is not specified then
 * exception will be thrown for errors during processing causing the pipeline to crash.
 */
@Internal
@AutoService(SchemaIOProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubSchemaIOProvider implements SchemaIOProvider {
  public static final FieldType ATTRIBUTE_MAP_FIELD_TYPE =
      Schema.FieldType.map(FieldType.STRING.withNullable(false), FieldType.STRING);
  public static final Schema ATTRIBUTE_ARRAY_ENTRY_SCHEMA =
      Schema.builder().addStringField("key").addStringField("value").build();
  public static final FieldType ATTRIBUTE_ARRAY_FIELD_TYPE =
      Schema.FieldType.array(Schema.FieldType.row(ATTRIBUTE_ARRAY_ENTRY_SCHEMA));

  /** Returns an id that uniquely represents this IO. */
  @Override
  public String identifier() {
    return "pubsub";
  }

  /**
   * Returns the expected schema of the configuration object. Note this is distinct from the schema
   * of the data source itself.
   */
  @Override
  public Schema configurationSchema() {
    return Schema.builder()
        .addNullableField("timestampAttributeKey", FieldType.STRING)
        .addNullableField("deadLetterQueue", FieldType.STRING)
        .addNullableField("format", FieldType.STRING)
        // For ThriftPayloadSerializerProvider
        .addNullableField("thriftClass", FieldType.STRING)
        .addNullableField("thriftProtocolFactoryClass", FieldType.STRING)
        // For ProtoPayloadSerializerProvider
        .addNullableField("protoClass", FieldType.STRING)
        .build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public PubsubSchemaIO from(String location, Row configuration, Schema dataSchema) {
    validateConfigurationSchema(configuration);
    validateDlq(configuration.getValue("deadLetterQueue"));
    validateDataSchema(dataSchema);
    return new PubsubSchemaIO(location, configuration, dataSchema);
  }

  @Override
  public boolean requiresDataSchema() {
    return true;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.UNBOUNDED;
  }

  private void validateDataSchema(Schema schema) {
    if (schema == null) {
      throw new InvalidSchemaException(
          "Unsupported schema specified for Pubsub source in CREATE TABLE."
              + "CREATE TABLE for Pubsub topic must not be null");
    }
    if (!PubsubSchemaIO.fieldPresent(schema, TIMESTAMP_FIELD, FieldType.DATETIME)) {
      throw new InvalidSchemaException(
          "Unsupported schema specified for Pubsub source in CREATE TABLE."
              + "CREATE TABLE for Pubsub topic must include at least 'event_timestamp' field of "
              + "type 'TIMESTAMP'");
    }
  }

  private void validateDlq(String deadLetterQueue) {
    if (deadLetterQueue != null && deadLetterQueue.isEmpty()) {
      throw new InvalidConfigurationException("Dead letter queue topic name is not specified");
    }
  }

  private void validateConfigurationSchema(Row configuration) {
    if (!configuration.getSchema().equals(configurationSchema())) {
      throw new InvalidConfigurationException(
          "Configuration schema provided does not match expected");
    }
  }

  /** An abstraction to create schema aware IOs. */
  private static class PubsubSchemaIO implements SchemaIO, Serializable {
    protected final Schema dataSchema;
    protected final String location;
    protected final boolean useFlatSchema;
    protected final Config config;

    private PubsubSchemaIO(String location, Row config, Schema dataSchema) {
      this.dataSchema = dataSchema;
      this.location = location;
      this.useFlatSchema = !shouldUseNestedSchema(dataSchema);
      this.config =
          new AutoValueSchema().fromRowFunction(TypeDescriptor.of(Config.class)).apply(config);
    }

    @Override
    public Schema schema() {
      return dataSchema;
    }

    private boolean needsSerializer() {
      return useFlatSchema || !fieldPresent(schema(), PAYLOAD_FIELD, FieldType.BYTES);
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin begin) {
          PubsubMessageToRow.Builder builder =
              PubsubMessageToRow.builder()
                  .messageSchema(dataSchema)
                  .useDlq(config.useDeadLetterQueue())
                  .useFlatSchema(useFlatSchema);
          if (needsSerializer()) {
            builder.serializerProvider(config::serializer);
          }
          PCollectionTuple rowsWithDlq =
              begin
                  .apply("ReadFromPubsub", readMessagesWithAttributes())
                  .apply("PubsubMessageToRow", builder.build());
          rowsWithDlq.get(MAIN_TAG).setRowSchema(dataSchema);

          if (config.useDeadLetterQueue()) {
            rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
          }

          return rowsWithDlq.get(MAIN_TAG);
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, POutput> buildWriter() {
      @Nullable
      PayloadSerializer serializer =
          needsSerializer() ? config.serializer(stripFromTimestampField(dataSchema)) : null;

      return new PTransform<PCollection<Row>, POutput>() {
        @Override
        public POutput expand(PCollection<Row> input) {
          PCollection<Row> filtered =
              input.apply(new AddTimestampAttribute(config.useTimestampAttribute()));
          PCollection<PubsubMessage> transformed;
          if (useFlatSchema) {
            transformed =
                filtered.apply(
                    "Transform Flat Schema",
                    MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                        .via(
                            row ->
                                new PubsubMessage(serializer.serialize(row), ImmutableMap.of())));
          } else {
            transformed =
                filtered.apply(
                    "Transform Nested Schema",
                    MapElements.via(new NestedRowToMessage(serializer, filtered.getSchema())));
          }
          return transformed.apply(createPubsubMessageWrite());
        }
      };
    }

    private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
      PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributes().fromTopic(location);

      return config.useTimestampAttribute()
          ? read.withTimestampAttribute(config.getTimestampAttributeKey())
          : read;
    }

    private PubsubIO.Write<PubsubMessage> createPubsubMessageWrite() {
      PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(location);
      if (config.useTimestampAttribute()) {
        write = write.withTimestampAttribute(config.getTimestampAttributeKey());
      }
      return write;
    }

    private PubsubIO.Write<PubsubMessage> writeMessagesToDlq() {
      PubsubIO.Write<PubsubMessage> write =
          PubsubIO.writeMessages().to(config.getDeadLetterQueue());

      return config.useTimestampAttribute()
          ? write.withTimestampAttribute(config.getTimestampAttributeKey())
          : write;
    }

    private boolean hasValidAttributesField(Schema schema) {
      return fieldPresent(schema, ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE)
          || fieldPresent(schema, ATTRIBUTES_FIELD, ATTRIBUTE_ARRAY_FIELD_TYPE);
    }

    private boolean hasValidPayloadField(Schema schema) {
      if (!schema.hasField(PAYLOAD_FIELD)) {
        return false;
      }
      if (fieldPresent(schema, PAYLOAD_FIELD, FieldType.BYTES)) {
        return true;
      }
      return schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(ROW);
    }

    private boolean shouldUseNestedSchema(Schema schema) {
      return hasValidPayloadField(schema) && hasValidAttributesField(schema);
    }

    private static boolean fieldPresent(
        Schema schema, String field, Schema.FieldType expectedType) {
      return schema.hasField(field)
          && expectedType.equivalent(
              schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
    }
  }

  private static Schema stripFromTimestampField(Schema schema) {
    List<Field> selectedFields =
        schema.getFields().stream()
            .filter(field -> !TIMESTAMP_FIELD.equals(field.getName()))
            .collect(toList());
    return Schema.of(selectedFields.toArray(new Schema.Field[0]));
  }

  @AutoValue
  abstract static class Config implements Serializable {

    abstract @Nullable String getTimestampAttributeKey();

    abstract @Nullable String getDeadLetterQueue();

    abstract @Nullable String getFormat();

    // For ThriftPayloadSerializerProvider
    abstract @Nullable String getThriftClass();

    abstract @Nullable String getThriftProtocolFactoryClass();

    // For ProtoPayloadSerializerProvider
    abstract @Nullable String getProtoClass();

    boolean useDeadLetterQueue() {
      return getDeadLetterQueue() != null;
    }

    boolean useTimestampAttribute() {
      return getTimestampAttributeKey() != null;
    }

    PayloadSerializer serializer(Schema schema) {
      String format = getFormat() == null ? "json" : getFormat();
      ImmutableMap.Builder<String, Object> params = ImmutableMap.builder();
      if (getThriftClass() != null) {
        params.put("thriftClass", getThriftClass());
      }
      if (getThriftProtocolFactoryClass() != null) {
        params.put("thriftProtocolFactoryClass", getThriftProtocolFactoryClass());
      }
      if (getProtoClass() != null) {
        params.put("protoClass", getProtoClass());
      }
      return PayloadSerializers.getSerializer(format, schema, params.build());
    }
  }
}
