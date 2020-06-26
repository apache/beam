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

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.schemas.io.SchemaCapableIOProvider;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.schemas.transforms.DropFields;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.MAIN_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.TIMESTAMP_FIELD;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

/**
 * {@link org.apache.beam.sdk.schemas.io.SchemaCapableIOProvider} to create {@link PubsubSchemaIO}
 * that implements {@link org.apache.beam.sdk.schemas.io.SchemaIO}.
 */
@Internal
@AutoService(SchemaCapableIOProvider.class)
public class PubsubSchemaCapableIOProvider implements SchemaCapableIOProvider {
  public static final FieldType VARCHAR = FieldType.STRING;
  public static final FieldType TIMESTAMP = FieldType.DATETIME;

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
        .build();
  }

  /**
   * Produce a SchemaIO given a String representing the data's location, the schema of the data that
   * resides there, and some IO-specific configuration object.
   */
  @Override
  public PubsubSchemaIO from(String location, Row configuration, Schema dataSchema) {
    validateDlq(configuration.getValue("deadLetterQueue"));
    validateEventTimestamp(dataSchema);
    return PubsubSchemaIO.withConfiguration(location,configuration,dataSchema);
  }

  private void validateEventTimestamp(Schema schema) {
    if (!PubsubSchemaIO.fieldPresent(schema, TIMESTAMP_FIELD, TIMESTAMP)) {
      throw new InvalidConfigurationException(
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

  /** An abstraction to create schema aware IOs. */
  @Internal
  private static class PubsubSchemaIO implements SchemaIO, Serializable {
    protected final Row config;
    protected final Schema dataSchema;
    protected final String location;
    protected final Boolean useFlatSchema;

    private PubsubSchemaIO(String location, Row config, Schema dataSchema) {
      this.config = config;
      this.dataSchema = dataSchema;
      this.location = location;
      this.useFlatSchema = !definesAttributeAndPayload(dataSchema);
    }

    static PubsubSchemaIO withConfiguration(String location, Row config, Schema dataSchema) {
      return new PubsubSchemaIO(location, config, dataSchema);
    }

    @Override
    public Schema schema() {
      return dataSchema;
    }

    @Override
    public PTransform<PBegin, PCollection<Row>> buildReader() {
      return new PTransform<PBegin, PCollection<Row>>() {
        @Override
        public PCollection<Row> expand(PBegin begin) {
          PCollectionTuple rowsWithDlq =
                  begin
                          .apply("ReadFromPubsub", readMessagesWithAttributes())
                          .apply(
                                  "PubsubMessageToRow",
                                  PubsubMessageToRow.builder()
                                          .messageSchema(dataSchema)
                                          .useDlq(useDlqCheck(config))
                                          .useFlatSchema(useFlatSchema)
                                          .build());
          rowsWithDlq.get(MAIN_TAG).setRowSchema(dataSchema);

          if (useDlqCheck(config)) {
            rowsWithDlq.get(DLQ_TAG).apply(writeMessagesToDlq());
          }

          return rowsWithDlq.get(MAIN_TAG);
        }
      };
    }

    @Override
    public PTransform<PCollection<Row>, POutput> buildWriter() {
      if (!useFlatSchema) {
        throw new UnsupportedOperationException(
                "Writing to a Pubsub topic is only supported for flattened schemas");
      }

      return new PTransform<PCollection<Row>, POutput>() {
        @Override
        public POutput expand(PCollection<Row> input) {
          return input
                  .apply(RowToPubsubMessage.fromTableConfig(config, useFlatSchema))
                  .apply(createPubsubMessageWrite());
        }
      };
    }

    private PubsubIO.Read<PubsubMessage> readMessagesWithAttributes() {
      PubsubIO.Read<PubsubMessage> read = PubsubIO.readMessagesWithAttributes().fromTopic(location);

      return useTimestampAttribute(config)
              ? read.withTimestampAttribute(config.getValue("timestampAttributeKey"))
              : read;
    }

    private PubsubIO.Write<PubsubMessage> createPubsubMessageWrite() {
      PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(location);
      if (useTimestampAttribute(config)) {
        write = write.withTimestampAttribute(config.getValue("timestampAttributeKey"));
      }
      return write;
    }

    private PubsubIO.Write<PubsubMessage> writeMessagesToDlq() {
      PubsubIO.Write<PubsubMessage> write =
              PubsubIO.writeMessages().to(config.getString("deadLetterQueue"));

      return useTimestampAttribute(config)
              ? write.withTimestampAttribute(config.getString("timestampAttributeKey"))
              : write;
    }

    private boolean useDlqCheck(Row config) {
      return config.getValue("deadLetterQueue") != null;
    }

    private boolean useTimestampAttribute(Row config) {
      return config.getValue("timestampAttributeKey") != null;
    }

    private boolean definesAttributeAndPayload(Schema schema) {
      return fieldPresent(
              schema, ATTRIBUTES_FIELD, Schema.FieldType.map(VARCHAR.withNullable(false), VARCHAR))
              && (schema.hasField(PAYLOAD_FIELD)
              && ROW.equals(schema.getField(PAYLOAD_FIELD).getType().getTypeName()));
    }

    private static boolean fieldPresent(Schema schema, String field, Schema.FieldType expectedType) {
      return schema.hasField(field)
              && expectedType.equivalent(
              schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
    }

  }

    /**
     * A {@link PTransform} to convert {@link Row} to {@link PubsubMessage} with JSON payload.
     *
     * <p>Currently only supports writing a flat schema into a JSON payload. This means that all Row
     * field values are written to the {@link PubsubMessage} JSON payload, except for {@code
     * event_timestamp}, which is either ignored or written to the message attributes, depending on
     * whether config.getValue("timestampAttributeKey") is set.
     */
    private static class RowToPubsubMessage extends PTransform<PCollection<Row>, PCollection<PubsubMessage>> {
      private final Row config;

      private RowToPubsubMessage(Row config, Boolean useFlatSchema) {
        checkArgument(
                useFlatSchema,
                "RowToPubsubMessage is only supported for flattened schemas.");

        this.config = config;
      }

      public static RowToPubsubMessage fromTableConfig(Row config, Boolean useFlatSchema) {
        return new RowToPubsubMessage(config, useFlatSchema);
      }

      @Override
      public PCollection<PubsubMessage> expand(PCollection<Row> input) {
        PCollection<Row> withTimestamp =
                (useTimestampAttribute(config))
                        ? input.apply(
                        WithTimestamps.of((row) -> row.getDateTime("event_timestamp").toInstant()))
                        : input;

        return withTimestamp
                .apply(DropFields.fields("event_timestamp"))
                .apply(ToJson.of())
                .apply(
                        MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                                .via(
                                        (String json) ->
                                                new PubsubMessage(
                                                        json.getBytes(StandardCharsets.ISO_8859_1), ImmutableMap.of())));
      }

      private boolean useTimestampAttribute(Row config) {
        return config.getValue("timestampAttributeKey") != null;
      }
    }

  private static boolean definesAttributeAndPayload(Schema schema) {
    return fieldPresent(
            schema, ATTRIBUTES_FIELD, Schema.FieldType.map(VARCHAR.withNullable(false), VARCHAR))
            && (schema.hasField(PAYLOAD_FIELD)
            && ROW.equals(schema.getField(PAYLOAD_FIELD).getType().getTypeName()));
  }

  private static boolean fieldPresent(Schema schema, String field, Schema.FieldType expectedType) {
    return schema.hasField(field)
            && expectedType.equivalent(
            schema.getField(field).getType(), Schema.EquivalenceNullablePolicy.IGNORE);
  }
}
