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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.ATTRIBUTES_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_ATTRIBUTES_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_EVENT_TIMESTAMP_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_PAYLOAD_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.ERROR;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.OUTPUT;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.PAYLOAD_BYTES_TYPE_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.PAYLOAD_ROW_TYPE_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.removeFields;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SchemaPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.FieldMatcher;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.SchemaReflection;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubWriteSchemaTransformConfiguration.SourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for Pub/Sub reads configured using
 * {@link PubsubWriteSchemaTransformConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
@AutoService(SchemaTransformProvider.class)
public class PubsubWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<PubsubWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:pubsub_write:v1";
  static final String INPUT_TAG = "input";
  static final String ERROR_TAG = "error";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<PubsubWriteSchemaTransformConfiguration> configurationClass() {
    return PubsubWriteSchemaTransformConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  public SchemaTransform from(PubsubWriteSchemaTransformConfiguration configuration) {
    return new PubsubWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link SchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since a
   * single input is expected, this returns a list with a single name.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. The
   * only expected output is the {@link #ERROR_TAG}.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(ERROR_TAG);
  }

  /**
   * An implementation of {@link SchemaTransform} for Pub/Sub writes configured using {@link
   * PubsubWriteSchemaTransformConfiguration}.
   */
  static class PubsubWriteSchemaTransform extends SchemaTransform {

    private final PubsubWriteSchemaTransformConfiguration configuration;

    private PubsubClient.PubsubClientFactory pubsubClientFactory;

    PubsubWriteSchemaTransform(PubsubWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    PubsubWriteSchemaTransform withPubsubClientFactory(PubsubClient.PubsubClientFactory factory) {
      this.pubsubClientFactory = factory;
      return this;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (input.getAll().size() != 1 || !input.has(INPUT_TAG)) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s input is expected to contain a single %s tagged PCollection<Row>",
                input.getClass().getSimpleName(), getClass().getSimpleName(), INPUT_TAG));
      }

      PCollection<Row> rows = input.get(INPUT_TAG);
      if (rows.getSchema().getFieldCount() == 0) {
        throw new IllegalArgumentException(String.format("empty Schema for %s", INPUT_TAG));
      }

      Schema targetSchema = buildTargetSchema(rows.getSchema());

      rows =
          rows.apply(
                  ConvertForRowToMessage.class.getSimpleName(),
                  convertForRowToMessage(targetSchema))
              .setRowSchema(targetSchema);

      Schema schema = rows.getSchema();

      Schema serializableSchema =
          removeFields(schema, DEFAULT_ATTRIBUTES_KEY_NAME, DEFAULT_EVENT_TIMESTAMP_KEY_NAME);
      FieldMatcher payloadRowMatcher = FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME, TypeName.ROW);
      if (payloadRowMatcher.match(serializableSchema)) {
        serializableSchema =
            serializableSchema.getField(DEFAULT_PAYLOAD_KEY_NAME).getType().getRowSchema();
      }

      validateTargetSchemaAgainstPubsubSchema(serializableSchema, input.getPipeline().getOptions());

      PCollectionTuple pct =
          rows.apply(
              PubsubRowToMessage.class.getSimpleName(),
              buildPubsubRowToMessage(serializableSchema));

      PCollection<PubsubMessage> messages = pct.get(OUTPUT);
      messages.apply(PubsubIO.Write.class.getSimpleName(), buildPubsubWrite());
      return PCollectionRowTuple.of(ERROR_TAG, pct.get(ERROR));
    }

    PayloadSerializer getPayloadSerializer(Schema schema) {
      if (configuration.getFormat() == null) {
        return null;
      }
      String format = configuration.getFormat();
      Set<String> availableFormats =
          Providers.loadProviders(PayloadSerializerProvider.class).keySet();
      if (!availableFormats.contains(format)) {
        String availableFormatsString = String.join(",", availableFormats);
        throw new IllegalArgumentException(
            String.format(
                "%s is not among the valid formats: [%s]", format, availableFormatsString));
      }
      return PayloadSerializers.getSerializer(configuration.getFormat(), schema, ImmutableMap.of());
    }

    PubsubRowToMessage buildPubsubRowToMessage(Schema schema) {
      PubsubRowToMessage.Builder builder =
          PubsubRowToMessage.builder().setPayloadSerializer(getPayloadSerializer(schema));

      if (configuration.getTarget() != null) {
        builder =
            builder.setTargetTimestampAttributeName(
                configuration.getTarget().getTimestampAttributeKey());
      }

      return builder.build();
    }

    PubsubIO.Write<PubsubMessage> buildPubsubWrite() {
      PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages().to(configuration.getTopic());

      if (configuration.getIdAttribute() != null) {
        write = write.withIdAttribute(configuration.getIdAttribute());
      }

      if (pubsubClientFactory != null) {
        write = write.withClientFactory(pubsubClientFactory);
      }

      return write;
    }

    void validateSourceSchemaAgainstConfiguration(Schema sourceSchema) {
      if (sourceSchema.getFieldCount() == 0) {
        throw new IllegalArgumentException(String.format("empty Schema for %s", INPUT_TAG));
      }

      if (configuration.getSource() == null) {
        return;
      }

      SourceConfiguration source = configuration.getSource();

      if (source.getAttributesFieldName() != null) {
        String fieldName = source.getAttributesFieldName();
        FieldType fieldType = ATTRIBUTES_FIELD_TYPE;
        FieldMatcher fieldMatcher = FieldMatcher.of(fieldName, fieldType);
        checkArgument(
            fieldMatcher.match(sourceSchema),
            String.format("schema missing field: %s for type %s: ", fieldName, fieldType));
      }

      if (source.getTimestampFieldName() != null) {
        String fieldName = source.getTimestampFieldName();
        FieldType fieldType = EVENT_TIMESTAMP_FIELD_TYPE;
        FieldMatcher fieldMatcher = FieldMatcher.of(fieldName, fieldType);
        checkArgument(
            fieldMatcher.match(sourceSchema),
            String.format("schema missing field: %s for type: %s", fieldName, fieldType));
      }

      if (source.getPayloadFieldName() == null) {
        return;
      }

      String fieldName = source.getPayloadFieldName();
      FieldMatcher bytesFieldMatcher = FieldMatcher.of(fieldName, PAYLOAD_BYTES_TYPE_NAME);
      FieldMatcher rowFieldMatcher = FieldMatcher.of(fieldName, PAYLOAD_ROW_TYPE_NAME);
      SchemaReflection schemaReflection = SchemaReflection.of(sourceSchema);
      checkArgument(
          schemaReflection.matchesAny(bytesFieldMatcher, rowFieldMatcher),
          String.format(
              "schema missing field: %s for types %s or %s",
              fieldName, PAYLOAD_BYTES_TYPE_NAME, PAYLOAD_ROW_TYPE_NAME));

      String[] fieldsToExclude =
          Stream.of(
                  source.getAttributesFieldName(),
                  source.getTimestampFieldName(),
                  source.getPayloadFieldName())
              .filter(Objects::nonNull)
              .toArray(String[]::new);

      Schema userFieldsSchema = removeFields(sourceSchema, fieldsToExclude);

      if (userFieldsSchema.getFieldCount() > 0) {
        throw new IllegalArgumentException(
            String.format("user fields incompatible with %s field", source.getPayloadFieldName()));
      }
    }

    void validateTargetSchemaAgainstPubsubSchema(Schema targetSchema, PipelineOptions options) {
      checkArgument(options != null);

      try (PubsubClient pubsubClient = getPubsubClient(options.as(PubsubOptions.class))) {
        PubsubClient.TopicPath topicPath = PubsubClient.topicPathFromPath(configuration.getTopic());
        PubsubClient.SchemaPath schemaPath = pubsubClient.getSchemaPath(topicPath);
        if (schemaPath == null || schemaPath.equals(SchemaPath.DELETED_SCHEMA)) {
          return;
        }
        Schema expectedSchema = pubsubClient.getSchema(schemaPath);
        checkState(
            targetSchema.equals(expectedSchema),
            String.format(
                "input schema mismatch with expected schema at path: %s\ninput schema: %s\nPub/Sub schema: %s",
                schemaPath, targetSchema, expectedSchema));
      } catch (IOException e) {
        throw new IllegalStateException(e.getMessage());
      }
    }

    Schema buildTargetSchema(Schema sourceSchema) {
      validateSourceSchemaAgainstConfiguration(sourceSchema);
      FieldType payloadFieldType = null;

      List<String> fieldsToRemove = new ArrayList<>();

      if (configuration.getSource() != null) {
        SourceConfiguration source = configuration.getSource();

        if (source.getAttributesFieldName() != null) {
          fieldsToRemove.add(source.getAttributesFieldName());
        }

        if (source.getTimestampFieldName() != null) {
          fieldsToRemove.add(source.getTimestampFieldName());
        }

        if (source.getPayloadFieldName() != null) {
          String fieldName = source.getPayloadFieldName();
          Field field = sourceSchema.getField(fieldName);
          payloadFieldType = field.getType();
          fieldsToRemove.add(fieldName);
        }
      }

      Schema targetSchema =
          PubsubRowToMessage.builder()
              .build()
              .inputSchemaFactory(payloadFieldType)
              .buildSchema(sourceSchema.getFields().toArray(new Field[0]));

      return removeFields(targetSchema, fieldsToRemove.toArray(new String[0]));
    }

    private PubsubClient.PubsubClientFactory getPubsubClientFactory() {
      if (pubsubClientFactory != null) {
        return pubsubClientFactory;
      }
      return PubsubGrpcClient.FACTORY;
    }

    private PubsubClient getPubsubClient(PubsubOptions options) throws IOException {
      return getPubsubClientFactory()
          .newClient(
              configuration.getTarget().getTimestampAttributeKey(),
              configuration.getIdAttribute(),
              options);
    }

    ParDo.SingleOutput<Row, Row> convertForRowToMessage(Schema targetSchema) {
      return convertForRowToMessage(targetSchema, null);
    }

    ParDo.SingleOutput<Row, Row> convertForRowToMessage(
        Schema targetSchema, @Nullable Clock clock) {
      String attributesName = null;
      String timestampName = null;
      String payloadName = null;
      SourceConfiguration source = configuration.getSource();
      if (source != null) {
        attributesName = source.getAttributesFieldName();
        timestampName = source.getTimestampFieldName();
        payloadName = source.getPayloadFieldName();
      }
      return ParDo.of(
          new ConvertForRowToMessage(
              targetSchema, clock, attributesName, timestampName, payloadName));
    }
  }

  private static class ConvertForRowToMessage extends DoFn<Row, Row> {
    private final Schema targetSchema;
    @Nullable private final Clock clock;
    @Nullable private final String attributesFieldName;
    @Nullable private final String timestampFieldName;
    @Nullable private final String payloadFieldName;

    ConvertForRowToMessage(
        Schema targetSchema,
        @Nullable Clock clock,
        @Nullable String attributesFieldName,
        @Nullable String timestampFieldName,
        @Nullable String payloadFieldName) {
      this.targetSchema = targetSchema;
      this.clock = clock;
      this.attributesFieldName = attributesFieldName;
      this.timestampFieldName = timestampFieldName;
      this.payloadFieldName = payloadFieldName;
    }

    @ProcessElement
    public void process(@Element Row row, OutputReceiver<Row> receiver) {
      Instant now = Instant.now();
      if (clock != null) {
        now = Instant.ofEpochMilli(clock.currentTimeMillis());
      }
      Map<String, Object> values = new HashMap<>();

      // Default attributes value
      checkState(targetSchema.hasField(DEFAULT_ATTRIBUTES_KEY_NAME));
      values.put(DEFAULT_ATTRIBUTES_KEY_NAME, ImmutableMap.of());

      // Default timestamp value
      checkState(targetSchema.hasField(DEFAULT_EVENT_TIMESTAMP_KEY_NAME));
      values.put(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, now);

      for (String fieldName : row.getSchema().getFieldNames()) {
        if (targetSchema.hasField(fieldName)) {
          values.put(fieldName, row.getValue(fieldName));
        }

        if (attributesFieldName != null) {
          values.put(DEFAULT_ATTRIBUTES_KEY_NAME, row.getValue(attributesFieldName));
        }
        if (timestampFieldName != null) {
          values.put(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, row.getValue(timestampFieldName));
        }
        if (payloadFieldName != null) {
          values.put(DEFAULT_PAYLOAD_KEY_NAME, row.getValue(payloadFieldName));
        }
      }
      receiver.output(Row.withSchema(targetSchema).withFieldValues(values).build());
    }
  }
}
