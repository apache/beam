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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.CloudRegionOrZone;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.gcp.pubsublite.internal.Uuid;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class PubsubLiteReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteReadSchemaTransformProvider.PubsubLiteReadSchemaTransformConfiguration> {

  private static final Logger LOG =
      LoggerFactory.getLogger(PubsubLiteReadSchemaTransformProvider.class);

  public static final String VALID_FORMATS_STR = "RAW,AVRO,JSON,PROTO";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  @Override
  protected Class<PubsubLiteReadSchemaTransformConfiguration> configurationClass() {
    return PubsubLiteReadSchemaTransformConfiguration.class;
  }

  public static class ErrorFn extends DoFn<SequencedMessage, Row> {
    private final SerializableFunction<byte[], Row> valueMapper;
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;

    private final List<String> attributes;

    private final String attributeMap;

    private final Schema errorSchema;

    private final Schema attributeSchema;

    public ErrorFn(
        String name,
        SerializableFunction<byte[], Row> valueMapper,
        Schema errorSchema,
        boolean handleErrors) {
      this.errorCounter = Metrics.counter(PubsubLiteReadSchemaTransformProvider.class, name);
      this.valueMapper = valueMapper;
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
      this.attributes = new ArrayList<>();
      this.attributeMap = "";
      this.attributeSchema = Schema.builder().build();
    }

    public ErrorFn(
        String name,
        SerializableFunction<byte[], Row> valueMapper,
        Schema errorSchema,
        List<String> attributes,
        String attributeMap,
        Schema attributeSchema,
        boolean handleErrors) {
      this.errorCounter = Metrics.counter(PubsubLiteReadSchemaTransformProvider.class, name);
      this.valueMapper = valueMapper;
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
      this.attributes = attributes;
      this.attributeMap = attributeMap;
      this.attributeSchema = attributeSchema;
    }

    @ProcessElement
    public void process(@DoFn.Element SequencedMessage seqMessage, MultiOutputReceiver receiver) {
      Row mappedRow = null;
      try {
        if (attributes.isEmpty()
            && attributeSchema.getFields().isEmpty()
            && attributeMap.isEmpty()) {
          mappedRow = valueMapper.apply(seqMessage.getMessage().getData().toByteArray());
        } else {
          PubSubMessage message = seqMessage.getMessage();
          Row row = valueMapper.apply(message.getData().toByteArray());
          Row.Builder rowBuilder = Row.withSchema(attributeSchema).addValues(row.getValues());
          Map<String, String> stringAttributeMap = new HashMap<>();
          message
              .getAttributesMap()
              .forEach(
                  (attributeName, attributeValues) -> {
                    if (attributes.contains(attributeName)) {
                      processAttribute(attributeValues, rowBuilder::addValue);
                    }

                    if (!attributeMap.isEmpty()) {
                      processAttribute(
                          attributeValues, value -> stringAttributeMap.put(attributeName, value));
                    }
                  });
          if (!attributeMap.isEmpty() && !stringAttributeMap.isEmpty()) {
            rowBuilder.addValue(stringAttributeMap);
          }
          mappedRow = rowBuilder.build();
        }
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        LOG.warn("Error while parsing the element", e);
        receiver
            .get(ERROR_TAG)
            .output(
                ErrorHandling.errorRecord(
                    errorSchema, seqMessage.getMessage().getData().toByteArray(), e));
      }
      if (mappedRow != null) {
        receiver.get(OUTPUT_TAG).output(mappedRow);
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  @Override
  public SchemaTransform from(PubsubLiteReadSchemaTransformConfiguration configuration) {
    if (!VALID_DATA_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          String.format(
              "Format %s not supported. Only supported formats are %s",
              configuration.getFormat(), VALID_FORMATS_STR));
    }
    boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
    String format = configuration.getFormat();
    String inputSchema = configuration.getSchema();
    List<String> attributes = configuration.getAttributes();
    SerializableFunction<byte[], Row> valueMapper;
    Schema beamSchema;

    if (format != null && format.equals("RAW")) {

      beamSchema = Schema.builder().addField("payload", Schema.FieldType.BYTES).build();
      valueMapper = getRawBytesToRowFunction(beamSchema);

    } else if (format != null && format.equals("PROTO")) {
      String fileDescriptorPath = configuration.getFileDescriptorPath();
      String messageName = configuration.getMessageName();

      if (fileDescriptorPath != null && messageName != null) {
        beamSchema = ProtoByteUtils.getBeamSchemaFromProto(fileDescriptorPath, messageName);
        valueMapper = ProtoByteUtils.getProtoBytesToRowFunction(fileDescriptorPath, messageName);
      } else if (inputSchema != null && messageName != null) {
        beamSchema = ProtoByteUtils.getBeamSchemaFromProtoSchema(inputSchema, messageName);
        valueMapper = ProtoByteUtils.getProtoBytesToRowFromSchemaFunction(inputSchema, messageName);
      } else {
        throw new IllegalArgumentException(
            "To read from PubSubLite in PROTO format, either descriptorPath or schema must be provided.");
      }

    } else {
      if (inputSchema != null) {
        beamSchema =
            Objects.equals(configuration.getFormat(), "JSON")
                ? JsonUtils.beamSchemaFromJsonSchema(inputSchema)
                : AvroUtils.toBeamSchema(new org.apache.avro.Schema.Parser().parse(inputSchema));
        valueMapper =
            Objects.equals(configuration.getFormat(), "JSON")
                ? JsonUtils.getJsonBytesToRowFunction(beamSchema)
                : AvroUtils.getAvroBytesToRowFunction(beamSchema);
      } else {
        throw new IllegalArgumentException(
            "To read from Pubsub Lite in JSON or AVRO format, you must provide a schema.");
      }
    }
    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        String project = configuration.getProject();
        if (Strings.isNullOrEmpty(project)) {
          project = input.getPipeline().getOptions().as(GcpOptions.class).getProject();
        }
        if (project == null) {
          throw new IllegalArgumentException(
              "Unable to infer the project to read from Pubsub Lite. Please provide a project.");
        }
        Schema errorSchema = ErrorHandling.errorSchemaBytes();
        List<String> attributeList = new ArrayList<>();
        if (attributes != null) {
          attributeList = attributes;
        }
        String attributeMapValue = configuration.getAttributeMap();
        String attributeMap = attributeMapValue == null ? "" : attributeMapValue;
        Schema resultingBeamSchema =
            buildSchemaWithAttributes(beamSchema, attributeList, attributeMap);
        PCollection<SequencedMessage> readPubsubLite =
            input
                .getPipeline()
                .apply(
                    PubsubLiteIO.read(
                        SubscriberOptions.newBuilder()
                            .setSubscriptionPath(
                                SubscriptionPath.newBuilder()
                                    .setLocation(
                                        CloudRegionOrZone.parse(configuration.getLocation()))
                                    .setProject(ProjectId.of(project))
                                    .setName(
                                        SubscriptionName.of(configuration.getSubscriptionName()))
                                    .build())
                            .build()));

        String attributeId = configuration.getAttributeId();
        PCollectionTuple outputTuple;
        PCollection<SequencedMessage> transformSequencedMessage;
        if (attributeId != null && !attributeId.isEmpty()) {
          UuidDeduplicationOptions.Builder uuidExtractor =
              UuidDeduplicationOptions.newBuilder()
                  .setUuidExtractor(getUuidFromMessage(attributeId));
          transformSequencedMessage =
              readPubsubLite.apply(PubsubLiteIO.deduplicate(uuidExtractor.build()));
        } else {
          transformSequencedMessage = readPubsubLite;
        }

        outputTuple =
            transformSequencedMessage.apply(
                ParDo.of(
                        new ErrorFn(
                            "PubsubLite-read-error-counter",
                            valueMapper,
                            errorSchema,
                            attributeList,
                            attributeMap,
                            resultingBeamSchema,
                            handleErrors))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));
        return PCollectionRowTuple.of(
            "output",
            outputTuple.get(OUTPUT_TAG).setRowSchema(resultingBeamSchema),
            "errors",
            outputTuple.get(ERROR_TAG).setRowSchema(errorSchema));
      }
    };
  }

  /**
   * Builds a new {@link Schema} by adding additional optional attributes and map field to the
   * provided schema.
   *
   * @param schema The base schema to which additional attributes and map field will be added.
   * @param attributes A list of optional attribute names to be added as STRING fields to the
   *     schema.
   * @param attributesMap The name of the optional map field to be added to the schema. If empty, no
   *     map field will be added.
   * @return A new {@link Schema} with the specified attributes and an optional map field.
   * @throws IllegalArgumentException if the schema is null or if any attribute name in the
   *     attributes list is null or empty.
   */
  public static Schema buildSchemaWithAttributes(
      Schema schema, List<String> attributes, String attributesMap) {
    Schema.Builder schemaBuilder = Schema.builder();
    // Copy fields from the original schema
    schema.getFields().forEach(field -> schemaBuilder.addField(field.getName(), field.getType()));

    // Add optional additional attributes as STRING fields
    attributes.forEach(
        attribute -> {
          if (attribute == null || attribute.isEmpty()) {
            throw new IllegalArgumentException(
                "Attribute names in the attributes list must not be null or empty.");
          }
          schemaBuilder.addField(attribute, Schema.FieldType.STRING);
        });

    // Add an optional map field if attributesMap is not empty
    if (!attributesMap.isEmpty()) {
      schemaBuilder
          .addMapField(attributesMap, Schema.FieldType.STRING, Schema.FieldType.STRING)
          .build();
    }
    return schemaBuilder.build();
  }

  /**
   * Processes the attribute values, invoking the specified consumer with the processed value. If
   * the attribute values are null or contain multiple values, an exception is thrown.
   *
   * @param attributeValues The attribute values to be processed. If null, the method does nothing.
   * @param valueConsumer The consumer to accept the processed value.
   * @throws RuntimeException if attributeValues is not null and contains multiple values.
   */
  private static void processAttribute(
      @Nullable AttributeValues attributeValues, Consumer<String> valueConsumer) {
    if (attributeValues != null) {
      List<ByteString> valueList = attributeValues.getValuesList();
      if (valueList.size() != 1) {
        throw new RuntimeException(
            "Received an unparseable message with multiple values for an attribute.");
      }
      valueConsumer.accept(valueList.get(0).toStringUtf8());
    }
  }

  public static SerializableFunction<byte[], Row> getRawBytesToRowFunction(Schema rawSchema) {
    return new SimpleFunction<byte[], Row>() {
      @Override
      public Row apply(byte[] input) {
        return Row.withSchema(rawSchema).addValue(input).build();
      }
    };
  }

  public static SerializableFunction<SequencedMessage, Uuid> getUuidFromMessage(
      String attributeId) {
    return new SimpleFunction<SequencedMessage, Uuid>() {
      @Override
      public Uuid apply(SequencedMessage input) {
        AttributeValues attribute = input.getMessage().getAttributesMap().get(attributeId);
        if (attribute != null) {
          if (attribute.getValuesCount() != 1) {
            throw new RuntimeException(
                "Received an unparseable message with multiple values for an attribute.");
          }
          return Uuid.of(attribute.getValues(0));
        } else {
          throw new RuntimeException("Uuid attribute missing.");
        }
      }
    };
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:pubsublite_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList("output", "errors");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteReadSchemaTransformConfiguration {

    public void validate() {
      final String dataFormat = this.getFormat();
      assert dataFormat == null || VALID_DATA_FORMATS.contains(dataFormat)
          : "Valid data formats are " + VALID_DATA_FORMATS;

      final String inputSchema = this.getSchema();
      final String messageName = this.getMessageName();

      if (dataFormat != null && dataFormat.equals("RAW")) {
        assert inputSchema == null
            : "To read from Pubsub Lite in RAW format, you can't provide a schema.";
      }

      if (dataFormat != null && dataFormat.equals("PROTO")) {
        assert messageName != null
            : "To read from Pubsub Lite in PROTO format, messageName must be provided.";
      }
    }

    @SchemaFieldDescription(
        "The encoding format for the data stored in Pubsub Lite. Valid options are: "
            + VALID_FORMATS_STR)
    public abstract String getFormat();

    @SchemaFieldDescription(
        "The schema in which the data is encoded in the Pubsub Lite topic. "
            + "For AVRO data, this is a schema defined with AVRO schema syntax "
            + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas). "
            + "For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/).")
    public abstract @Nullable String getSchema();

    @SchemaFieldDescription(
        "The GCP project where the Pubsub Lite reservation resides. This can be a "
            + "project number of a project ID.")
    public abstract @Nullable String getProject();

    @SchemaFieldDescription(
        "The name of the subscription to consume data. This will be concatenated with "
            + "the project and location parameters to build a full subscription path.")
    public abstract String getSubscriptionName();

    @SchemaFieldDescription("The region or zone where the Pubsub Lite reservation resides.")
    public abstract String getLocation();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    public abstract @Nullable ErrorHandling getErrorHandling();

    @SchemaFieldDescription(
        "List of attribute keys whose values will be flattened into the "
            + "output message as additional fields.  For example, if the format is `RAW` "
            + "and attributes is `[\"a\", \"b\"]` then this read will produce elements of "
            + "the form `Row(payload=..., a=..., b=...)`")
    public abstract @Nullable List<String> getAttributes();

    @SchemaFieldDescription(
        "Name of a field in which to store the full set of attributes "
            + "associated with this message.  For example, if the format is `RAW` and "
            + "`attribute_map` is set to `\"attrs\"` then this read will produce elements "
            + "of the form `Row(payload=..., attrs=...)` where `attrs` is a Map type "
            + "of string to string. "
            + "If both `attributes` and `attribute_map` are set, the overlapping "
            + "attribute values will be present in both the flattened structure and the "
            + "attribute map.")
    public abstract @Nullable String getAttributeMap();

    @SchemaFieldDescription(
        "The attribute on incoming Pubsub Lite messages to use as a unique "
            + "record identifier. When specified, the value of this attribute (which "
            + "can be any string that uniquely identifies the record) will be used for "
            + "deduplication of messages. If not provided, we cannot guarantee "
            + "that no duplicate data will be delivered on the Pub/Sub stream. In this "
            + "case, deduplication of the stream will be strictly best effort.")
    public abstract @Nullable String getAttributeId();

    @SchemaFieldDescription(
        "The path to the Protocol Buffer File Descriptor Set file. This file is used for schema"
            + " definition and message serialization.")
    @Nullable
    public abstract String getFileDescriptorPath();

    @SchemaFieldDescription(
        "The name of the Protocol Buffer message to be used for schema"
            + " extraction and data conversion.")
    @Nullable
    public abstract String getMessageName();

    public static Builder builder() {
      return new AutoValue_PubsubLiteReadSchemaTransformProvider_PubsubLiteReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFormat(String format);

      public abstract Builder setSchema(String schema);

      public abstract Builder setProject(String project);

      public abstract Builder setSubscriptionName(String subscriptionName);

      public abstract Builder setLocation(String location);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setAttributes(List<String> attributes);

      @SuppressWarnings("unused")
      public abstract Builder setAttributeMap(String attributeMap);

      @SuppressWarnings("unused")
      public abstract Builder setAttributeId(String attributeId);

      @SuppressWarnings("unused")
      public abstract Builder setFileDescriptorPath(String fileDescriptorPath);

      @SuppressWarnings("unused")
      public abstract Builder setMessageName(String messageName);

      public abstract PubsubLiteReadSchemaTransformConfiguration build();
    }
  }
}
