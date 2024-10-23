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
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
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
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class PubsubLiteWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        PubsubLiteWriteSchemaTransformProvider.PubsubLiteWriteSchemaTransformConfiguration> {

  public static final String SUPPORTED_FORMATS_STR = "RAW,JSON,AVRO,PROTO";
  public static final Set<String> SUPPORTED_FORMATS =
      Sets.newHashSet(SUPPORTED_FORMATS_STR.split(","));
  public static final TupleTag<PubSubMessage> OUTPUT_TAG = new TupleTag<PubSubMessage>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(PubsubLiteWriteSchemaTransformProvider.class);

  @Override
  protected Class<PubsubLiteWriteSchemaTransformConfiguration> configurationClass() {
    return PubsubLiteWriteSchemaTransformConfiguration.class;
  }

  public static class ErrorCounterFn extends DoFn<Row, PubSubMessage> {
    private final SerializableFunction<Row, byte[]> toBytesFn;
    private final Counter errorCounter;
    private long errorsInBundle = 0L;

    private final Schema errorSchema;

    private final boolean handleErrors;

    private final List<String> attributes;

    private final Schema schemaWithoutAttributes;

    public ErrorCounterFn(
        String name,
        SerializableFunction<Row, byte[]> toBytesFn,
        Schema errorSchema,
        boolean handleErrors) {
      this.toBytesFn = toBytesFn;
      errorCounter = Metrics.counter(PubsubLiteWriteSchemaTransformProvider.class, name);
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
      this.attributes = new ArrayList<>();
      this.schemaWithoutAttributes = Schema.builder().build();
    }

    public ErrorCounterFn(
        String name,
        SerializableFunction<Row, byte[]> toBytesFn,
        Schema errorSchema,
        boolean handleErrors,
        List<String> attributes,
        Schema schemaWithoutAttributes) {
      this.toBytesFn = toBytesFn;
      errorCounter = Metrics.counter(PubsubLiteWriteSchemaTransformProvider.class, name);
      this.errorSchema = errorSchema;
      this.handleErrors = handleErrors;
      this.attributes = attributes;
      this.schemaWithoutAttributes = schemaWithoutAttributes;
    }

    @ProcessElement
    public void process(@DoFn.Element Row row, MultiOutputReceiver receiver) {
      try {
        PubSubMessage message;
        if (attributes.isEmpty()) {
          message =
              PubSubMessage.newBuilder()
                  .setData(ByteString.copyFrom(Objects.requireNonNull(toBytesFn.apply(row))))
                  .build();
        } else {
          Row.Builder builder = Row.withSchema(schemaWithoutAttributes);
          schemaWithoutAttributes
              .getFields()
              .forEach(field -> builder.addValue(row.getValue(field.getName())));

          Row resultingRow = builder.build();
          Map<String, AttributeValues> attributeValuesHashMap =
              getStringAttributeValuesMap(row, attributes);
          message =
              PubSubMessage.newBuilder()
                  .setData(
                      ByteString.copyFrom(Objects.requireNonNull(toBytesFn.apply(resultingRow))))
                  .putAllAttributes(attributeValuesHashMap)
                  .build();
        }

        receiver.get(OUTPUT_TAG).output(message);
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        LOG.warn("Error while processing the element", e);
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, row, e));
      }
    }

    @FinishBundle
    public void finish() {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  @Override
  public SchemaTransform from(PubsubLiteWriteSchemaTransformConfiguration configuration) {

    if (!SUPPORTED_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          "Format "
              + configuration.getFormat()
              + " is not supported. "
              + "Supported formats are: "
              + String.join(", ", SUPPORTED_FORMATS));
    }

    return new SchemaTransform() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        List<String> attributesConfigValue = configuration.getAttributes();
        String attributeId = configuration.getAttributeId();
        List<String> attributes =
            attributesConfigValue != null ? attributesConfigValue : new ArrayList<>();
        Schema inputSchema;
        if (!attributes.isEmpty()) {
          inputSchema = getSchemaWithoutAttributes(input.get("input").getSchema(), attributes);
        } else {
          inputSchema = input.get("input").getSchema();
        }
        ErrorHandling errorHandling = configuration.getErrorHandling();
        boolean handleErrors = ErrorHandling.hasOutput(errorHandling);
        Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

        final SerializableFunction<Row, byte[]> toBytesFn;
        if (configuration.getFormat().equals("RAW")) {
          int numFields = inputSchema.getFields().size();
          if (numFields != 1) {
            throw new IllegalArgumentException("Expecting exactly one field, found " + numFields);
          }
          if (!inputSchema.getField(0).getType().equals(Schema.FieldType.BYTES)) {
            throw new IllegalArgumentException(
                "The input schema must have exactly one field of type byte.");
          }
          toBytesFn = getRowToRawBytesFunction(inputSchema.getField(0).getName());
        } else if (configuration.getFormat().equals("PROTO")) {
          String descriptorPath = configuration.getFileDescriptorPath();
          String schema = configuration.getSchema();
          String messageName = configuration.getMessageName();

          if (descriptorPath != null && messageName != null) {
            toBytesFn = ProtoByteUtils.getRowToProtoBytes(descriptorPath, messageName);
          } else if (schema != null && messageName != null) {
            toBytesFn = ProtoByteUtils.getRowToProtoBytesFromSchema(schema, messageName);
          } else {
            throw new IllegalArgumentException(
                "At least a descriptorPath or a PROTO schema is required.");
          }
        } else if (configuration.getFormat().equals("JSON")) {
          toBytesFn = JsonUtils.getRowToJsonBytesFunction(inputSchema);
        } else {
          toBytesFn = AvroUtils.getRowToAvroBytesFunction(inputSchema);
        }

        PCollectionTuple outputTuple =
            input
                .get("input")
                .apply(
                    "Map Rows to PubSubMessages",
                    ParDo.of(
                            new ErrorCounterFn(
                                "PubSubLite-write-error-counter",
                                toBytesFn,
                                errorSchema,
                                handleErrors,
                                attributes,
                                inputSchema))
                        .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

        outputTuple
            .get(OUTPUT_TAG)
            .apply(
                "Add UUIDs",
                (attributeId != null && !attributeId.isEmpty())
                    ? new SetUuidFromPubSubMessage(attributeId)
                    : PubsubLiteIO.addUuids())
            .apply(
                "Write to PS Lite",
                PubsubLiteIO.write(
                    PublisherOptions.newBuilder()
                        .setTopicPath(
                            TopicPath.newBuilder()
                                .setProject(ProjectId.of(configuration.getProject()))
                                .setName(TopicName.of(configuration.getTopicName()))
                                .setLocation(CloudRegionOrZone.parse(configuration.getLocation()))
                                .build())
                        .build()));

        PCollection<Row> errorOutput =
            outputTuple.get(ERROR_TAG).setRowSchema(ErrorHandling.errorSchema(errorSchema));

        String outputString = errorHandling != null ? errorHandling.getOutput() : "errors";
        return PCollectionRowTuple.of(handleErrors ? outputString : "errors", errorOutput);
      }
    };
  }

  public static Schema getSchemaWithoutAttributes(Schema inputSchema, List<String> attributes) {
    Schema.Builder schemaBuilder = Schema.builder();

    inputSchema
        .getFields()
        .forEach(
            field -> {
              if (!attributes.contains(field.getName())) {
                schemaBuilder.addField(field.getName(), field.getType());
              }
            });
    return schemaBuilder.build();
  }

  private static Map<String, AttributeValues> getStringAttributeValuesMap(
      Row row, List<String> attributes) {
    Map<String, AttributeValues> attributeValuesHashMap = new HashMap<>();
    attributes.forEach(
        attribute -> {
          String value = row.getValue(attribute);
          if (value != null) {
            attributeValuesHashMap.put(
                attribute,
                AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8(value)).build());
          }
        });
    return attributeValuesHashMap;
  }

  public static SerializableFunction<Row, byte[]> getRowToRawBytesFunction(String rowFieldName) {
    return new SimpleFunction<Row, byte[]>() {
      @Override
      public byte[] apply(Row input) {
        byte[] rawBytes = input.getBytes(rowFieldName);
        if (rawBytes == null) {
          throw new NullPointerException();
        }
        return rawBytes;
      }
    };
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:pubsublite_write:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList("errors");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PubsubLiteWriteSchemaTransformConfiguration {

    public void validate() {
      final String dataFormat = this.getFormat();
      final String inputSchema = this.getSchema();
      final String messageName = this.getMessageName();
      final String descriptorPath = this.getFileDescriptorPath();

      if (dataFormat != null && dataFormat.equals("PROTO")) {
        assert messageName != null : "Expecting messageName to be non-null.";
        assert descriptorPath != null && inputSchema != null
            : "You must include a descriptorPath or a PROTO schema but not both.";
      }
    }

    @SchemaFieldDescription(
        "The GCP project where the Pubsub Lite reservation resides. This can be a "
            + "project number of a project ID.")
    public abstract String getProject();

    @SchemaFieldDescription("The region or zone where the Pubsub Lite reservation resides.")
    public abstract String getLocation();

    @SchemaFieldDescription(
        "The name of the topic to publish data into. This will be concatenated with "
            + "the project and location parameters to build a full topic path.")
    public abstract String getTopicName();

    @SchemaFieldDescription(
        "The encoding format for the data stored in Pubsub Lite. Valid options are: "
            + SUPPORTED_FORMATS_STR)
    public abstract String getFormat();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    public abstract @Nullable ErrorHandling getErrorHandling();

    @SchemaFieldDescription(
        "List of attribute keys whose values will be pulled out as "
            + "Pubsub Lite message attributes.  For example, if the format is `JSON` "
            + "and attributes is `[\"a\", \"b\"]` then elements of the form "
            + "`Row(any_field=..., a=..., b=...)` will result in Pubsub Lite messages whose "
            + "payload has the contents of any_field and whose attribute will be "
            + "populated with the values of `a` and `b`.")
    public abstract @Nullable List<String> getAttributes();

    @SchemaFieldDescription(
        "If set, will set an attribute for each Pubsub Lite message "
            + "with the given name and a unique value. This attribute can then be used "
            + "in a ReadFromPubSubLite PTransform to deduplicate messages.")
    public abstract @Nullable String getAttributeId();

    @SchemaFieldDescription(
        "The path to the Protocol Buffer File Descriptor Set file. This file is used for schema"
            + " definition and message serialization.")
    public abstract @Nullable String getFileDescriptorPath();

    @SchemaFieldDescription(
        "The name of the Protocol Buffer message to be used for schema"
            + " extraction and data conversion.")
    public abstract @Nullable String getMessageName();

    public abstract @Nullable String getSchema();

    public static Builder builder() {
      return new AutoValue_PubsubLiteWriteSchemaTransformProvider_PubsubLiteWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setProject(String project);

      public abstract Builder setLocation(String location);

      public abstract Builder setTopicName(String topicName);

      public abstract Builder setFormat(String format);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setAttributes(List<String> attributes);

      @SuppressWarnings("unused")
      public abstract Builder setAttributeId(String attributeId);

      @SuppressWarnings("unused")
      public abstract Builder setFileDescriptorPath(String fileDescriptorPath);

      @SuppressWarnings("unused")
      public abstract Builder setMessageName(String messageName);

      @SuppressWarnings("unused")
      public abstract Builder setSchema(String schema);

      public abstract PubsubLiteWriteSchemaTransformConfiguration build();
    }
  }

  public static class SetUuidFromPubSubMessage
      extends PTransform<PCollection<PubSubMessage>, PCollection<PubSubMessage>> {
    private final String attributeId;

    public SetUuidFromPubSubMessage(String attributeId) {
      this.attributeId = attributeId;
    }

    @Override
    public PCollection<PubSubMessage> expand(PCollection<PubSubMessage> input) {
      return input.apply("SetUuidFromPubSubMessage", ParDo.of(new SetUuidFn(attributeId)));
    }

    public static class SetUuidFn extends DoFn<PubSubMessage, PubSubMessage> {
      private final String attributeId;

      public SetUuidFn(String attributeId) {
        this.attributeId = attributeId;
      }

      @ProcessElement
      public void processElement(
          @Element PubSubMessage input, OutputReceiver<PubSubMessage> outputReceiver) {
        PubSubMessage.Builder builder = input.toBuilder();
        builder.putAttributes(
            attributeId, AttributeValues.newBuilder().addValues(Uuid.random().value()).build());
        outputReceiver.output(builder.build());
      }
    }
  }
}
