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
package org.apache.beam.sdk.io.kafka;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class KafkaWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransformConfiguration> {

  public static final String SUPPORTED_FORMATS_STR = "RAW,JSON,AVRO,PROTO";
  public static final Set<String> SUPPORTED_FORMATS =
      Sets.newHashSet(SUPPORTED_FORMATS_STR.split(","));
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final TupleTag<KV<byte[], byte[]>> OUTPUT_TAG =
      new TupleTag<KV<byte[], byte[]>>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(KafkaWriteSchemaTransformProvider.class);

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<KafkaWriteSchemaTransformConfiguration>
      configurationClass() {
    return KafkaWriteSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      KafkaWriteSchemaTransformConfiguration configuration) {
    if (!SUPPORTED_FORMATS.contains(configuration.getFormat())) {
      throw new IllegalArgumentException(
          "Format "
              + configuration.getFormat()
              + " is not supported. "
              + "Supported formats are: "
              + String.join(", ", SUPPORTED_FORMATS));
    }
    return new KafkaWriteSchemaTransform(configuration);
  }

  static final class KafkaWriteSchemaTransform extends SchemaTransform implements Serializable {
    final KafkaWriteSchemaTransformConfiguration configuration;

    KafkaWriteSchemaTransform(KafkaWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically
        return SchemaRegistry.createDefault()
            .getToRowFunction(KafkaWriteSchemaTransformConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    public static class ErrorCounterFn extends DoFn<Row, KV<byte[], byte[]>> {
      private final SerializableFunction<Row, byte[]> toBytesFn;
      private final Counter errorCounter;
      private Long errorsInBundle = 0L;
      private final boolean handleErrors;
      private final Schema errorSchema;

      public ErrorCounterFn(
          String name,
          SerializableFunction<Row, byte[]> toBytesFn,
          Schema errorSchema,
          boolean handleErrors) {
        this.toBytesFn = toBytesFn;
        this.errorCounter = Metrics.counter(KafkaWriteSchemaTransformProvider.class, name);
        this.handleErrors = handleErrors;
        this.errorSchema = errorSchema;
      }

      @ProcessElement
      public void process(@DoFn.Element Row row, MultiOutputReceiver receiver) {
        KV<byte[], byte[]> output = null;
        try {
          output = KV.of(new byte[1], toBytesFn.apply(row));
        } catch (Exception e) {
          if (!handleErrors) {
            throw new RuntimeException(e);
          }
          errorsInBundle += 1;
          LOG.warn("Error while processing the element", e);
          receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, row, e));
        }
        if (output != null) {
          receiver.get(OUTPUT_TAG).output(output);
        }
      }

      @FinishBundle
      public void finish() {
        errorCounter.inc(errorsInBundle);
        errorsInBundle = 0L;
      }
    }

    @SuppressWarnings({
      "nullness" // TODO(https://github.com/apache/beam/issues/20497)
    })
    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema inputSchema = input.get("input").getSchema();
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
      } else if (configuration.getFormat().equals("JSON")) {
        toBytesFn = JsonUtils.getRowToJsonBytesFunction(inputSchema);
      } else if (configuration.getFormat().equals("PROTO")) {
        String descriptorPath = configuration.getFileDescriptorPath();
        String schema = configuration.getSchema();
        String messageName = configuration.getMessageName();
        if (messageName == null) {
          throw new IllegalArgumentException("Expecting messageName to be non-null.");
        }
        if (descriptorPath != null && schema != null) {
          throw new IllegalArgumentException(
              "You must include a descriptorPath or a proto Schema but not both.");
        } else if (descriptorPath != null) {
          toBytesFn = ProtoByteUtils.getRowToProtoBytes(descriptorPath, messageName);
        } else if (schema != null) {
          toBytesFn = ProtoByteUtils.getRowToProtoBytesFromSchema(schema, messageName);
        } else {
          throw new IllegalArgumentException(
              "At least a descriptorPath or a proto Schema is required.");
        }

      } else {
        toBytesFn = AvroUtils.getRowToAvroBytesFunction(inputSchema);
      }

      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      final Map<String, String> configOverrides = configuration.getProducerConfigUpdates();
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
      PCollectionTuple outputTuple =
          input
              .get("input")
              .apply(
                  "Map rows to Kafka messages",
                  ParDo.of(
                          new ErrorCounterFn(
                              "Kafka-write-error-counter", toBytesFn, errorSchema, handleErrors))
                      .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      outputTuple
          .get(OUTPUT_TAG)
          .apply(
              KafkaIO.<byte[], byte[]>write()
                  .withTopic(configuration.getTopic())
                  .withBootstrapServers(configuration.getBootstrapServers())
                  .withProducerConfigUpdates(
                      configOverrides == null
                          ? new HashMap<>()
                          : new HashMap<String, Object>(configOverrides))
                  .withKeySerializer(ByteArraySerializer.class)
                  .withValueSerializer(ByteArraySerializer.class));

      // TODO: include output from KafkaIO Write once updated from PDone
      PCollection<Row> errorOutput =
          outputTuple.get(ERROR_TAG).setRowSchema(ErrorHandling.errorSchema(errorSchema));
      return PCollectionRowTuple.of(
          handleErrors ? configuration.getErrorHandling().getOutput() : "errors", errorOutput);
    }
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
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:kafka_write:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.singletonList("input");
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.emptyList();
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class KafkaWriteSchemaTransformConfiguration implements Serializable {
    @SchemaFieldDescription(
        "The encoding format for the data stored in Kafka. Valid options are: "
            + SUPPORTED_FORMATS_STR)
    public abstract String getFormat();

    public abstract String getTopic();

    @SchemaFieldDescription(
        "A list of host/port pairs to use for establishing the initial connection to the"
            + " Kafka cluster. The client will make use of all servers irrespective of which servers are specified"
            + " here for bootstrapping—this list only impacts the initial hosts used to discover the full set"
            + " of servers. | Format: host1:port1,host2:port2,...")
    public abstract String getBootstrapServers();

    @SchemaFieldDescription(
        "A list of key-value pairs that act as configuration parameters for Kafka producers."
            + " Most of these configurations will not be needed, but if you need to customize your Kafka producer,"
            + " you may use this. See a detailed list:"
            + " https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html")
    @Nullable
    public abstract Map<String, String> getProducerConfigUpdates();

    @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
    @Nullable
    public abstract ErrorHandling getErrorHandling();

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

    @Nullable
    public abstract String getSchema();

    public static Builder builder() {
      return new AutoValue_KafkaWriteSchemaTransformProvider_KafkaWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setFormat(String format);

      public abstract Builder setTopic(String topic);

      public abstract Builder setBootstrapServers(String bootstrapServers);

      public abstract Builder setProducerConfigUpdates(Map<String, String> producerConfigUpdates);

      public abstract Builder setErrorHandling(ErrorHandling errorHandling);

      public abstract Builder setFileDescriptorPath(String fileDescriptorPath);

      public abstract Builder setMessageName(String messageName);

      public abstract Builder setSchema(String schema);

      public abstract KafkaWriteSchemaTransformConfiguration build();
    }
  }
}
