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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;

/**
 * Configuration for reading from a Kafka topic.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class KafkaReadSchemaTransformConfiguration {

  public static final Set<String> VALID_START_OFFSET_VALUES = Sets.newHashSet("earliest", "latest");

  public static final String VALID_FORMATS_STR = "RAW,AVRO,JSON,PROTO";
  public static final Set<String> VALID_DATA_FORMATS =
      Sets.newHashSet(VALID_FORMATS_STR.split(","));

  public void validate() {
    final String startOffset = this.getAutoOffsetResetConfig();
    checkArgument(
        startOffset == null || VALID_START_OFFSET_VALUES.contains(startOffset),
        "Valid Kafka Start offset values are " + VALID_START_OFFSET_VALUES);
    final String dataFormat = this.getFormat();
    checkArgument(
        dataFormat == null || VALID_DATA_FORMATS.contains(dataFormat),
        "Valid data formats are " + VALID_DATA_FORMATS);

    final String inputSchema = this.getSchema();
    final String messageName = this.getMessageName();
    final String fileDescriptorPath = this.getFileDescriptorPath();
    final String confluentSchemaRegUrl = this.getConfluentSchemaRegistryUrl();
    final String confluentSchemaRegSubject = this.getConfluentSchemaRegistrySubject();

    if (confluentSchemaRegUrl != null) {
      checkNotNull(
          confluentSchemaRegSubject,
          "To read from Kafka, a schema must be provided directly or though Confluent "
              + "Schema Registry. Make sure you are providing one of these parameters.");
    } else if (dataFormat != null && dataFormat.equals("RAW")) {
      checkArgument(
          inputSchema == null, "To read from Kafka in RAW format, you can't provide a schema.");
    } else if (dataFormat != null && dataFormat.equals("JSON")) {
      checkNotNull(inputSchema, "To read from Kafka in JSON format, you must provide a schema.");
    } else if (dataFormat != null && dataFormat.equals("PROTO")) {
      checkNotNull(
          messageName, "To read from Kafka in PROTO format, messageName must be provided.");
      checkArgument(
          fileDescriptorPath != null || inputSchema != null,
          "To read from Kafka in PROTO format, fileDescriptorPath or schema must be provided.");
    } else {
      checkNotNull(inputSchema, "To read from Kafka in AVRO format, you must provide a schema.");
    }
  }

  /** Instantiates a {@link KafkaReadSchemaTransformConfiguration.Builder} instance. */
  public static Builder builder() {
    return new AutoValue_KafkaReadSchemaTransformConfiguration.Builder();
  }

  /** Sets the bootstrap servers for the Kafka consumer. */
  @SchemaFieldDescription(
      "A list of host/port pairs to use for establishing the initial connection to the"
          + " Kafka cluster. The client will make use of all servers irrespective of which servers are specified"
          + " here for bootstrapping—this list only impacts the initial hosts used to discover the full set"
          + " of servers. This list should be in the form `host1:port1,host2:port2,...`")
  public abstract String getBootstrapServers();

  @Nullable
  public abstract String getConfluentSchemaRegistryUrl();

  @SchemaFieldDescription(
      "The encoding format for the data stored in Kafka. Valid options are: " + VALID_FORMATS_STR)
  @Nullable
  public abstract String getFormat();

  @Nullable
  public abstract String getConfluentSchemaRegistrySubject();

  @SchemaFieldDescription(
      "The schema in which the data is encoded in the Kafka topic. "
          + "For AVRO data, this is a schema defined with AVRO schema syntax "
          + "(https://avro.apache.org/docs/1.10.2/spec.html#schemas). "
          + "For JSON data, this is a schema defined with JSON-schema syntax (https://json-schema.org/). "
          + "If a URL to Confluent Schema Registry is provided, then this field is ignored, and the schema "
          + "is fetched from Confluent Schema Registry.")
  @Nullable
  public abstract String getSchema();

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

  @SchemaFieldDescription(
      "What to do when there is no initial offset in Kafka or if the current offset"
          + " does not exist any more on the server. (1) earliest: automatically reset the offset to the earliest"
          + " offset. (2) latest: automatically reset the offset to the latest offset"
          + " (3) none: throw exception to the consumer if no previous offset is found for the consumer’s group")
  @Nullable
  public abstract String getAutoOffsetResetConfig();

  @SchemaFieldDescription(
      "A list of key-value pairs that act as configuration parameters for Kafka consumers."
          + " Most of these configurations will not be needed, but if you need to customize your Kafka consumer,"
          + " you may use this. See a detailed list:"
          + " https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html")
  @Nullable
  public abstract Map<String, String> getConsumerConfigUpdates();

  /** Sets the topic from which to read. */
  public abstract String getTopic();

  @SchemaFieldDescription("Upper bound of how long to read from Kafka.")
  @Nullable
  public abstract Integer getMaxReadTimeSeconds();

  @SchemaFieldDescription("This option specifies whether and where to output unwritable rows.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  /** Builder for the {@link KafkaReadSchemaTransformConfiguration}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the bootstrap servers for the Kafka consumer. */
    public abstract Builder setBootstrapServers(String value);

    public abstract Builder setConfluentSchemaRegistryUrl(String schemaRegistry);

    public abstract Builder setConfluentSchemaRegistrySubject(String subject);

    public abstract Builder setSchema(String schema);

    public abstract Builder setFileDescriptorPath(String fileDescriptorPath);

    public abstract Builder setMessageName(String messageName);

    public abstract Builder setFormat(String format);

    public abstract Builder setAutoOffsetResetConfig(String startOffset);

    public abstract Builder setConsumerConfigUpdates(Map<String, String> consumerConfigUpdates);

    /** Sets the topic from which to read. */
    public abstract Builder setTopic(String value);

    public abstract Builder setMaxReadTimeSeconds(Integer maxReadTimeSeconds);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    /** Builds a {@link KafkaReadSchemaTransformConfiguration} instance. */
    public abstract KafkaReadSchemaTransformConfiguration build();
  }
}
