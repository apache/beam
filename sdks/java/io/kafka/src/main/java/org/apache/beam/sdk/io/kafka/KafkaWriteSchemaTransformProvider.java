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
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class KafkaWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransformConfiguration> {

  public static final String SUPPORTED_FORMATS_STR = "JSON,AVRO";
  public static final Set<String> SUPPORTED_FORMATS =
      Sets.newHashSet(SUPPORTED_FORMATS_STR.split(","));

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

  static final class KafkaWriteSchemaTransform implements SchemaTransform, Serializable {
    final KafkaWriteSchemaTransformConfiguration configuration;

    KafkaWriteSchemaTransform(KafkaWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized PTransform<
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple,
            @UnknownKeyFor @NonNull @Initialized PCollectionRowTuple>
        buildTransform() {
      return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
        @Override
        public PCollectionRowTuple expand(PCollectionRowTuple input) {
          Schema inputSchema = input.get("input").getSchema();
          final SerializableFunction<Row, byte[]> toBytesFn =
              configuration.getFormat().equals("JSON")
                  ? JsonUtils.getRowToJsonBytesFunction(inputSchema)
                  : AvroUtils.getRowToAvroBytesFunction(inputSchema);

          final Map<String, String> configOverrides = configuration.getProducerConfigUpdates();
          input
              .get("input")
              .apply(
                  "Map Rows to Kafka Messages",
                  MapElements.via(
                      new SimpleFunction<Row, KV<byte[], byte[]>>(
                          row -> KV.of(new byte[1], toBytesFn.apply(row))) {}))
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
          return PCollectionRowTuple.empty(input.getPipeline());
        }
      };
    }
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

      public abstract KafkaWriteSchemaTransformConfiguration build();
    }
  }
}
