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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.sdk.extensions.sql.meta.provider.kafka.Schemas.PAYLOAD_FIELD;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;

/**
 * Kafka table provider.
 *
 * <p>A sample of text table is:
 *
 * <pre>{@code
 * CREATE TABLE ORDERS(
 *   ID INT COMMENT 'this is the primary key',
 *   NAME VARCHAR(127) COMMENT 'this is the name'
 * )
 * COMMENT 'this is the table orders'
 * TYPE kafka
 * // Optional. One broker host:port pair to bootstrap with and a topic.
 * // Only one topic overall may be provided for writing.
 * LOCATION 'my.company.url.com:2181/topic1'
 * // Extra bootstrap_servers and topics can be provided explicitly. These will be merged
 * // with the server and topic in LOCATION.
 * TBLPROPERTIES '{
 *   "bootstrap_servers": ["104.126.7.88:7743", "104.111.9.22:7743"],
 *   "topics": ["topic2", "topic3"]
 * }'
 * }</pre>
 */
@AutoService(TableProvider.class)
public class KafkaTableProvider extends InMemoryMetaTableProvider {
  private static class ParsedLocation {
    String brokerLocation = "";
    String topic = "";
  }

  private static ParsedLocation parseLocation(String location) {
    ParsedLocation parsed = new ParsedLocation();
    List<String> split = Splitter.on('/').splitToList(location);
    checkArgument(
        split.size() >= 2,
        "Location string `%s` invalid: must be <broker bootstrap location>/<topic>.",
        location);
    parsed.topic = Iterables.getLast(split);
    parsed.brokerLocation = String.join("/", split.subList(0, split.size() - 1));
    return parsed;
  }

  private static List<String> mergeParam(Optional<String> initial, @Nullable ArrayNode toMerge) {
    ImmutableList.Builder<String> merged = ImmutableList.builder();
    initial.ifPresent(merged::add);
    if (toMerge != null) {
      toMerge.forEach(o -> merged.add(o.asText()));
    }
    return merged.build();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema schema = table.getSchema();
    ObjectNode properties = table.getProperties();

    Optional<ParsedLocation> parsedLocation = Optional.empty();
    if (!Strings.isNullOrEmpty(table.getLocation())) {
      parsedLocation = Optional.of(parseLocation(checkArgumentNotNull(table.getLocation())));
    }
    List<String> topics =
        mergeParam(parsedLocation.map(loc -> loc.topic), (ArrayNode) properties.get("topics"));
    List<String> allBootstrapServers =
        mergeParam(
            parsedLocation.map(loc -> loc.brokerLocation),
            (ArrayNode) properties.get("bootstrap_servers"));
    String bootstrapServers = String.join(",", allBootstrapServers);

    Optional<String> payloadFormat =
        properties.has("format")
            ? Optional.of(properties.get("format").asText())
            : Optional.empty();

    TimestampPolicyFactory timestampPolicyFactory = TimestampPolicyFactory.withProcessingTime();
    if (properties.has("watermark.type")) {
      String type = properties.get("watermark.type").asText().toUpperCase();

      switch (type) {
        case "PROCESSINGTIME":
          timestampPolicyFactory = TimestampPolicyFactory.withProcessingTime();
          break;
        case "LOGAPPENDTIME":
          timestampPolicyFactory = TimestampPolicyFactory.withLogAppendTime();
          break;
        case "CREATETIME":
          Duration delay = Duration.ZERO;
          if (properties.has("watermark.delay")) {
            String delayStr = properties.get("watermark.delay").asText();
            delay = PeriodFormat.getDefault().parsePeriod(delayStr).toStandardDuration();
          }
          timestampPolicyFactory = TimestampPolicyFactory.withCreateTime(delay);
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown watermark type: "
                  + type
                  + ". Supported types are ProcessingTime, LogAppendTime, CreateTime.");
      }
    }

    SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFnClass;
    if (properties.has("consumer.factory.fn")) {
      String consumerFactoryFnAsString = properties.get("consumer.factory.fn").asText();
      if (consumerFactoryFnAsString.contains("KerberosConsumerFactoryFn")) {
        if (!properties.has("consumer.factory.fn.params") || !properties.get("consumer.factory.fn.params").has("krb5Location")) {
          throw new RuntimeException("KerberosConsumerFactoryFn requires a krb5Location parameter, but none was set.");
        }
      }
      try {
        consumerFactoryFnClass =
            InstanceBuilder.ofType(
                new TypeDescriptor<
                    SerializableFunction<
                        Map<String, Object>, Consumer<byte[], byte[]>>>() {
                })
                .fromClassName(properties.get("consumer.factory.fn").asText())
                .withArg(String.class,
                    Objects
                        .requireNonNull(properties.get("consumer.factory.fn.params")
                        .get("krb5Location")
                        .asText()))
              .build();
      } catch (Exception e) {
        throw new RuntimeException("Unable to construct the ConsumerFactoryFn class.", e.getMessage());
      }
    }

    BeamKafkaTable kafkaTable = null;
    if (Schemas.isNestedSchema(schema)) {
      Optional<PayloadSerializer> serializer =
          payloadFormat.map(
              format ->
                  PayloadSerializers.getSerializer(
                      format,
                      checkArgumentNotNull(schema.getField(PAYLOAD_FIELD).getType().getRowSchema()),
                      TableUtils.convertNode2Map(properties)));
      kafkaTable =
          new NestedPayloadKafkaTable(
              schema, bootstrapServers, topics, serializer, timestampPolicyFactory, consumerFactoryFnClass);
    } else {
      /*
       * CSV is handled separately because multiple rows can be produced from a single message, which
       * adds complexity to payload extraction. It remains here and as the default because it is the
       * historical default, but it will not be extended to support attaching extended attributes to
       * rows.
       */
      if (payloadFormat.orElse("csv").equals("csv")) {
        kafkaTable =
            new BeamKafkaCSVTable(schema, bootstrapServers, topics, timestampPolicyFactory, consumerFactoryFnClass);
      } else {
        PayloadSerializer serializer =
            PayloadSerializers.getSerializer(
                payloadFormat.get(), schema, TableUtils.convertNode2Map(properties));
        kafkaTable =
            new PayloadSerializerKafkaTable(
                schema, bootstrapServers, topics, serializer, timestampPolicyFactory, consumerFactoryFnClass);
      }
    }

    // Get Consumer Properties from Table properties
    HashMap<String, Object> configUpdates = new HashMap<String, Object>();
    Iterator<Entry<String, JsonNode>> tableProperties = properties.fields();
    while (tableProperties.hasNext()) {
      Entry<String, JsonNode> field = tableProperties.next();
      if (field.getKey().startsWith("properties.")) {
        configUpdates.put(field.getKey().replace("properties.", ""), field.getValue().textValue());
      }
    }

    if (!configUpdates.isEmpty()) {
      kafkaTable.updateConsumerProperties(configUpdates);
    }

    return kafkaTable;
  }

  @Override
  public String getTableType() {
    return "kafka";
  }
}
