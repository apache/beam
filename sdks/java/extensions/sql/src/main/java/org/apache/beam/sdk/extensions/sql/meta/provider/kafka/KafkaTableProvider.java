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

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  private static List<String> mergeParam(Optional<String> initial, @Nullable List<Object> toMerge) {
    ImmutableList.Builder<String> merged = ImmutableList.builder();
    initial.ifPresent(merged::add);
    if (toMerge != null) {
      toMerge.forEach(o -> merged.add(o.toString()));
    }
    return merged.build();
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    Schema schema = table.getSchema();
    JSONObject properties = table.getProperties();

    Optional<ParsedLocation> parsedLocation = Optional.empty();
    if (!Strings.isNullOrEmpty(table.getLocation())) {
      parsedLocation = Optional.of(parseLocation(checkArgumentNotNull(table.getLocation())));
    }
    List<String> topics =
        mergeParam(parsedLocation.map(loc -> loc.topic), properties.getJSONArray("topics"));
    List<String> allBootstrapServers =
        mergeParam(
            parsedLocation.map(loc -> loc.brokerLocation),
            properties.getJSONArray("bootstrap_servers"));
    String bootstrapServers = String.join(",", allBootstrapServers);

    Optional<String> payloadFormat =
        properties.containsKey("format")
            ? Optional.of(properties.getString("format"))
            : Optional.empty();
    if (Schemas.isNestedSchema(schema)) {
      Optional<PayloadSerializer> serializer =
          payloadFormat.map(
              format ->
                  PayloadSerializers.getSerializer(
                      format,
                      checkArgumentNotNull(schema.getField(PAYLOAD_FIELD).getType().getRowSchema()),
                      properties.getInnerMap()));
      return new NestedPayloadKafkaTable(schema, bootstrapServers, topics, serializer);
    } else {
      /*
       * CSV is handled separately because multiple rows can be produced from a single message, which
       * adds complexity to payload extraction. It remains here and as the default because it is the
       * historical default, but it will not be extended to support attaching extended attributes to
       * rows.
       */
      if (payloadFormat.orElse("csv").equals("csv")) {
        return new BeamKafkaCSVTable(schema, bootstrapServers, topics);
      }
      PayloadSerializer serializer =
          PayloadSerializers.getSerializer(payloadFormat.get(), schema, properties.getInnerMap());
      return new PayloadSerializerKafkaTable(schema, bootstrapServers, topics, serializer);
    }
  }

  @Override
  public String getTableType() {
    return "kafka";
  }
}
