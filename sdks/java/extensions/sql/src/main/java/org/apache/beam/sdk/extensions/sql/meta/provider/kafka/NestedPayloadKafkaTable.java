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

import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

/** A class which transforms kafka records with attributes to a nested table. */
class NestedPayloadKafkaTable extends BeamKafkaTable {
  private final @Nullable PayloadSerializer payloadSerializer;

  public NestedPayloadKafkaTable(
      Schema beamSchema,
      String bootstrapServers,
      List<String> topics,
      Optional<PayloadSerializer> payloadSerializer) {
    this(
        beamSchema,
        bootstrapServers,
        topics,
        payloadSerializer,
        TimestampPolicyFactory.withLogAppendTime(),
        null);
  }

  public NestedPayloadKafkaTable(
      Schema beamSchema,
      String bootstrapServers,
      List<String> topics,
      Optional<PayloadSerializer> payloadSerializer,
      TimestampPolicyFactory timestampPolicyFactory) {
    this(
        beamSchema,
        bootstrapServers,
        topics,
        payloadSerializer,
        timestampPolicyFactory,
        /*consumerFactoryFn=*/null);
  }

  public NestedPayloadKafkaTable(
      Schema beamSchema,
      String bootstrapServers,
      List<String> topics,
      Optional<PayloadSerializer> payloadSerializer,
      TimestampPolicyFactory timestampPolicyFactory,
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
    super(beamSchema, bootstrapServers, topics, timestampPolicyFactory, consumerFactoryFn);

    checkArgument(Schemas.isNestedSchema(schema));
    Schemas.validateNestedSchema(schema);
    if (payloadSerializer.isPresent()) {
      checkArgument(
          schema.getField(Schemas.PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
      this.payloadSerializer = payloadSerializer.get();
    } else {
      checkArgument(schema.getField(Schemas.PAYLOAD_FIELD).getType().equals(FieldType.BYTES));
      this.payloadSerializer = null;
    }
  }

  @Override
  protected PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>
      getPTransformForInput() {
    return new PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
        return input.apply(
            MapElements.into(new TypeDescriptor<Row>() {}).via(record -> transformInput(record)));
      }
    };
  }

  @VisibleForTesting
  Row transformInput(KafkaRecord<byte[], byte[]> record) {
    Row.FieldValueBuilder builder = Row.withSchema(getSchema()).withFieldValues(ImmutableMap.of());
    if (schema.hasField(Schemas.MESSAGE_KEY_FIELD)) {
      builder.withFieldValue(Schemas.MESSAGE_KEY_FIELD, record.getKV().getKey());
    }
    if (schema.hasField(Schemas.EVENT_TIMESTAMP_FIELD)) {
      builder.withFieldValue(
          Schemas.EVENT_TIMESTAMP_FIELD, Instant.ofEpochMilli(record.getTimestamp()));
    }
    if (schema.hasField(Schemas.HEADERS_FIELD)) {
      @Nullable Headers recordHeaders = record.getHeaders();
      if (recordHeaders != null) {
        ImmutableListMultimap.Builder<String, byte[]> headersBuilder =
            ImmutableListMultimap.builder();
        recordHeaders.forEach(header -> headersBuilder.put(header.key(), header.value()));
        ImmutableList.Builder<Row> listBuilder = ImmutableList.builder();
        headersBuilder
            .build()
            .asMap()
            .forEach(
                (key, values) -> {
                  Row entry =
                      Row.withSchema(Schemas.HEADERS_ENTRY_SCHEMA)
                          .withFieldValue(Schemas.HEADERS_KEY_FIELD, key)
                          .withFieldValue(Schemas.HEADERS_VALUES_FIELD, values)
                          .build();
                  listBuilder.add(entry);
                });
        builder.withFieldValue(Schemas.HEADERS_FIELD, listBuilder.build());
      }
    }
    if (payloadSerializer == null) {
      builder.withFieldValue(Schemas.PAYLOAD_FIELD, record.getKV().getValue());
    } else {
      byte[] payload = record.getKV().getValue();
      if (payload != null) {
        builder.withFieldValue(
            Schemas.PAYLOAD_FIELD, payloadSerializer.deserialize(record.getKV().getValue()));
      }
    }
    return builder.build();
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>
      getPTransformForOutput() {
    return new PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>() {
      @Override
      public PCollection<ProducerRecord<byte[], byte[]>> expand(PCollection<Row> input) {
        return input.apply(
            MapElements.into(new TypeDescriptor<ProducerRecord<byte[], byte[]>>() {})
                .via(row -> transformOutput(row)));
      }
    };
  }

  // Suppress nullability warnings: ProducerRecord is supposed to accept null arguments.
  @SuppressWarnings("argument")
  @VisibleForTesting
  ProducerRecord<byte[], byte[]> transformOutput(Row row) {
    row = castRow(row, row.getSchema(), schema);
    String topic = Iterables.getOnlyElement(getTopics());
    byte[] key = null;
    byte[] payload;
    List<Header> headers = ImmutableList.of();
    Long timestampMillis = null;
    if (schema.hasField(Schemas.MESSAGE_KEY_FIELD)) {
      key = row.getBytes(Schemas.MESSAGE_KEY_FIELD);
    }
    if (schema.hasField(Schemas.EVENT_TIMESTAMP_FIELD)) {
      ReadableDateTime time = row.getDateTime(Schemas.EVENT_TIMESTAMP_FIELD);
      if (time != null) {
        timestampMillis = time.getMillis();
      }
    }
    if (schema.hasField(Schemas.HEADERS_FIELD)) {
      Collection<Row> headerRows = checkArgumentNotNull(row.getArray(Schemas.HEADERS_FIELD));
      ImmutableList.Builder<Header> headersBuilder = ImmutableList.builder();
      headerRows.forEach(
          entry -> {
            String headerKey = checkArgumentNotNull(entry.getString(Schemas.HEADERS_KEY_FIELD));
            Collection<byte[]> values =
                checkArgumentNotNull(entry.getArray(Schemas.HEADERS_VALUES_FIELD));
            values.forEach(value -> headersBuilder.add(new RecordHeader(headerKey, value)));
          });
      headers = headersBuilder.build();
    }
    if (payloadSerializer == null) {
      payload = row.getBytes(Schemas.PAYLOAD_FIELD);
    } else {
      payload =
          payloadSerializer.serialize(checkArgumentNotNull(row.getRow(Schemas.PAYLOAD_FIELD)));
    }
    return new ProducerRecord<>(topic, null, timestampMillis, key, payload, headers);
  }
}
