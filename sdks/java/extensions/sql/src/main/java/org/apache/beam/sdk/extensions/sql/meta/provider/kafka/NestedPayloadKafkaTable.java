package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.sdk.extensions.sql.meta.provider.kafka.Schemas.*;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import avro.shaded.com.google.common.collect.ImmutableListMultimap;
import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.MultimapBuilder.ListMultimapBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Instant;

/**
 * A class which transforms kafka records with attributes to a nested table.
 */
class NestedPayloadKafkaTable extends BeamKafkaTable {
  private final PayloadSerializer serializer;

  public NestedPayloadKafkaTable(Schema beamSchema,
      String bootstrapServers, List<String> topics) {
    super(beamSchema, bootstrapServers, topics);
    checkArgument(isNestedSchema(schema));
  }

  @Override
  protected PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>> getPTransformForInput() {
    return new PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<Row>>() {
      @Override
      public PCollection<Row> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
        return input.apply(MapElements.into(new TypeDescriptor<Row>() {}).via(record -> transformInput(record)));
      }
    };
  }

  private Row transformInput(KafkaRecord<byte[], byte[]> record) {
    Row.FieldValueBuilder builder = Row.withSchema(getSchema()).withFieldValues(ImmutableMap.of());
    if (schema.hasField(MESSAGE_KEY_FIELD)) {
      builder.withFieldValue(MESSAGE_KEY_FIELD, record.getKV().getKey());
    }
    if (schema.hasField(EVENT_TIMESTAMP_FIELD)) {
      builder.withFieldValue(
          EVENT_TIMESTAMP_FIELD,
          Instant.ofEpochMilli(record.getTimestamp()));
    }
    if (schema.hasField(HEADERS_FIELD)) {
      ImmutableList.Builder<Row> listBuilder = ImmutableList.builder();
      ImmutableListMultimap.Builder<String, byte[]> headersBuilder = ImmutableListMultimap.builder();
      record.getHeaders().forEach(header -> {
        headersBuilder.put(header.key(), header.value());
      });
      message
          .getMessage()
          .getAttributesMap()
          .forEach(
              (key, values) -> {
                Row entry =
                    Row.withSchema(ATTRIBUTES_ENTRY_SCHEMA)
                        .withFieldValue(ATTRIBUTES_KEY_FIELD, key)
                        .withFieldValue(
                            ATTRIBUTES_VALUES_FIELD,
                            values.getValuesList().stream()
                                .map(ByteString::toByteArray)
                                .collect(Collectors.toList()))
                        .build();
                listBuilder.add(entry);
              });
      builder.withFieldValue(ATTRIBUTES_FIELD, listBuilder.build());
    }
    if (payloadSerializer == null) {
      builder.withFieldValue(PAYLOAD_FIELD, message.getMessage().getData().toByteArray());
    } else {
      builder.withFieldValue(
          PAYLOAD_FIELD,
          payloadSerializer.deserialize(message.getMessage().getData().toByteArray()));
    }
    return builder.build();
  }

  @Override
  protected PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>> getPTransformForOutput() {
    return new PTransform<PCollection<Row>, PCollection<ProducerRecord<byte[], byte[]>>>() {
      @Override
      public PCollection<ProducerRecord<byte[], byte[]>> expand(PCollection<Row> input) {
        return input.apply(MapElements.into(new TypeDescriptor<ProducerRecord<byte[], byte[]>>() {}).via(row -> transformOutput(row)));
      }
    };
  }

  private ProducerRecord<byte[], byte[]> transformOutput(Row row) {
    String topic = Iterables.getOnlyElement(getTopics());

  }
}
