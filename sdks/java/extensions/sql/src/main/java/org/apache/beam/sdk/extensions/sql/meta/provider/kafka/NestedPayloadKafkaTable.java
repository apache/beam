package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static org.apache.beam.sdk.extensions.sql.meta.provider.kafka.Schemas.*;
import static org.apache.beam.sdk.extensions.sql.meta.provider.kafka.Schemas.HEADERS_VALUES_FIELD;
import static org.apache.beam.sdk.schemas.transforms.Cast.castRow;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import avro.shaded.com.google.common.collect.ImmutableListMultimap;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

/**
 * A class which transforms kafka records with attributes to a nested table.
 */
class NestedPayloadKafkaTable extends BeamKafkaTable {
  private final @Nullable PayloadSerializer payloadSerializer;

  public NestedPayloadKafkaTable(Schema beamSchema,
      String bootstrapServers, List<String> topics, Optional<PayloadSerializer> payloadSerializer) {
    super(beamSchema, bootstrapServers, topics);

    checkArgument(isNestedSchema(schema));
    if (payloadSerializer.isPresent()) {
      checkArgument(schema.getField(PAYLOAD_FIELD).getType().equals(FieldType.BYTES));
      this.payloadSerializer = payloadSerializer.get();
    } else {
      checkArgument(schema.getField(PAYLOAD_FIELD).getType().getTypeName().equals(TypeName.ROW));
      this.payloadSerializer = null;
    }
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
      ImmutableListMultimap.Builder<String, byte[]> headersBuilder = ImmutableListMultimap.builder();
      record.getHeaders().forEach(header -> headersBuilder.put(header.key(), header.value()));
      ImmutableList.Builder<Row> listBuilder = ImmutableList.builder();
      headersBuilder.build().asMap()
          .forEach(
              (key, values) -> {
                Row entry =
                    Row.withSchema(HEADERS_ENTRY_SCHEMA)
                        .withFieldValue(HEADERS_KEY_FIELD, key)
                        .withFieldValue(HEADERS_VALUES_FIELD, values)
                        .build();
                listBuilder.add(entry);
              });
      builder.withFieldValue(HEADERS_FIELD, listBuilder.build());
    }
    if (payloadSerializer == null) {
      builder.withFieldValue(PAYLOAD_FIELD, record.getKV().getValue());
    } else {
      builder.withFieldValue(
          PAYLOAD_FIELD,
          payloadSerializer.deserialize(record.getKV().getValue()));
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
    row = castRow(row, row.getSchema(), schema);
    String topic = Iterables.getOnlyElement(getTopics());
    byte[] key = null;
    byte[] payload;
    List<Header> headers = ImmutableList.of();
    Long timestampMillis = null;
    if (schema.hasField(MESSAGE_KEY_FIELD)) {
      key = row.getBytes(MESSAGE_KEY_FIELD);
    }
    if (schema.hasField(EVENT_TIMESTAMP_FIELD)) {
      ReadableDateTime time = row.getDateTime(EVENT_TIMESTAMP_FIELD);
      if (time != null) {
        timestampMillis = time.getMillis();
      }
    }
    if (schema.hasField(HEADERS_FIELD)) {
      Collection<Row> headerRows = checkArgumentNotNull(row.getArray(HEADERS_FIELD));
      ImmutableList.Builder<Header> headersBuilder = ImmutableList.builder();
      headerRows.forEach(
          entry -> {
            String headerKey = checkArgumentNotNull(entry.getString(HEADERS_KEY_FIELD));
            Collection<byte[]> values =
                checkArgumentNotNull(entry.getArray(HEADERS_VALUES_FIELD));
            values.forEach(value -> headersBuilder.add(new RecordHeader(headerKey, value)));
          });
      headers = headersBuilder.build();
    }
    if (payloadSerializer == null) {
      payload = row.getBytes(PAYLOAD_FIELD);
    } else {
      payload = payloadSerializer.serialize(checkArgumentNotNull(row.getRow(PAYLOAD_FIELD)));
    }
    return new ProducerRecord<>(topic, null, timestampMillis, key, payload, headers);
  }
}
