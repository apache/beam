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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.runners.kafka.streams.v1.KafkaStreamsPayloadProtos.KafkaStreamsPayload;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@link Serde} for {@link KStreamsPayload}, enabling the envelope to cross topic boundaries
 * (e.g. the repartition topic a {@code GroupByKey} introduces). Until now {@link KStreamsPayload}
 * only flowed in-JVM via {@code ProcessorContext#forward}, so no serialization was needed.
 *
 * <p>The wire form is the {@link KafkaStreamsPayload} protobuf message — protobuf gives compatible
 * schema evolution and compact varint encoding. The data variant carries the {@link WindowedValue}
 * encoded with the {@link Coder} supplied for the topic's PCollection; the watermark variant
 * carries the coder-independent watermark report. A {@link KStreamsPayloadSerde} is therefore
 * parameterized by the data {@link Coder} (different topics carry different element types).
 *
 * <p>The serde assumes non-null payloads: the topics it is used on (repartition and watermark
 * fan-out) are not log-compacted, so no tombstone (null-valued) records occur.
 *
 * @param <T> the data element type carried by data payloads on this topic
 */
public final class KStreamsPayloadSerde<T> implements Serde<KStreamsPayload<T>> {

  private final Coder<WindowedValue<T>> dataCoder;

  public KStreamsPayloadSerde(Coder<WindowedValue<T>> dataCoder) {
    this.dataCoder = dataCoder;
  }

  @Override
  public Serializer<KStreamsPayload<T>> serializer() {
    return new PayloadSerializer();
  }

  @Override
  public Deserializer<KStreamsPayload<T>> deserializer() {
    return new PayloadDeserializer();
  }

  private final class PayloadSerializer implements Serializer<KStreamsPayload<T>> {
    @Override
    public byte[] serialize(String topic, KStreamsPayload<T> payload) {
      KafkaStreamsPayload.Builder proto = KafkaStreamsPayload.newBuilder();
      if (payload.isData()) {
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try {
          dataCoder.encode(payload.getData(), encoded);
        } catch (IOException e) {
          throw new SerializationException("Failed to encode KStreamsPayload data element", e);
        }
        proto.setData(
            KafkaStreamsPayload.DataPayload.newBuilder()
                .setValue(ByteString.copyFrom(encoded.toByteArray())));
      } else {
        WatermarkPayload watermark = payload.asWatermark();
        proto.setWatermark(
            KafkaStreamsPayload.WatermarkPayload.newBuilder()
                .setMillis(watermark.getWatermarkMillis())
                .setTransformId(watermark.getTransformId())
                .setSourcePartition(watermark.getSourcePartition())
                .setTotalPartitions(watermark.getTotalSourcePartitions()));
      }
      return proto.build().toByteArray();
    }
  }

  private final class PayloadDeserializer implements Deserializer<KStreamsPayload<T>> {
    @Override
    public KStreamsPayload<T> deserialize(String topic, byte[] bytes) {
      KafkaStreamsPayload proto;
      try {
        proto = KafkaStreamsPayload.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new SerializationException("Failed to parse KStreamsPayload", e);
      }
      switch (proto.getPayloadCase()) {
        case DATA:
          try {
            return KStreamsPayload.data(dataCoder.decode(proto.getData().getValue().newInput()));
          } catch (IOException e) {
            throw new SerializationException("Failed to decode KStreamsPayload data element", e);
          }
        case WATERMARK:
          KafkaStreamsPayload.WatermarkPayload watermark = proto.getWatermark();
          return KStreamsPayload.watermark(
              watermark.getMillis(),
              watermark.getTransformId(),
              watermark.getSourcePartition(),
              watermark.getTotalPartitions());
        case PAYLOAD_NOT_SET:
        default:
          throw new SerializationException(
              "KStreamsPayload has no payload variant set: " + proto.getPayloadCase());
      }
    }
  }
}
