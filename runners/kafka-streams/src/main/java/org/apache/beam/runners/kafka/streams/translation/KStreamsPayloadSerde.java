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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Kafka {@link Serde} for {@link KStreamsPayload}, enabling the envelope to cross topic boundaries
 * (e.g. the repartition topic a {@code GroupByKey} introduces). Until now {@link KStreamsPayload}
 * only flowed in-JVM via {@code ProcessorContext#forward}, so no serialization was needed.
 *
 * <p>The wire format is a one-byte discriminator followed by the variant body:
 *
 * <ul>
 *   <li><b>data</b>: {@code [0x00][windowedValueCoder-encoded WindowedValue]} — the data element is
 *       encoded with the {@link Coder} supplied for the topic's PCollection.
 *   <li><b>watermark</b>: {@code [0x01][long watermarkMillis][int sourcePartition][int
 *       totalSourcePartitions]} — the in-band watermark report, coder-independent.
 * </ul>
 *
 * <p>A {@link KStreamsPayloadSerde} is parameterized by the {@link Coder} for the data variant
 * because different topics carry different element types; the watermark variant needs no coder.
 *
 * <p>The serde assumes non-null payloads: the topics it is used on (repartition and watermark
 * fan-out) are not log-compacted, so no tombstone (null-valued) records occur.
 *
 * @param <T> the data element type carried by data payloads on this topic
 */
public final class KStreamsPayloadSerde<T> implements Serde<KStreamsPayload<T>> {

  private static final byte DATA_TAG = 0;
  private static final byte WATERMARK_TAG = 1;

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
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        if (payload.isData()) {
          out.write(DATA_TAG);
          dataCoder.encode(payload.getData(), out);
        } else {
          WatermarkPayload watermark = payload.asWatermark();
          DataOutputStream dataOut = new DataOutputStream(out);
          dataOut.writeByte(WATERMARK_TAG);
          dataOut.writeLong(watermark.getWatermarkMillis());
          dataOut.writeInt(watermark.getSourcePartition());
          dataOut.writeInt(watermark.getTotalSourcePartitions());
          dataOut.flush();
        }
      } catch (IOException e) {
        throw new SerializationException("Failed to serialize KStreamsPayload", e);
      }
      return out.toByteArray();
    }
  }

  private final class PayloadDeserializer implements Deserializer<KStreamsPayload<T>> {
    @Override
    public KStreamsPayload<T> deserialize(String topic, byte[] bytes) {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      try {
        int tag = in.read();
        if (tag == DATA_TAG) {
          return KStreamsPayload.data(dataCoder.decode(in));
        } else if (tag == WATERMARK_TAG) {
          DataInputStream dataIn = new DataInputStream(in);
          long watermarkMillis = dataIn.readLong();
          int sourcePartition = dataIn.readInt();
          int totalSourcePartitions = dataIn.readInt();
          return KStreamsPayload.watermark(watermarkMillis, sourcePartition, totalSourcePartitions);
        } else {
          throw new SerializationException("Unknown KStreamsPayload tag: " + tag);
        }
      } catch (IOException e) {
        throw new SerializationException("Failed to deserialize KStreamsPayload", e);
      }
    }
  }
}
