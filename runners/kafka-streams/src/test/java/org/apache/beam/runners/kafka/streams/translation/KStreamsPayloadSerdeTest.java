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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

/** Tests for {@link KStreamsPayloadSerde}. */
public class KStreamsPayloadSerdeTest {

  private static final String TOPIC = "ks-payload-serde-test";

  private final Coder<WindowedValue<Integer>> dataCoder =
      WindowedValues.getFullCoder(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);
  private final KStreamsPayloadSerde<Integer> serde = new KStreamsPayloadSerde<>(dataCoder);

  private KStreamsPayload<Integer> roundTrip(KStreamsPayload<Integer> payload) {
    Serializer<KStreamsPayload<Integer>> serializer = serde.serializer();
    Deserializer<KStreamsPayload<Integer>> deserializer = serde.deserializer();
    return deserializer.deserialize(TOPIC, serializer.serialize(TOPIC, payload));
  }

  @Test
  public void roundTripsDataPayload() {
    KStreamsPayload<Integer> payload = KStreamsPayload.data(WindowedValues.valueInGlobalWindow(42));
    KStreamsPayload<Integer> out = roundTrip(payload);
    assertThat(out.isData(), is(true));
    assertThat(out.getData().getValue(), is(42));
    assertThat(out, is(payload));
  }

  @Test
  public void roundTripsWatermarkPayload() {
    KStreamsPayload<Integer> payload = KStreamsPayload.watermark(12345L, "transform-a", 2, 4);
    KStreamsPayload<Integer> out = roundTrip(payload);
    assertThat(out.isWatermark(), is(true));
    assertThat(out.asWatermark().getWatermarkMillis(), is(12345L));
    assertThat(out.asWatermark().getTransformId(), is("transform-a"));
    assertThat(out.asWatermark().getSourcePartition(), is(2));
    assertThat(out.asWatermark().getTotalSourcePartitions(), is(4));
    assertThat(out, is(payload));
  }

  @Test
  public void roundTripsTerminalMaxWatermark() {
    KStreamsPayload<Integer> payload =
        KStreamsPayload.watermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis(), "t", 0, 1);
    assertThat(
        roundTrip(payload).asWatermark().getWatermarkMillis(),
        is(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
  }

  @Test
  public void roundTripsNegativeWatermark() {
    // Beam event times can be negative; sint64 must round-trip them losslessly.
    KStreamsPayload<Integer> payload =
        KStreamsPayload.watermark(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis(), "t", 0, 1);
    assertThat(
        roundTrip(payload).asWatermark().getWatermarkMillis(),
        is(BoundedWindow.TIMESTAMP_MIN_VALUE.getMillis()));
  }

  @Test
  public void malformedBytesThrow() {
    // 0x7f encodes field 15 with the invalid wire type 7, so protobuf parsing fails.
    byte[] bogus = new byte[] {(byte) 0x7f};
    assertThrows(
        SerializationException.class, () -> serde.deserializer().deserialize(TOPIC, bogus));
  }
}
