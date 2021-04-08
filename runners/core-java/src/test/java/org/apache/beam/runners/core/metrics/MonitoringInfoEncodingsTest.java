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
package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeDoubleCounter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleCounter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleDistribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Gauge;
import static org.junit.Assert.assertEquals;

import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MonitoringInfoEncodings}. */
@RunWith(JUnit4.class)
public class MonitoringInfoEncodingsTest {
  @Test
  public void testInt64DistributionEncoding() {
    DistributionData data = DistributionData.create(1L, 2L, 3L, 4L);
    ByteString payload = encodeInt64Distribution(data);
    assertEquals(ByteString.copyFrom(new byte[] {2, 1, 3, 4}), payload);
    assertEquals(data, decodeInt64Distribution(payload));
  }

  @Test
  public void testDoubleDistributionEncoding() {
    ByteString payload = encodeDoubleDistribution(1L, 2.0, 3.0, 4.0);
    assertEquals(
        ByteString.copyFrom(
            new byte[] {
              1, 64, 0, 0, 0, 0, 0, 0, 0, 64, 8, 0, 0, 0, 0, 0, 0, 64, 16, 0, 0, 0, 0, 0, 0
            }),
        payload);
  }

  @Test
  public void testInt64GaugeEncoding() {
    GaugeData data = GaugeData.create(1L, new Instant(2L));
    ByteString payload = encodeInt64Gauge(data);
    assertEquals(ByteString.copyFrom(new byte[] {2, 1}), payload);
    assertEquals(data, decodeInt64Gauge(payload));
  }

  @Test
  public void testInt64CounterEncoding() {
    ByteString payload = encodeInt64Counter(1L);
    assertEquals(ByteString.copyFrom(new byte[] {0x01}), payload);
    assertEquals(1L, decodeInt64Counter(payload));
  }

  @Test
  public void testDoubleCounterEncoding() {
    ByteString payload = encodeDoubleCounter(1.0);
    assertEquals(ByteString.copyFrom(new byte[] {0x3f, (byte) 0xf0, 0, 0, 0, 0, 0, 0}), payload);
    assertEquals(1.0, decodeDoubleCounter(payload), 0.001);
  }
}
