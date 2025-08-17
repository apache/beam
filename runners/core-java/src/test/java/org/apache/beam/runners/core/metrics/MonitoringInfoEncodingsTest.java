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

import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeBoundedTrie;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeDoubleCounter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeInt64Histogram;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.decodeStringSet;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeBoundedTrie;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleCounter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeDoubleDistribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Counter;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Distribution;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Gauge;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeInt64Histogram;
import static org.apache.beam.runners.core.metrics.MonitoringInfoEncodings.encodeStringSet;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.runners.core.metrics.BoundedTrieData.BoundedTrieNode;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.util.HistogramData;
import org.apache.beam.sdk.util.HistogramData.HistogramParsingException;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MonitoringInfoEncodings}. */
@RunWith(JUnit4.class)
public class MonitoringInfoEncodingsTest {
  @Rule
  public ExpectedLogs monitoringInfoCodingsExpectedLogs =
      ExpectedLogs.none(MonitoringInfoEncodings.class);

  @Rule public ExpectedException thrown = ExpectedException.none();

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
  public void testStringSetEncoding() {

    // test empty string set encoding
    StringSetData data = StringSetData.create(Collections.emptySet());
    ByteString payload = encodeStringSet(data);
    assertEquals(data, decodeStringSet(payload));

    // test single element string set encoding
    data = StringSetData.create(ImmutableSet.of("ab"));
    payload = encodeStringSet(data);
    assertEquals(data, decodeStringSet(payload));

    // test multiple element string set encoding
    data = StringSetData.create(ImmutableSet.of("ab", "cd", "ef"));
    payload = encodeStringSet(data);
    assertEquals(data, decodeStringSet(payload));

    // test empty string encoding
    data = StringSetData.create(ImmutableSet.of("ab", "", "ef"));
    payload = encodeStringSet(data);
    assertEquals(data, decodeStringSet(payload));
  }

  @Test
  public void testBoundedTrieEncoding() {
    // test empty bounded trie encoding
    BoundedTrieData data = new BoundedTrieData(Collections.emptyList());
    ByteString payload = encodeBoundedTrie(data);
    assertEquals(data, decodeBoundedTrie(payload));

    // test singleton encoding
    data = new BoundedTrieData(ImmutableList.of("ab"));
    payload = encodeBoundedTrie(data);
    assertEquals(data, decodeBoundedTrie(payload));

    // test multiple element bounded trie encoding
    data = new BoundedTrieData(ImmutableList.of("a", "b"));
    data.add(ImmutableList.of("c", "d"));
    payload = encodeBoundedTrie(data);
    assertEquals(data, decodeBoundedTrie(payload));

    // test encoding with trim
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));
    root.trim();
    data = new BoundedTrieData(root);
    payload = encodeBoundedTrie(data);
    assertEquals(data, decodeBoundedTrie(payload));
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

  @Test
  public void testHistgramInt64EncodingLinearHist() {
    HistogramData.BucketType buckets = HistogramData.LinearBuckets.of(0, 5, 5);

    HistogramData inputHistogram = new HistogramData(buckets);
    inputHistogram.record(5, 10, 15, 20);
    ByteString payload = encodeInt64Histogram(inputHistogram);

    assertEquals(inputHistogram, decodeInt64Histogram(payload));
  }

  @Test
  public void testHistgramInt64EncodingExpHist() {
    HistogramData.BucketType buckets = HistogramData.ExponentialBuckets.of(1, 10);
    HistogramData inputHistogram = new HistogramData(buckets);
    inputHistogram.record(2, 4, 8, 16, 32);
    ByteString payload = encodeInt64Histogram(inputHistogram);
    assertEquals(inputHistogram, decodeInt64Histogram(payload));
  }

  @Test
  public void testHistgramInt64EncodingUnsupportedBucket() {
    thrown.expect(HistogramParsingException.class);
    thrown.expectMessage("Unable to encode Int64 Histogram, bucket is not recognized");

    HistogramData.BucketType buckets = HistogramData.UnsupportedBuckets.of();

    HistogramData inputHistogram = new HistogramData(buckets);
    inputHistogram.record(2, 4, 8, 16, 32);
    encodeInt64Histogram(inputHistogram);
  }
}
