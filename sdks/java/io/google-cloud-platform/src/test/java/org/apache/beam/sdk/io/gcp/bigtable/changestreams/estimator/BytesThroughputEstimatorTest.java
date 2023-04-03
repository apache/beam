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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

public class BytesThroughputEstimatorTest {
  private static final double DELTA = 1e-10;
  private static final int WINDOW_SIZE_SECONDS = 10;
  private BytesThroughputEstimator<byte[]> estimator;

  @Before
  public void setup() {
    final SizeEstimator<byte[]> sizeEstimator = new SizeEstimator<>(new TestCoder());
    estimator = new BytesThroughputEstimator<>(WINDOW_SIZE_SECONDS, sizeEstimator, 1);
  }

  @Test
  public void testThroughputIsZeroWhenNothingsBeenRegistered() {
    assertEquals(0D, estimator.get(), DELTA);
    assertEquals(0D, estimator.getFrom(Instant.now()), DELTA);
  }

  @Test
  public void testThroughputCalculation() {
    estimator.update(Instant.ofEpochSecond(2), new byte[10]);
    estimator.update(Instant.ofEpochSecond(3), new byte[20]);
    estimator.update(Instant.ofEpochSecond(5), new byte[30]);
    estimator.update(Instant.ofEpochSecond(10), new byte[40]);
    // (10 + 20 + 30 + 40) / 10 sec window = 10
    assertEquals(10D, estimator.getFrom(Instant.ofEpochSecond(11)), DELTA);

    estimator.update(Instant.ofEpochSecond(20), new byte[10]);
    estimator.update(Instant.ofEpochSecond(21), new byte[20]);
    estimator.update(Instant.ofEpochSecond(21), new byte[10]);
    estimator.update(Instant.ofEpochSecond(29), new byte[40]);
    // (10 + 20 + 10 + 40) / 10 sec window = 8
    assertEquals(8D, estimator.getFrom(Instant.ofEpochSecond(30)), DELTA);

    estimator.update(Instant.ofEpochSecond(31), new byte[10]);
    estimator.update(Instant.ofEpochSecond(35), new byte[40]);
    // (10 + 40) / 10 sec window = 5
    assertEquals(5D, estimator.getFrom(Instant.ofEpochSecond(41)), DELTA);

    // No values in the past 10 seconds
    assertEquals(0D, estimator.getFrom(Instant.ofEpochSecond(50)), DELTA);
  }

  @Test
  public void testThroughputIsAccumulatedWithin60SecondsWindow() {
    List<ImmutablePair<Instant, byte[]>> pairs = generateTestData(100, 0, 10);
    pairs.sort(Comparator.comparing(ImmutablePair::getLeft));
    final Instant lastUpdateTimestamp = pairs.get(pairs.size() - 1).getLeft();

    BigDecimal sum = BigDecimal.valueOf(0L);
    for (ImmutablePair<Instant, byte[]> pair : pairs) {
      sum = sum.add(BigDecimal.valueOf(pair.getRight().length));
    }
    final BigDecimal want =
        sum.divide(BigDecimal.valueOf(WINDOW_SIZE_SECONDS), MathContext.DECIMAL128);

    for (ImmutablePair<Instant, byte[]> pair : pairs) {
      estimator.update(pair.getLeft(), pair.getRight());
    }

    double actual = estimator.getFrom(Instant.ofEpochSecond(10));
    assertEquals(want.doubleValue(), actual, DELTA);

    // After window without updates the throughput should be zero
    final Instant afterWindowTimestamp =
        Instant.ofEpochSecond(
            TimestampConverter.toSeconds(lastUpdateTimestamp) + WINDOW_SIZE_SECONDS + 1);
    assertEquals(0D, estimator.getFrom(afterWindowTimestamp), DELTA);
  }

  @Test
  public void testThroughputIsAccumulatedWithin50SecondsWindow() {
    final List<ImmutablePair<Instant, byte[]>> excludedPairs = generateTestData(300, 0, 10);
    final List<ImmutablePair<Instant, byte[]>> expectedPairs = generateTestData(50, 10, 20);
    final List<ImmutablePair<Instant, byte[]>> pairs =
        Stream.concat(excludedPairs.stream(), expectedPairs.stream())
            .sorted(Comparator.comparing(ImmutablePair::getLeft))
            .collect(Collectors.toList());
    final Instant lastUpdateTimestamp = pairs.get(pairs.size() - 1).getLeft();

    BigDecimal sum = BigDecimal.valueOf(0L);
    for (ImmutablePair<Instant, byte[]> pair : expectedPairs) {
      sum = sum.add(BigDecimal.valueOf(pair.getRight().length));
    }
    final BigDecimal want =
        sum.divide(BigDecimal.valueOf(WINDOW_SIZE_SECONDS), MathContext.DECIMAL128);
    for (ImmutablePair<Instant, byte[]> pair : pairs) {
      estimator.update(pair.getLeft(), pair.getRight());
    }

    double actual = estimator.getFrom(Instant.ofEpochSecond(20));
    assertEquals(want.doubleValue(), actual, DELTA);

    // After window without updates the throughput should be zero
    final Instant afterWindowTimestamp =
        Instant.ofEpochSecond(
            TimestampConverter.toSeconds(lastUpdateTimestamp) + WINDOW_SIZE_SECONDS + 1);
    assertEquals(0D, estimator.getFrom(afterWindowTimestamp), DELTA);
  }

  private List<ImmutablePair<Instant, byte[]>> generateTestData(
      int size, int startSeconds, int endSeconds) {
    ThreadLocalRandom random = ThreadLocalRandom.current();
    List<ImmutablePair<Instant, byte[]>> pairs = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      int seconds = random.nextInt(endSeconds - startSeconds) + startSeconds;
      pairs.add(new ImmutablePair<>(Instant.ofEpochSecond(seconds), new byte[random.nextInt(100)]));
    }
    return pairs;
  }

  private static class TestCoder extends Coder<byte[]> {
    @Override
    public void encode(byte[] value, OutputStream outStream) throws IOException {
      outStream.write(value);
    }

    @Override
    public byte[] decode(InputStream inStream) throws IOException {
      return IOUtils.toByteArray(inStream);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {
      // NoOp
    }
  }
}
