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
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.beam.sdk.coders.Coder;
import org.joda.time.Instant;
import org.junit.Test;

public class BytesThroughputEstimatorTest {
  private static final double DELTA = 1e-10;
  private final SizeEstimator<byte[]> sizeEstimator = new CoderSizeEstimator<>(new TestCoder());

  @Test
  public void testThroughputIsZeroWhenNothingsBeenRegistered() {
    BytesThroughputEstimator<byte[]> estimator =
        new BytesThroughputEstimator<>(sizeEstimator, Instant.now());
    assertEquals(0D, estimator.get().doubleValue(), DELTA);
  }

  @Test
  public void testThroughputCalculation() {
    BytesThroughputEstimator<byte[]> estimator =
        new BytesThroughputEstimator<>(sizeEstimator, 1, Instant.ofEpochSecond(0));
    estimator.update(Instant.ofEpochSecond(2), new byte[10]);
    estimator.update(Instant.ofEpochSecond(3), new byte[20]);
    estimator.update(Instant.ofEpochSecond(5), new byte[30]);
    estimator.update(Instant.ofEpochSecond(10), new byte[40]);
    // (10 + 20 + 30 + 40) / 10 sec window = 10
    assertEquals(10D, estimator.get().doubleValue(), DELTA);

    BytesThroughputEstimator<byte[]> estimator2 =
        new BytesThroughputEstimator<>(sizeEstimator, 1, Instant.ofEpochSecond(20));
    estimator2.update(Instant.ofEpochSecond(21), new byte[10]);
    estimator2.update(Instant.ofEpochSecond(22), new byte[20]);
    estimator2.update(Instant.ofEpochSecond(23), new byte[10]);
    estimator2.update(Instant.ofEpochSecond(30), new byte[40]);
    // (10 + 20 + 10 + 40) / 10 sec window = 8
    assertEquals(8D, estimator2.get().doubleValue(), DELTA);

    BytesThroughputEstimator<byte[]> estimator3 =
        new BytesThroughputEstimator<>(sizeEstimator, 1, Instant.ofEpochSecond(30));
    estimator3.update(Instant.ofEpochSecond(31), new byte[10]);
    estimator3.update(Instant.ofEpochSecond(40), new byte[40]);
    // (10 + 40) / 10 sec window = 5
    assertEquals(5D, estimator3.get().doubleValue(), DELTA);
  }

  @Test
  public void testThroughputIsAccumulatedWithin60SecondsWindow() {
    List<ImmutablePair<Instant, byte[]>> pairs = generateTestData(100, 0, 11);
    pairs.sort(Comparator.comparing(ImmutablePair::getLeft));

    BigDecimal sum = BigDecimal.valueOf(0L);
    for (ImmutablePair<Instant, byte[]> pair : pairs) {
      sum = sum.add(BigDecimal.valueOf(pair.getRight().length));
    }
    final BigDecimal want = sum.divide(BigDecimal.valueOf(10), MathContext.DECIMAL128);

    BytesThroughputEstimator<byte[]> estimator =
        new BytesThroughputEstimator<>(sizeEstimator, 1, Instant.ofEpochSecond(0));
    for (ImmutablePair<Instant, byte[]> pair : pairs) {
      estimator.update(pair.getLeft(), pair.getRight());
    }

    double actual = estimator.get().doubleValue();
    assertEquals(want.doubleValue(), actual, 1);
  }

  @Test
  public void testThroughputHandlesNoTimeDifference() {
    BytesThroughputEstimator<byte[]> estimator =
        new BytesThroughputEstimator<>(sizeEstimator, 1, Instant.ofEpochSecond(0));
    estimator.update(Instant.ofEpochSecond(0), new byte[10]);
    // (10 / 1) * 1000 because we round up to one millisecond
    assertEquals(10000D, estimator.get().doubleValue(), DELTA);
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
