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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;

public class ThroughputEstimatorTest {
  private static final double DELTA = 1e-10;
  private ThroughputEstimator estimator;

  @Before
  public void setup() {
    estimator = new ThroughputEstimator();
  }

  @Test
  public void testThroughputCalculation() {
    estimator.update(Timestamp.ofTimeSecondsAndNanos(20, 0), 10);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(30, 0), 20);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(59, 0), 30);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(60, 0), 40); // Exclusive
    assertEquals(20D, estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(61, 0)), DELTA);

    estimator.update(Timestamp.ofTimeSecondsAndNanos(100, 0), 10);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(110, 0), 20);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(110, 0), 10);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(140, 0), 40); // Exclusive
    assertEquals(20D, estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(141, 0)), DELTA);

    estimator.update(Timestamp.ofTimeSecondsAndNanos(201, 0), 10);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(250, 0), 40); // Exclusive
    assertEquals(10D, estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(261, 0)), DELTA);

    assertEquals(0D, estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(350, 0)), DELTA);
  }

  @Test
  public void testThroughputIsAccumulatedWithin60SecondsWindow() {
    List<ImmutablePair<Timestamp, Long>> pairs = generateTestData(100, 0, 60, Long.MAX_VALUE);
    pairs.sort((a, b) -> a.getLeft().compareTo(b.getLeft()));

    final long count = pairs.stream().map(ImmutablePair::getLeft).distinct().count();
    BigDecimal sum = BigDecimal.valueOf(0L);
    for (ImmutablePair<Timestamp, Long> pair : pairs) {
      sum = sum.add(BigDecimal.valueOf(pair.getRight()));
    }
    final BigDecimal want = sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);

    for (int i = 0; i < pairs.size(); i++) {
      estimator.update(pairs.get(i).getLeft(), pairs.get(i).getRight());
    }

    // This is needed to push the current window into the queue.
    estimator.update(Timestamp.ofTimeSecondsAndNanos(60, 0), 10);
    double actual = estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(60, 0));
    assertEquals(want.doubleValue(), actual, DELTA);
  }

  @Test
  public void testThroughputIsAccumulatedWithin300SecondsWindow() {
    List<ImmutablePair<Timestamp, Long>> excludedPairs =
        generateTestData(300, 0, 240, Long.MAX_VALUE);
    List<ImmutablePair<Timestamp, Long>> expectedPairs =
        generateTestData(50, 240, 300, Long.MAX_VALUE);
    List<ImmutablePair<Timestamp, Long>> pairs =
        Stream.concat(excludedPairs.stream(), expectedPairs.stream()).collect(Collectors.toList());
    pairs.sort((a, b) -> a.getLeft().compareTo(b.getLeft()));

    final long count = expectedPairs.stream().map(ImmutablePair::getLeft).distinct().count();
    BigDecimal sum = BigDecimal.valueOf(0L);
    for (ImmutablePair<Timestamp, Long> pair : expectedPairs) {
      sum = sum.add(BigDecimal.valueOf(pair.getRight()));
    }
    final BigDecimal want = sum.divide(BigDecimal.valueOf(count), MathContext.DECIMAL128);
    for (int i = 0; i < pairs.size(); i++) {
      estimator.update(pairs.get(i).getLeft(), pairs.get(i).getRight());
    }

    // This is needed to push the current window into the queue.
    estimator.update(Timestamp.ofTimeSecondsAndNanos(300, 0), 10);
    double actual = estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(300, 0));
    assertEquals(want.doubleValue(), actual, DELTA);
  }

  @Test
  public void testThroughputShouldNotBeNegative() {
    estimator.update(Timestamp.ofTimeSecondsAndNanos(0, 0), -10);
    estimator.update(Timestamp.ofTimeSecondsAndNanos(1, 0), 10);
    double actual = estimator.getFrom(Timestamp.ofTimeSecondsAndNanos(0, 0));
    assertEquals(0D, actual, DELTA);
  }

  private List<ImmutablePair<Timestamp, Long>> generateTestData(
      int size, int startSeconds, int endSeconds, long maxBytes) {
    Random random = new Random();
    List<ImmutablePair<Timestamp, Long>> pairs = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      int seconds = random.nextInt(endSeconds - startSeconds) + startSeconds;
      pairs.add(
          new ImmutablePair<>(
              Timestamp.ofTimeSecondsAndNanos(seconds, 0),
              ThreadLocalRandom.current().nextLong(maxBytes)));
    }
    return pairs;
  }
}
