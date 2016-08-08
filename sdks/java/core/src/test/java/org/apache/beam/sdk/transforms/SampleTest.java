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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.TestUtils.LINES;
import static org.apache.beam.sdk.TestUtils.NO_LINES;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static com.google.common.base.Preconditions.checkArgument;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.Joiner;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Tests for Sample transform.
 */
@RunWith(JUnit4.class)
public class SampleTest {
  static final Integer[] EMPTY = new Integer[] { };
  static final Integer[] DATA = new Integer[] {1, 2, 3, 4, 5};
  static final Integer[] REPEATED_DATA = new Integer[] {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};

  /**
   * Verifies that the result of a Sample operation contains the expected number of elements,
   * and that those elements are a subset of the items in expected.
   */
  @SuppressWarnings("rawtypes")
  public static class VerifyCorrectSample<T extends Comparable>
      implements SerializableFunction<Iterable<T>, Void> {
    private T[] expectedValues;
    private int expectedSize;

    /**
     * expectedSize is the number of elements that the Sample should contain. expected is the set
     * of elements that the sample may contain.
     */
    @SafeVarargs
    VerifyCorrectSample(int expectedSize, T... expected) {
      this.expectedValues = expected;
      this.expectedSize = expectedSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Void apply(Iterable<T> in) {
      List<T> actual = new ArrayList<>();
      for (T elem : in) {
        actual.add(elem);
      }

      assertEquals(expectedSize, actual.size());

      Collections.sort(actual);  // We assume that @expected is already sorted.
      int i = 0;  // Index into @expected
      for (T s : actual) {
        boolean matchFound = false;
        for (; i < expectedValues.length; i++) {
          if (s.equals(expectedValues[i])) {
            matchFound = true;
            break;
          }
        }
        assertTrue("Invalid sample: " +  Joiner.on(',').join(actual), matchFound);
        i++;  // Don't match the same element again.
      }
      return null;
    }
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSample() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(DATA)
        .withCoder(BigEndianIntegerCoder.of()));
    PCollection<Iterable<Integer>> output = input.apply(
        Sample.<Integer>fixedSizeGlobally(3));

    PAssert.thatSingletonIterable(output)
        .satisfies(new VerifyCorrectSample<>(3, DATA));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSampleEmpty() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(EMPTY)
        .withCoder(BigEndianIntegerCoder.of()));
    PCollection<Iterable<Integer>> output = input.apply(
        Sample.<Integer>fixedSizeGlobally(3));

    PAssert.thatSingletonIterable(output)
        .satisfies(new VerifyCorrectSample<>(0, EMPTY));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSampleZero() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(DATA)
        .withCoder(BigEndianIntegerCoder.of()));
    PCollection<Iterable<Integer>> output = input.apply(
        Sample.<Integer>fixedSizeGlobally(0));

    PAssert.thatSingletonIterable(output)
        .satisfies(new VerifyCorrectSample<>(0, DATA));
    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSampleInsufficientElements() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(DATA)
        .withCoder(BigEndianIntegerCoder.of()));
    PCollection<Iterable<Integer>> output = input.apply(
        Sample.<Integer>fixedSizeGlobally(10));

    PAssert.thatSingletonIterable(output)
        .satisfies(new VerifyCorrectSample<>(5, DATA));
    p.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleNegative() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(DATA)
        .withCoder(BigEndianIntegerCoder.of()));
    input.apply(Sample.<Integer>fixedSizeGlobally(-1));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSampleMultiplicity() {
    Pipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(REPEATED_DATA)
        .withCoder(BigEndianIntegerCoder.of()));
    // At least one value must be selected with multiplicity.
    PCollection<Iterable<Integer>> output = input.apply(
        Sample.<Integer>fixedSizeGlobally(6));

    PAssert.thatSingletonIterable(output)
        .satisfies(new VerifyCorrectSample<>(6, REPEATED_DATA));
    p.run();
  }

  private static class VerifyAnySample implements SerializableFunction<Iterable<String>, Void> {
    private final List<String> lines;
    private final int limit;
    private VerifyAnySample(List<String> lines, int limit) {
      this.lines = lines;
      this.limit = limit;
    }

    @Override
    public Void apply(Iterable<String> actualIter) {
      final int expectedSize = Math.min(limit, lines.size());

      // Make sure actual is the right length, and is a
      // subset of expected.
      List<String> actual = new ArrayList<>();
      for (String s : actualIter) {
        actual.add(s);
      }
      assertEquals(expectedSize, actual.size());
      Set<String> actualAsSet = new TreeSet<>(actual);
      Set<String> linesAsSet = new TreeSet<>(lines);
      assertEquals(actual.size(), actualAsSet.size());
      assertEquals(lines.size(), linesAsSet.size());
      assertTrue(linesAsSet.containsAll(actualAsSet));
      return null;
    }
  }

  void runPickAnyTest(final List<String> lines, int limit) {
    checkArgument(new HashSet<String>(lines).size() == lines.size(), "Duplicates are unsupported.");
    Pipeline p = TestPipeline.create();

    PCollection<String> input = p.apply(Create.of(lines)
        .withCoder(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(Sample.<String>any(limit));


    PAssert.that(output)
        .satisfies(new VerifyAnySample(lines, limit));

    p.run();
  }

  @Test
  @Category(RunnableOnService.class)
  public void testPickAny() {
    runPickAnyTest(LINES, 0);
    runPickAnyTest(LINES, LINES.size() / 2);
    runPickAnyTest(LINES, LINES.size() * 2);
  }

  @Test
  // Extra tests, not worth the time to run on the real service.
  @Category(NeedsRunner.class)
  public void testPickAnyMore() {
    runPickAnyTest(LINES, LINES.size() - 1);
    runPickAnyTest(LINES, LINES.size());
    runPickAnyTest(LINES, LINES.size() + 1);
  }

  @Test
  @Category(RunnableOnService.class)
  public void testPickAnyWhenEmpty() {
    runPickAnyTest(NO_LINES, 0);
    runPickAnyTest(NO_LINES, 1);
  }

  @Test
  public void testSampleGetName() {
    assertEquals("Sample.SampleAny", Sample.<String>any(1).getName());
  }

  @Test
  public void testDisplayData() {
    PTransform<?, ?> sampleAny = Sample.any(1234);
    DisplayData sampleAnyDisplayData = DisplayData.from(sampleAny);
    assertThat(sampleAnyDisplayData, hasDisplayItem("sampleSize", 1234));

    PTransform<?, ?> samplePerKey = Sample.fixedSizePerKey(2345);
    DisplayData perKeyDisplayData = DisplayData.from(samplePerKey);
    assertThat(perKeyDisplayData, hasDisplayItem("sampleSize", 2345));
  }
}
