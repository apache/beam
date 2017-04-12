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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

/**
 * Tests for Sample transform.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    SampleTest.PickAnyTest.class,
    SampleTest.MiscTest.class
})
public class SampleTest {
  private static final Integer[] EMPTY = new Integer[] { };
  private static final Integer[] DATA = new Integer[] {1, 2, 3, 4, 5};
  private static final Integer[] REPEATED_DATA = new Integer[] {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};

  /**
   * Test variations for Sample transform.
   */
  @RunWith(Parameterized.class)
  public static class PickAnyTest {
    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    @Parameterized.Parameters(name = "limit_{1}")
    public static Iterable<Object[]> data() throws IOException {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {
                  TestUtils.NO_LINES,
                  0
              },
              new Object[] {
                  TestUtils.NO_LINES,
                  1
              },
              new Object[] {
                  TestUtils.LINES,
                  1
              },
              new Object[] {
                  TestUtils.LINES,
                  TestUtils.LINES.size() / 2
              },
              new Object[] {
                  TestUtils.LINES,
                  TestUtils.LINES.size() * 2
              },
              new Object[] {
                  TestUtils.LINES,
                  TestUtils.LINES.size() - 1
              },
              new Object[] {
                  TestUtils.LINES,
                  TestUtils.LINES.size()
              },
              new Object[] {
                  TestUtils.LINES,
                  TestUtils.LINES.size() + 1
              }
              )
          .build();
    }

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public List<String> lines;

    @Parameterized.Parameter(1)
    public int limit;

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
      checkArgument(new HashSet<String>(lines).size() == lines.size(),
                    "Duplicates are unsupported.");

      PCollection<String> input = p.apply(Create.of(lines)
                                                .withCoder(StringUtf8Coder.of()));

      PCollection<String> output =
          input.apply(Sample.<String>any(limit));


      PAssert.that(output)
             .satisfies(new VerifyAnySample(lines, limit));

      p.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testPickAny() {
      runPickAnyTest(lines, limit);
    }
  }

  /**
   * Further tests for Sample transform.
   */
  @RunWith(JUnit4.class)
  public static class MiscTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

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

      /**
       * expectedSize is the number of elements that the Sample should contain. expected is the set
       * of elements that the sample may contain.
       */
      VerifyCorrectSample(int expectedSize, Collection<T> expected) {
        this.expectedValues = (T[]) expected.toArray();
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
    @Category(ValidatesRunner.class)
    public void testSample() {

      PCollection<Integer> input =
          pipeline.apply(
              Create.of(ImmutableList.copyOf(DATA)).withCoder(BigEndianIntegerCoder.of()));
      PCollection<Iterable<Integer>> output = input.apply(Sample.<Integer>fixedSizeGlobally(3));

      PAssert.thatSingletonIterable(output)
             .satisfies(new VerifyCorrectSample<>(3, DATA));
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSampleEmpty() {

      PCollection<Integer> input = pipeline.apply(Create.empty(BigEndianIntegerCoder.of()));
      PCollection<Iterable<Integer>> output = input.apply(
          Sample.<Integer>fixedSizeGlobally(3));

      PAssert.thatSingletonIterable(output)
             .satisfies(new VerifyCorrectSample<>(0, EMPTY));
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSampleZero() {

      PCollection<Integer> input = pipeline.apply(Create.of(ImmutableList.copyOf(DATA))
                                                        .withCoder(BigEndianIntegerCoder.of()));
      PCollection<Iterable<Integer>> output = input.apply(
          Sample.<Integer>fixedSizeGlobally(0));

      PAssert.thatSingletonIterable(output)
             .satisfies(new VerifyCorrectSample<>(0, DATA));
      pipeline.run();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSampleInsufficientElements() {

      PCollection<Integer> input =
          pipeline.apply(
              Create.of(ImmutableList.copyOf(DATA)).withCoder(BigEndianIntegerCoder.of()));
      PCollection<Iterable<Integer>> output = input.apply(
          Sample.<Integer>fixedSizeGlobally(10));

      PAssert.thatSingletonIterable(output)
             .satisfies(new VerifyCorrectSample<>(5, DATA));
      pipeline.run();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSampleNegative() {
      pipeline.enableAbandonedNodeEnforcement(false);

      PCollection<Integer> input =
          pipeline.apply(
              Create.of(ImmutableList.copyOf(DATA)).withCoder(BigEndianIntegerCoder.of()));
      input.apply(Sample.<Integer>fixedSizeGlobally(-1));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testSampleMultiplicity() {

      PCollection<Integer> input =
          pipeline.apply(
              Create.of(ImmutableList.copyOf(REPEATED_DATA)).withCoder(BigEndianIntegerCoder.of()));
      // At least one value must be selected with multiplicity.
      PCollection<Iterable<Integer>> output = input.apply(
          Sample.<Integer>fixedSizeGlobally(6));

      PAssert.thatSingletonIterable(output)
             .satisfies(new VerifyCorrectSample<>(6, REPEATED_DATA));
      pipeline.run();
    }

    @Test
    public void testSampleGetName() {
      assertEquals("Sample.Any", Sample.<String>any(1).getName());
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
}
