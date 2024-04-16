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
package org.apache.beam.sdk.transforms.join;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests the CoGbkResult. */
@RunWith(Parameterized.class)
public class CoGbkResultTest {

  @Parameter(0)
  public boolean useReiterator;

  @Parameterized.Parameters(name = "{index}: Test with usesReiterable={0}")
  public static Collection<Object[]> data() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  private static final Logger LOG = LoggerFactory.getLogger(CoGbkResultTest.class);

  private static final int TEST_CACHE_SIZE = 5;

  @Test
  public void testExpectedResults() {
    runExpectedResult(0);
    runExpectedResult(1);
    runExpectedResult(3);
    runExpectedResult(10);
  }

  public void runExpectedResult(int cacheSize) {
    int valueLen = 7;
    TestUnionValues values = new TestUnionValues(0, 1, 0, 3, 0, 3, 3);
    CoGbkResult result = new CoGbkResult(createSchema(5), values, cacheSize, 0);
    assertThat(values.maxPos(), equalTo(Math.min(cacheSize, valueLen)));
    assertThat(result.getAll(new TupleTag<>("tag0")), contains(0, 2, 4));
    assertThat(values.maxPos(), equalTo(valueLen));
    assertThat(result.getAll(new TupleTag<>("tag3")), contains(3, 5, 6));
    assertThat(result.getAll(new TupleTag<Integer>("tag2")), emptyIterable());
    assertThat(result.getOnly(new TupleTag<>("tag1")), equalTo(1));
    assertThat(result.getAll(new TupleTag<>("tag0")), contains(0, 2, 4));
  }

  @Test
  public void testLazyResults() {
    TestUnionValues values = new TestUnionValues(0, 0, 1, 1, 0, 1, 1);
    CoGbkResult result = new CoGbkResult(createSchema(5), values, 0, 2);
    // Nothing is read until we try to iterate.
    assertThat(values.maxPos(), equalTo(0));
    Iterable<?> tag0iterable = result.getAll("tag0");
    assertThat(values.maxPos(), equalTo(0));
    Iterator<?> ignored = tag0iterable.iterator();
    assertThat(values.maxPos(), equalTo(0));

    // Iterating reads (nearly) the minimal number of values.
    Iterator<?> tag0 = tag0iterable.iterator();
    tag0.next();
    assertThat(values.maxPos(), lessThanOrEqualTo(2));
    tag0.next();
    assertThat(values.maxPos(), equalTo(2));
    // Note that we're skipping over tag 1.
    tag0.next();
    assertThat(values.maxPos(), equalTo(5));

    // Iterating again does not cause more reads.
    Iterator<?> tag0iterAgain = tag0iterable.iterator();
    tag0iterAgain.next();
    tag0iterAgain.next();
    tag0iterAgain.next();
    assertThat(values.maxPos(), equalTo(5));

    // Iterating over other tags does not cause more reads for values we have seen.
    Iterator<?> tag1 = result.getAll("tag1").iterator();
    tag1.next();
    tag1.next();
    assertThat(values.maxPos(), equalTo(5));
    // However, finding the next tag1 value does require more reads.
    tag1.next();
    assertThat(values.maxPos(), equalTo(6));
  }

  @Test
  @SuppressWarnings("BoxedPrimitiveEquality")
  public void testCachedResults() {
    // The caching strategies are different for the different implementations.
    Assume.assumeTrue(useReiterator);

    // Ensure we don't fail below due to a non-default java.lang.Integer.IntegerCache.high setting,
    // as we want to test our cache is working as expected, unimpeded by a higher-level cache.
    int integerCacheLimit = 128;
    assertThat(
        Integer.valueOf(integerCacheLimit), not(sameInstance(Integer.valueOf(integerCacheLimit))));

    int perTagCache = 10;
    int crossTagCache = 2 * integerCacheLimit;
    int[] tags = new int[crossTagCache + 8 * perTagCache];
    for (int i = 0; i < 2 * perTagCache; i++) {
      tags[crossTagCache + 4 * i] = 1;
      tags[crossTagCache + 4 * i + 1] = 2;
    }

    TestUnionValues values = new TestUnionValues(tags);
    CoGbkResult result = new CoGbkResult(createSchema(5), values, crossTagCache, perTagCache);

    // More that perTagCache values should be cached for the first tag, as they came first.
    List<Object> tag0 = Lists.newArrayList(result.getAll("tag0").iterator());
    List<Object> tag0again = Lists.newArrayList(result.getAll("tag0").iterator());
    assertThat(tag0.get(0), sameInstance(tag0again.get(0)));
    assertThat(tag0.get(integerCacheLimit), sameInstance(tag0again.get(integerCacheLimit)));
    assertThat(tag0.get(crossTagCache - 1), sameInstance(tag0again.get(crossTagCache - 1)));
    // However, not all elements are cached.
    assertThat(tag0.get(tag0.size() - 1), not(sameInstance(tag0again.get(tag0.size() - 1))));

    // For tag 1 and tag 2, we cache perTagCache elements, plus possibly one more due to peeking
    // iterators.
    List<Object> tag1 = Lists.newArrayList(result.getAll("tag1").iterator());
    List<Object> tag1again = Lists.newArrayList(result.getAll("tag1").iterator());
    assertThat(tag1.get(0), sameInstance(tag1again.get(0)));
    assertThat(tag1.get(perTagCache - 1), sameInstance(tag1again.get(perTagCache - 1)));
    assertThat(tag1.get(perTagCache + 1), not(sameInstance(tag1again.get(perTagCache + 1))));

    List<Object> tag2 = Lists.newArrayList(result.getAll("tag1").iterator());
    List<Object> tag2again = Lists.newArrayList(result.getAll("tag1").iterator());
    assertThat(tag2.get(0), sameInstance(tag2again.get(0)));
    assertThat(tag2.get(perTagCache - 1), sameInstance(tag2again.get(perTagCache - 1)));
    assertThat(tag2.get(perTagCache + 1), not(sameInstance(tag2again.get(perTagCache + 1))));
  }

  @Test
  public void testSingleTag() {
    runCrazyIteration(1, TEST_CACHE_SIZE / 2);
    runCrazyIteration(1, TEST_CACHE_SIZE * 2);
    runCrazyIteration(2, TEST_CACHE_SIZE / 2);
    runCrazyIteration(2, TEST_CACHE_SIZE * 2);
    runCrazyIteration(10, TEST_CACHE_SIZE * 10);
  }

  @Test
  public void testTwoTags() {
    runCrazyIteration(1, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE / 2);
    runCrazyIteration(1, TEST_CACHE_SIZE * 2, TEST_CACHE_SIZE * 2);
    runCrazyIteration(2, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE / 2);
    runCrazyIteration(2, TEST_CACHE_SIZE * 2, TEST_CACHE_SIZE * 2);
    runCrazyIteration(10, TEST_CACHE_SIZE * 10);
  }

  @Test
  public void testLargeSmall() {
    runCrazyIteration(1, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE * 2);
    runCrazyIteration(1, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE * 20);
    runCrazyIteration(2, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE * 20);
    runCrazyIteration(10, TEST_CACHE_SIZE / 2, TEST_CACHE_SIZE * 20);
  }

  @Test
  public void testManyTags() {
    runCrazyIteration(1, 2, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
    runCrazyIteration(2, 2, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
    runCrazyIteration(10, 2, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
  }

  public void runCrazyIteration(int numIterations, int... tagSizes) {
    for (int trial = 0; trial < 10; trial++) {
      // Populate this with a constant to reproduce failures.
      int seed = (int) (Integer.MAX_VALUE * Math.random());
      LOG.info("Running " + Arrays.toString(tagSizes) + " with seed " + seed);
      Random random = new Random(seed);
      List<Integer> tags = new ArrayList<>();
      for (int tagNum = 0; tagNum < tagSizes.length; tagNum++) {
        for (int i = 0; i < tagSizes[tagNum]; i++) {
          tags.add(tagNum);
        }
      }
      Collections.shuffle(tags, random);

      Map<TupleTag<Integer>, List<Integer>> expected = new HashMap<>();
      for (int tagNum = 0; tagNum < tagSizes.length; tagNum++) {
        expected.put(new TupleTag<>("tag" + tagNum), new ArrayList<>());
      }
      for (int i = 0; i < tags.size(); i++) {
        expected.get(new TupleTag<>("tag" + tags.get(i))).add(i);
      }

      List<KV<Integer, TupleTag<Integer>>> callOrder = new ArrayList<>();
      for (int i = 0; i < numIterations; i++) {
        for (int tagNum = 0; tagNum < tagSizes.length; tagNum++) {
          for (int k = 0; k < tagSizes[tagNum]; k++) {
            callOrder.add(KV.of(i, new TupleTag<>("tag" + tagNum)));
          }
        }
      }
      Collections.shuffle(callOrder, random);

      Map<KV<Integer, TupleTag<Integer>>, Iterator<Integer>> iters = new HashMap<>();
      Map<KV<Integer, TupleTag<Integer>>, List<Integer>> actual = new HashMap<>();
      TestUnionValues values = new TestUnionValues(tags.stream().mapToInt(i -> i).toArray());
      CoGbkResult coGbkResult =
          new CoGbkResult(createSchema(tagSizes.length), values, 0, TEST_CACHE_SIZE);

      for (KV<Integer, TupleTag<Integer>> call : callOrder) {
        if (!iters.containsKey(call)) {
          iters.put(call, coGbkResult.getAll(call.getValue()).iterator());
          actual.put(call, new ArrayList<>());
        }
        actual.get(call).add(iters.get(call).next());
        if (random.nextDouble() < 0.5 / numIterations) {
          Boolean ignored = iters.get(call).hasNext();
        }
      }

      for (Map.Entry<KV<Integer, TupleTag<Integer>>, List<Integer>> result : actual.entrySet()) {
        assertThat(result.getValue(), contains(expected.get(result.getKey().getValue()).toArray()));
      }
    }
  }

  private CoGbkResultSchema createSchema(int size) {
    List<TupleTag<?>> tags = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      tags.add(new TupleTag<Integer>("tag" + i));
    }
    return new CoGbkResultSchema(TupleTagList.of(tags));
  }

  private class TestUnionValues implements Iterable<RawUnionValue> {

    final int[] tags;
    int maxPos = 0;

    /**
     * This will create a list of RawUnionValues whose tags are as given and values are increasing
     * starting at 0 (i.e. the index in the constructor).
     */
    public TestUnionValues(int... tags) {
      this.tags = tags;
    }

    /** Returns the highest position iterated to so far, useful for ensuring laziness. */
    public int maxPos() {
      return maxPos;
    }

    @Override
    public Iterator<RawUnionValue> iterator() {
      return iterator(0);
    }

    public Iterator<RawUnionValue> iterator(final int start) {
      if (useReiterator) {
        return new Reiterator<RawUnionValue>() {
          int pos = start;

          @Override
          public boolean hasNext() {
            return pos < tags.length;
          }

          @Override
          public RawUnionValue next() {
            maxPos = Math.max(pos + 1, maxPos);
            return new RawUnionValue(tags[pos], pos++);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }

          @Override
          public Reiterator<RawUnionValue> copy() {
            return (Reiterator<RawUnionValue>) iterator(pos);
          }
        };
      } else {

        return new Iterator<RawUnionValue>() {
          int pos = start;

          @Override
          public boolean hasNext() {
            return pos < tags.length;
          }

          @Override
          public RawUnionValue next() {
            maxPos = Math.max(pos + 1, maxPos);
            return new RawUnionValue(tags[pos], pos++);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    }
  }
}
