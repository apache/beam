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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.beam.sdk.util.common.Reiterable;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the CoGbkResult. */
@RunWith(JUnit4.class)
public class CoGbkResultTest {

  @Test
  public void testLazyResults() {
    runLazyResult(0);
    runLazyResult(1);
    runLazyResult(3);
    runLazyResult(10);
  }

  public void runLazyResult(int cacheSize) {
    int valueLen = 7;
    TestUnionValues values = new TestUnionValues(0, 1, 0, 3, 0, 3, 3);
    CoGbkResult result = new CoGbkResult(createSchema(5), values, cacheSize);
    assertThat(values.maxPos(), equalTo(Math.min(cacheSize, valueLen)));
    assertThat(result.getAll(new TupleTag<>("tag0")), contains(0, 2, 4));
    assertThat(values.maxPos(), equalTo(valueLen));
    assertThat(result.getAll(new TupleTag<>("tag3")), contains(3, 5, 6));
    assertThat(result.getAll(new TupleTag<Integer>("tag2")), emptyIterable());
    assertThat(result.getOnly(new TupleTag<>("tag1")), equalTo(1));
    assertThat(result.getAll(new TupleTag<>("tag0")), contains(0, 2, 4));
  }

  @Test
  public void testCrazyIteration() {
    runCrazyIteration(new Random(), 1, 2, 3);
    runCrazyIteration(new Random(), 1, 5, 6, 7);
    runCrazyIteration(new Random(), 5, 2, 3);
    runCrazyIteration(new Random(), 5, 5, 6, 7);
  }

  public void runCrazyIteration(Random random, int numIterations, int... tagSizes) {
    int totalValues = 0;
    List<Integer> tags = new ArrayList<>();
    for (int tagNum = 0; tagNum < tagSizes.length; tagNum++) {
      totalValues += tagSizes[tagNum];
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
    TestUnionValues values = new TestUnionValues(tags.stream().mapToInt(i->i).toArray());
    CoGbkResult coGbkResult = new CoGbkResult(createSchema(tagSizes.length), values, 0);

    for (KV<Integer, TupleTag<Integer>> call : callOrder) {
      if (!iters.containsKey(call)) {
        iters.put(call, coGbkResult.getAll(call.getValue()).iterator());
        actual.put(call, new ArrayList<>());
      }
      actual.get(call).add(iters.get(call).next());
      iters.get(call).hasNext();
    }

    for (Map.Entry<KV<Integer, TupleTag<Integer>>, List<Integer>> result : actual.entrySet()) {
      assertThat(result.getValue(), contains(expected.get(result.getKey().getValue()).toArray()));
    }
  }

  private CoGbkResultSchema createSchema(int size) {
    List<TupleTag<?>> tags = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      tags.add(new TupleTag<Integer>("tag" + i));
    }
    return new CoGbkResultSchema(TupleTagList.of(tags));
  }

  private static class TestUnionValues implements Reiterable<RawUnionValue> {

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
    public Reiterator<RawUnionValue> iterator() {
      return iterator(0);
    }

    public Reiterator<RawUnionValue> iterator(final int start) {
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
          return iterator(pos);
        }
      };
    }
  }
}
