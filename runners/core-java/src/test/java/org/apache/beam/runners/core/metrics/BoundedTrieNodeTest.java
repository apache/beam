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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrie;
import org.apache.beam.runners.core.metrics.BoundedTrieData.BoundedTrieNode;
import org.apache.beam.sdk.metrics.BoundedTrieResult;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link BoundedTrieNode}. */
@RunWith(JUnit4.class)
public class BoundedTrieNodeTest {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedTrieNodeTest.class);

  /**
   * Generates {@code n} random segments with a fixed depth.
   *
   * @param n The number of segments to generate.
   * @param depth The depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   * @param rand A random number generator.
   * @return A list of segments.
   */
  private static List<List<String>> generateSegmentsFixedDepth(
      int n, int depth, double overlap, Random rand) {
    if (depth == 0) {
      return Collections.nCopies(n, Collections.emptyList());
    } else {
      List<List<String>> result = new ArrayList<>();
      List<String> seen = new ArrayList<>();
      for (List<String> suffix : generateSegmentsFixedDepth(n, depth - 1, overlap, rand)) {
        String prefix;
        if (seen.isEmpty() || rand.nextDouble() > overlap) {
          prefix = String.valueOf((char) ('a' + seen.size()));
          seen.add(prefix);
        } else {
          prefix = seen.get(rand.nextInt(seen.size()));
        }
        List<String> newSegments = new ArrayList<>();
        newSegments.add(prefix);
        newSegments.addAll(suffix);
        result.add(newSegments);
      }
      return result;
    }
  }

  /**
   * Generates {@code n} random segments with a depth between {@code minDepth} and {@code maxDepth}.
   *
   * @param n The number of segments to generate.
   * @param minDepth The minimum depth of each segment.
   * @param maxDepth The maximum depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   * @param rand A random number generator.
   * @return A list of segments.
   */
  private static List<List<String>> randomSegments(
      int n, int minDepth, int maxDepth, double overlap, Random rand) {
    List<List<String>> result = new ArrayList<>();
    List<Integer> depths = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      depths.add(minDepth + (i % (maxDepth - minDepth + 1)));
    }
    Iterator<Integer> depthIterator = depths.iterator();
    for (List<String> segments : generateSegmentsFixedDepth(n, maxDepth, overlap, rand)) {
      int depth = depthIterator.next();
      result.add(segments.subList(0, depth));
    }
    return result;
  }

  /**
   * Asserts that the given {@link BoundedTrieNode} covers the expected segments.
   *
   * @param node The {@link BoundedTrieNode} to check.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void assertCovers(BoundedTrieNode node, Set<List<String>> expected, int maxTruncated) {
    assertCoversFlattened(node.flattened(), expected, maxTruncated);
  }

  /**
   * Verifies that the flattened list of segments covers the expected segments.
   *
   * @param flattened The flattened list of segments.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void assertCoversFlattened(
      List<List<String>> flattened, Set<List<String>> expected, int maxTruncated) {
    Set<List<String>> exactSegments = new HashSet<>();
    Set<List<String>> truncatedSegments = new HashSet<>();
    for (List<String> entry : flattened) {
      List<String> segments = new ArrayList<>(entry.subList(0, entry.size() - 1));
      String last = entry.get(entry.size() - 1);
      if (Boolean.parseBoolean(last)) {
        truncatedSegments.add(segments);
      } else {
        exactSegments.add(segments);
      }
    }

    assertTrue(
        "Exact segments set should not be larger than expected set",
        exactSegments.size() <= expected.size());
    assertTrue(
        "Expected set should contain all exact segments", expected.containsAll(exactSegments));

    assertTrue(
        "Number of truncated segments should not exceed maxTruncated",
        truncatedSegments.size() <= maxTruncated);

    Set<List<String>> seenTruncated = new HashSet<>();
    for (List<String> segments : expected) {
      if (!exactSegments.contains(segments)) {
        int found = 0;
        for (int i = 0; i < segments.size(); i++) {
          if (truncatedSegments.contains(segments.subList(0, i))) {
            seenTruncated.add(segments.subList(0, i));
            found++;
          }
        }
        assertEquals(
            String.format(
                "Expected exactly one prefix of %s to occur in %s, found %s",
                segments, truncatedSegments, found),
            1,
            found);
      }
    }

    assertEquals(
        "Seen truncated segments should match the truncated segments set",
        seenTruncated,
        truncatedSegments);
  }

  /**
   * Runs a test case for the {@link #assertCoversFlattened} method.
   *
   * @param flattened The flattened list of segments.
   * @param expected The expected segments.
   * @param maxTruncated The maximum number of truncated segments allowed.
   */
  private void runCoversTest(List<String> flattened, List<String> expected, int maxTruncated) {
    List<List<String>> parsedFlattened =
        flattened.stream()
            .map(
                s -> {
                  List<String> result =
                      new ArrayList<>(Arrays.asList(s.replace("*", "").split("")));
                  result.add(s.endsWith("*") ? Boolean.TRUE.toString() : Boolean.FALSE.toString());
                  return result;
                })
            .collect(Collectors.toList());
    Set<List<String>> parsedExpected =
        expected.stream().map(s -> Arrays.asList(s.split(""))).collect(Collectors.toSet());
    assertCoversFlattened(parsedFlattened, parsedExpected, maxTruncated);
  }

  private Set<List<String>> everythingDeduped(Set<List<String>> everything) {
    Set<List<String>> allPrefixes = new HashSet<>();
    for (List<String> segments : everything) {
      for (int i = 0; i < segments.size(); i++) {
        allPrefixes.add(segments.subList(0, i));
      }
    }
    Set<List<String>> everythingDeduped = new HashSet<>(everything);
    everythingDeduped.removeAll(allPrefixes);
    return everythingDeduped;
  }

  /**
   * Runs a test case for the {@link BoundedTrieNode} class.
   *
   * @param toAdd The segments to add to the {@link BoundedTrieNode}.
   */
  private void runTest(List<List<String>> toAdd) {
    Set<List<String>> everything = new HashSet<>(toAdd);
    Set<List<String>> everythingDeduped = everythingDeduped(everything);

    // Test basic addition.
    BoundedTrieNode node = new BoundedTrieNode();
    int initialSize = node.getSize();
    assertEquals(1, initialSize);

    int totalSize = initialSize;
    for (List<String> segments : everything) {
      totalSize += node.add(segments);
    }

    assertEquals(everythingDeduped.size(), node.getSize());
    assertEquals(totalSize, node.getSize());
    assertCovers(node, everythingDeduped, 0);

    // Test merging.
    BoundedTrieNode node0 = new BoundedTrieNode();
    BoundedTrieNode node1 = new BoundedTrieNode();
    int i = 0;
    for (List<String> segments : everything) {
      if (i % 2 == 0) {
        node0.add(segments);
      } else {
        node1.add(segments);
      }
      i++;
    }
    int preMergeSize = node0.getSize();
    int mergeDelta = node0.merge(node1);
    assertEquals(preMergeSize + mergeDelta, node0.getSize());
    assertEquals(node0, node);

    // Test trimming.
    int trimDelta = 0;
    if (node.getSize() > 1) {
      trimDelta = node.trim();
      assertTrue(trimDelta < 0);
      assertEquals(totalSize + trimDelta, node.getSize());
      assertCovers(node, everythingDeduped, 1);
    }

    if (node.getSize() > 1) {
      int trim2Delta = node.trim();
      assertTrue(trim2Delta < 0);
      assertEquals(totalSize + trimDelta + trim2Delta, node.getSize());
      assertCovers(node, everythingDeduped, 2);
    }

    // Verify adding after trimming is a no-op.
    BoundedTrieNode nodeCopy = node.deepCopy();
    for (List<String> segments : everything) {
      assertEquals(0, node.add(segments));
    }
    assertEquals(node, nodeCopy);

    // Verify merging after trimming is a no-op.
    assertEquals(0, node.merge(node0));
    assertEquals(0, node.merge(node1));
    assertEquals(node, nodeCopy);

    // Test adding new values.
    int expectedDelta = node.isTruncated() ? 0 : 2;
    List<List<String>> newValues =
        Arrays.asList(Collections.singletonList("new1"), Arrays.asList("new2", "new2.1"));
    assertEquals(expectedDelta, node.addAll(newValues));

    Set<List<String>> expectedWithNewValues = new HashSet<>(everythingDeduped);
    expectedWithNewValues.addAll(newValues);
    assertCovers(node, expectedWithNewValues, 2);

    // Test merging new values.
    BoundedTrieNode newValuesNode = new BoundedTrieNode();
    newValuesNode.addAll(newValues);
    assertCovers(newValuesNode, new HashSet<>(newValues), 0);
    assertEquals(expectedDelta, nodeCopy.merge(newValuesNode));
    assertCovers(nodeCopy, expectedWithNewValues, 2);
    // adding after merge should not change previous node on which this was merged
    List<String> additionalValue = Arrays.asList("new3", "new3.1");
    expectedDelta = newValuesNode.isTruncated() ? 0 : 1;
    assertEquals(expectedDelta, newValuesNode.add(additionalValue));
    // previous node on which the merge was done should have remained same
    assertCovers(nodeCopy, expectedWithNewValues, 2);
    // the newValuesNode should have changed
    Set<List<String>> updatedNewValues = new HashSet<>(newValues);
    updatedNewValues.add(additionalValue);
    assertCovers(newValuesNode, updatedNewValues, 0);
  }

  /**
   * Fuzzy segment generator for testing {@link BoundedTrieNode} class.
   *
   * @param iterations The number of iterations to run.
   * @param n The number of segments to generate for each iteration.
   * @param minDepth The minimum depth of each segment.
   * @param maxDepth The maximum depth of each segment.
   * @param overlap The probability that a segment will share a prefix with a previous segment.
   */
  private void runFuzz(int iterations, int n, int minDepth, int maxDepth, double overlap) {
    for (int i = 0; i < iterations; i++) {
      long seed = new Random().nextLong();
      Random rand = new Random(seed);
      List<List<String>> segments = randomSegments(n, minDepth, maxDepth, overlap, rand);
      try {
        runTest(segments);
      } catch (AssertionError e) {
        LOG.info("SEED: {}", seed);
        throw e;
      }
    }
  }

  @Test
  public void testTrivial() {
    runTest(Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("a", "c")));
  }

  @Test
  public void testFlat() {
    runTest(
        Arrays.asList(Arrays.asList("a", "a"), Arrays.asList("b", "b"), Arrays.asList("c", "c")));
  }

  @Test
  public void testDeep() {
    runTest(Arrays.asList(Collections.nCopies(10, "a"), Collections.nCopies(12, "b")));
  }

  @Test
  public void testSmall() {
    runFuzz(10, 5, 2, 3, 0.5);
  }

  @Test
  public void testMedium() {
    runFuzz(10, 20, 2, 4, 0.5);
  }

  @Test
  public void testLargeSparse() {
    runFuzz(10, 120, 2, 4, 0.2);
  }

  @Test
  public void testLargeDense() {
    runFuzz(10, 120, 2, 4, 0.8);
  }

  @Test
  public void testCoversExact() {
    runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ab", "ac", "cd"), 0);

    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ac", "cd"), 0));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac"), Arrays.asList("ab", "ac", "cd"), 0));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "cd"), 0));
  }

  @Test
  public void testCoversTruncated() {
    runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "cd"), 1);
    runCoversTest(Arrays.asList("a*", "cd"), Arrays.asList("ab", "ac", "abcde", "cd"), 1);

    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac", "cd"), Arrays.asList("ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("ab", "ac"), Arrays.asList("ab", "ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "c*"), Arrays.asList("ab", "ac", "cd"), 1));
    assertThrows(
        AssertionError.class,
        () -> runCoversTest(Arrays.asList("a*", "c*"), Arrays.asList("ab", "ac"), 1));
  }

  @Test
  public void testBoundedTrieDataCombine() {
    BoundedTrieData empty = new BoundedTrieData();
    BoundedTrieData singletonA = new BoundedTrieData(ImmutableList.of("a", "a"));
    BoundedTrieData singletonB = new BoundedTrieData(ImmutableList.of("b", "b"));
    BoundedTrieNode lotsRoot = new BoundedTrieNode();
    lotsRoot.addAll(Arrays.asList(Arrays.asList("c", "c"), Arrays.asList("d", "d")));
    BoundedTrieData lots = new BoundedTrieData(lotsRoot);

    assertEquals(BoundedTrieResult.empty(), empty.extractResult());
    empty = empty.combine(singletonA);
    assertEquals(
        BoundedTrieResult.create(ImmutableSet.of(Arrays.asList("a", "a", String.valueOf(false)))),
        empty.extractResult());
    singletonA = singletonA.combine(empty);
    assertEquals(
        BoundedTrieResult.create(ImmutableSet.of(Arrays.asList("a", "a", String.valueOf(false)))),
        singletonA.extractResult());
    singletonA = singletonA.combine(singletonB);
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "b", String.valueOf(false)))),
        singletonA.extractResult());
    singletonA = singletonA.combine(lots);
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "b", String.valueOf(false)),
                Arrays.asList("c", "c", String.valueOf(false)),
                Arrays.asList("d", "d", String.valueOf(false)))),
        singletonA.extractResult());
    lots = lots.combine(singletonA);
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", "a", String.valueOf(false)),
                Arrays.asList("b", "b", String.valueOf(false)),
                Arrays.asList("c", "c", String.valueOf(false)),
                Arrays.asList("d", "d", String.valueOf(false)))),
        lots.extractResult());
  }

  @Test
  public void testBoundedTrieDataCombineTrim() {
    BoundedTrieNode left = new BoundedTrieNode();
    left.addAll(Arrays.asList(Arrays.asList("a", "x"), Arrays.asList("b", "d")));
    BoundedTrieNode right = new BoundedTrieNode();
    right.addAll(Arrays.asList(Arrays.asList("a", "y"), Arrays.asList("c", "d")));

    BoundedTrieData mainTree = new BoundedTrieData(null, left, 10);
    mainTree = mainTree.combine(new BoundedTrieData(null, right, 3));

    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                Arrays.asList("a", String.valueOf(true)),
                Arrays.asList("b", "d", String.valueOf(false)),
                Arrays.asList("c", "d", String.valueOf(false)))),
        mainTree.extractResult());
  }

  @Test
  public void testAddMultiThreaded() throws InterruptedException {
    final int numThreads = 10;
    final BoundedTrieData mainTrie = new BoundedTrieData();
    final CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

    Random rand = new Random(new Random().nextLong());
    List<List<String>> segments = randomSegments(numThreads, 3, 9, 0.5, rand);

    for (int curThread = 0; curThread < numThreads; curThread++) {
      int finalCurThread = curThread;
      executor.execute(
          () -> {
            try {
              mainTrie.add(segments.get(finalCurThread));
              // }
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    HashSet<List<String>> dedupedSegments = new HashSet<>(segments);
    assertEquals(everythingDeduped(dedupedSegments).size(), mainTrie.size());
    // Assert that all added paths are present in the mainTrie
    for (List<String> seg : dedupedSegments) {
      assertTrue(mainTrie.contains(seg));
    }
  }

  @Test
  public void testCombineMultiThreaded() throws InterruptedException {
    final int numThreads = 10;
    AtomicReference<BoundedTrieData> mainTrie = new AtomicReference<>(new BoundedTrieData());
    final CountDownLatch latch = new CountDownLatch(numThreads);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    Random rand = new Random(new Random().nextLong());

    // initialize mainTrie
    List<String> initialSegment = randomSegments(1, 3, 9, 0.5, rand).get(0);
    mainTrie.get().add(initialSegment);

    // prepare all segments in advance outside of multiple threads
    List<List<String>> segments = randomSegments(numThreads, 3, 9, 0.5, rand);
    segments.add(initialSegment);
    List<String> anotherSegment = randomSegments(1, 3, 9, 0.5, rand).get(0);
    segments.add(anotherSegment);

    for (int curThread = 0; curThread < numThreads; curThread++) {
      int finalCurThread = curThread;
      executor.execute(
          () -> {
            try {
              BoundedTrieData other = new BoundedTrieData();
              // only reads of segments; no write should be done here
              other.add(segments.get(finalCurThread));
              // for one node we add more than one segment to trigger root over
              // singleton and test combine with root.
              if (finalCurThread == 7) { // just a randomly selected prime number
                other.add(anotherSegment);
              }
              BoundedTrieData original;
              do {
                original = mainTrie.get();
              } while (!mainTrie.compareAndSet(original, original.combine(other)));
              // }
            } finally {
              latch.countDown();
            }
          });
    }

    latch.await();
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    HashSet<List<String>> dedupedSegments = new HashSet<>(segments);
    assertEquals(everythingDeduped(dedupedSegments).size(), mainTrie.get().size());
    // Assert that all added paths are present in the mainTrie
    for (List<String> seg : dedupedSegments) {
      assertTrue(mainTrie.get().contains(seg));
    }
  }

  @Test
  public void testTrim() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));
    assertEquals(3, root.getSize());
    assertEquals(-1, root.trim());
    assertEquals(2, root.getSize());
    List<List<String>> flattened = root.flattened();
    assertEquals(2, flattened.size());
    assertFalse(root.isTruncated());
    assertEquals(
        flattened,
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", BoundedTrieNode.TRUNCATED_TRUE),
                Arrays.asList("a", "e", BoundedTrieNode.TRUNCATED_FALSE))));
  }

  @Test
  public void testMerge() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    root1.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));

    BoundedTrieNode root2 = new BoundedTrieNode();
    root2.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "f"),
                Arrays.asList("a", "g"),
                Collections.singletonList("h"))));

    assertEquals(3, root1.merge(root2));
    assertEquals(6, root1.getSize());
  }

  @Test
  public void testMergeWithTruncatedNode() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    root1.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));

    BoundedTrieNode root2 = new BoundedTrieNode();
    root2.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "f"),
                Arrays.asList("a", "g"),
                Collections.singletonList("h"))));
    root2.trim();
    List<List<String>> expected =
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", BoundedTrieNode.TRUNCATED_TRUE),
                Arrays.asList("h", BoundedTrieNode.TRUNCATED_FALSE)));
    assertEquals(expected, root2.flattened());

    assertEquals(-1, root1.merge(root2));
    assertEquals(2, root1.getSize());
    assertEquals(expected, root1.flattened());
  }

  @Test
  public void testMergeWithEmptyNode() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    root1.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));

    BoundedTrieNode root2 = new BoundedTrieNode();

    assertEquals(0, root1.merge(root2));
    assertEquals(3, root1.getSize());
    assertFalse(root1.isTruncated());
  }

  @Test
  public void testMergeOnEmptyNode() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    BoundedTrieNode root2 = new BoundedTrieNode();
    root2.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "d"),
                Arrays.asList("a", "e"))));

    assertEquals(2, root1.merge(root2));
    assertEquals(3, root1.getSize());
    assertFalse(root1.isTruncated());
  }

  @Test
  public void testFlattenedWithTruncatedNode() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));
    root.trim();
    List<List<String>> expected =
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", BoundedTrieNode.TRUNCATED_TRUE),
                Arrays.asList("a", "e", BoundedTrieNode.TRUNCATED_FALSE)));
    assertEquals(expected, root.flattened());
  }

  @Test
  public void testContains() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));
    assertTrue(root.contains(Arrays.asList("a", "b", "c")));
    assertTrue(root.contains(Collections.singletonList("a")));
    assertFalse(root.contains(Arrays.asList("a", "b", "f")));
    assertFalse(root.contains(Collections.singletonList("z")));
  }

  @Test
  public void testDeepCopy() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));
    BoundedTrieNode copy = root.deepCopy();
    assertEquals(root, copy);

    // Modify the original and ensure the copy is not affected
    root.add(Arrays.asList("a", "f"));
    assertNotEquals(root, copy);
  }

  @Test
  public void testToProto() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));
    org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode proto = root.toProto();
    BoundedTrieNode fromProto = BoundedTrieNode.fromProto(proto);
    assertEquals(root, fromProto);
  }

  @Test
  public void testFromProto() {
    org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.Builder builder =
        org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder();
    builder.putChildren(
        "a",
        org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder()
            .putChildren(
                "b",
                org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder()
                    .putChildren(
                        "c",
                        org.apache.beam.model.pipeline.v1.MetricsApi.BoundedTrieNode.newBuilder()
                            .build())
                    .build())
            .build());
    BoundedTrieNode root = BoundedTrieNode.fromProto(builder.build());
    assertEquals(1, root.getSize());
    assertTrue(root.contains(Arrays.asList("a", "b", "c")));
    assertFalse(root.isTruncated());
  }

  @Test
  public void testBoundedTrieNodeEquals() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    root1.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));

    BoundedTrieNode root2 = new BoundedTrieNode();
    root2.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));

    assertEquals(root1, root2);
  }

  @Test
  public void testBoundedTrieNodeHashCode() {
    BoundedTrieNode root1 = new BoundedTrieNode();
    root1.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));

    BoundedTrieNode root2 = new BoundedTrieNode();
    root2.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));

    assertEquals(root1.hashCode(), root2.hashCode());
  }

  @Test
  public void testBoundedTrieNodeToString() {
    BoundedTrieNode root = new BoundedTrieNode();
    root.addAll(
        new ArrayList<>(
            Arrays.asList(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "e"),
                Arrays.asList("a", "b", "d"))));
    String expected = "{'abcfalse', 'abdfalse', 'aefalse'}";
    assertEquals(expected, root.toString());
  }

  @Test
  public void testEmptyTrie() {
    BoundedTrieData trie = new BoundedTrieData();
    assertEquals(0, trie.size());
    assertTrue(trie.extractResult().getResult().isEmpty());
  }

  @Test
  public void testSingleton() {
    List<String> path = ImmutableList.of("a", "b", "c");
    BoundedTrieData trie = new BoundedTrieData(path);
    assertEquals(1, trie.size());
    assertEquals(
        BoundedTrieResult.create(ImmutableSet.of(ImmutableList.of("a", "b", "c", "false"))),
        trie.extractResult());
    assertTrue(trie.contains(path));
    assertFalse(trie.contains(ImmutableList.of("a", "b")));
  }

  @Test
  public void testAddSingletonToTrie() {
    BoundedTrieData trie = new BoundedTrieData(ImmutableList.of("a", "b"));
    trie.add(ImmutableList.of("a", "c"));
    assertEquals(2, trie.size());
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                ImmutableList.of("a", "b", "false"), ImmutableList.of("a", "c", "false"))),
        trie.extractResult());
  }

  @Test
  public void testCombineEmptyTrie() {
    BoundedTrieData trie1 = new BoundedTrieData();
    BoundedTrieData trie2 = new BoundedTrieData();
    trie2.add(ImmutableList.of("a", "b"));
    trie1 = trie1.combine(trie2);
    assertEquals(1, trie1.size());
    assertEquals(
        BoundedTrieResult.create(ImmutableSet.of(ImmutableList.of("a", "b", "false"))),
        trie1.extractResult());
  }

  @Test
  public void testCombineWithSingleton() {
    BoundedTrieData trie1 = new BoundedTrieData();
    trie1.add(ImmutableList.of("a", "b"));

    BoundedTrieData trie2 = new BoundedTrieData(ImmutableList.of("c", "d"));

    trie1 = trie1.combine(trie2);
    assertEquals(2, trie1.size());
    assertEquals(
        BoundedTrieResult.create(
            ImmutableSet.of(
                ImmutableList.of("a", "b", "false"), ImmutableList.of("c", "d", "false"))),
        trie1.extractResult());
  }

  @Test
  public void testCombineWithItself() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    trie = trie.combine(trie);
    assertEquals(1, trie.size());
    assertEquals(
        BoundedTrieResult.create(ImmutableSet.of(ImmutableList.of("a", "b", "false"))),
        trie.extractResult());
  }

  @Test
  public void testClear() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    trie.clear();
    assertEquals(0, trie.size());
    assertTrue(trie.extractResult().getResult().isEmpty());
  }

  @Test
  public void testIsEmpty() {
    BoundedTrieData trie = new BoundedTrieData();
    assertTrue(trie.isEmpty());

    trie.add(Collections.emptyList());
    assertTrue(trie.isEmpty());

    trie.add(ImmutableList.of("a", "b"));
    assertFalse(trie.isEmpty());

    trie.add(ImmutableList.of("c", "d"));
    assertFalse(trie.isEmpty());

    trie.clear();
    assertTrue(trie.isEmpty());
  }

  @Test
  public void testBoundedTrieDataContains() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    assertTrue(trie.contains(ImmutableList.of("a", "b")));
    // path ab is not same as path a
    assertFalse(trie.contains(ImmutableList.of("a")));
    assertFalse(trie.contains(ImmutableList.of("a", "c")));
  }

  @Test
  public void testEquals() {
    BoundedTrieData trie1 = new BoundedTrieData();
    trie1.add(ImmutableList.of("a", "b"));
    BoundedTrieData trie2 = new BoundedTrieData();
    trie2.add(ImmutableList.of("a", "b"));
    assertEquals(trie1, trie2);
  }

  @Test
  public void testHashCode() {
    BoundedTrieData trie1 = new BoundedTrieData();
    trie1.add(ImmutableList.of("a", "b"));
    BoundedTrieData trie2 = new BoundedTrieData();
    trie2.add(ImmutableList.of("a", "b"));
    assertEquals(trie1.hashCode(), trie2.hashCode());
  }

  @Test
  public void testToString() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    assertTrue(trie.toString().contains("BoundedTrieData"));
  }

  @Test
  public void testToProtoFromProtoEmpty() {
    BoundedTrieData trie = new BoundedTrieData();
    BoundedTrie proto = trie.toProto();
    BoundedTrieData trieFromProto = BoundedTrieData.fromProto(proto);
    assertEquals(trieFromProto, trie);
  }

  @Test
  public void testToProtoFromProtoSingleton() {
    BoundedTrieData trie = new BoundedTrieData(ImmutableList.of("a", "b"));
    BoundedTrie proto = trie.toProto();
    BoundedTrieData trieFromProto = BoundedTrieData.fromProto(proto);
    assertEquals(trieFromProto, trie);
  }

  @Test
  public void testToProtoFromProtoWithData() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    trie.add(ImmutableList.of("a", "c"));
    BoundedTrie proto = trie.toProto();
    BoundedTrieData trieFromProto = BoundedTrieData.fromProto(proto);
    assertEquals(trieFromProto, trie);
  }

  @Test
  public void testConstructorInvalidInput() {
    assertThrows(
        AssertionError.class,
        () -> new BoundedTrieData(ImmutableList.of("a"), new BoundedTrieNode(), 100));
  }

  @Test
  public void testGetResultEmptyTrie() {
    BoundedTrieData trie = new BoundedTrieData();
    assertEquals(trie.extractResult(), BoundedTrieResult.empty());
  }

  @Test
  public void testGetResultSingleton() {
    List<String> singletonList = ImmutableList.of("a", "b");
    BoundedTrieData trie = new BoundedTrieData(singletonList);
    BoundedTrieResult result = trie.extractResult();
    assertEquals(
        result, BoundedTrieResult.create(ImmutableSet.of(ImmutableList.of("a", "b", "false"))));
  }

  @Test
  public void testGetCumulativeEmptyTrie() {
    BoundedTrieData trie = new BoundedTrieData();
    BoundedTrieData cumulativeTrie = trie.getCumulative();
    assertEquals(cumulativeTrie, trie);
    assertEquals(0, cumulativeTrie.size());
  }

  @Test
  public void testGetCumulativeSingleton() {
    List<String> singletonList = ImmutableList.of("a", "b");
    BoundedTrieData trie = new BoundedTrieData(singletonList);
    BoundedTrieData cumulativeTrie = trie.getCumulative();
    assertEquals(cumulativeTrie, trie);
    assertNotSame(cumulativeTrie, trie);
    // assert that the data in them are different
    cumulativeTrie.add(ImmutableList.of("g", "h"));
    assertTrue(cumulativeTrie.contains(ImmutableList.of("g", "h")));
    assertFalse(trie.contains(ImmutableList.of("g", "h")));
    assertEquals(1, trie.size());
  }

  @Test
  public void testGetCumulativeWithRoot() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(ImmutableList.of("a", "b"));
    trie.add(ImmutableList.of("d", "e"));
    BoundedTrieData cumulativeTrie = trie.getCumulative();
    assertEquals(cumulativeTrie, trie);
    assertNotSame(cumulativeTrie, trie);
    // assert that the data in them are different
    trie.add(ImmutableList.of("g", "h"));
    assertTrue(trie.contains(ImmutableList.of("g", "h")));
    assertFalse(cumulativeTrie.contains(ImmutableList.of("g", "h")));
  }

  @Test
  public void testContainsEmptyPath() {
    BoundedTrieData trie = new BoundedTrieData();
    trie.add(Collections.emptyList());
    assertFalse(trie.contains(Collections.emptyList()));
    assertTrue(trie.isEmpty());
  }
}
