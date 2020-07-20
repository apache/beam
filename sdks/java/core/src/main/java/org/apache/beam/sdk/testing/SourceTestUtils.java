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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper functions and test harnesses for checking correctness of {@link Source} implementations.
 *
 * <p>Contains a few lightweight utilities (e.g. reading items from a source or a reader, such as
 * {@link #readFromSource} and {@link #readFromUnstartedReader}), as well as heavyweight property
 * testing and stress testing harnesses that help getting a large amount of test coverage with few
 * code. Most notable ones are:
 *
 * <ul>
 *   <li>{@link #assertSourcesEqualReferenceSource} helps testing that the data read by the union of
 *       sources produced by {@link BoundedSource#split} is the same as data read by the original
 *       source.
 *   <li>If your source implements dynamic work rebalancing, use the {@code assertSplitAtFraction}
 *       family of functions - they test behavior of {@link
 *       BoundedSource.BoundedReader#splitAtFraction}, in particular, that various consistency
 *       properties are respected and the total set of data read by the source is preserved when
 *       splits happen. Use {@link #assertSplitAtFractionBehavior} to test individual cases of
 *       {@code splitAtFraction} and use {@link #assertSplitAtFractionExhaustive} as a heavy-weight
 *       stress test including concurrency. We strongly recommend to use both.
 * </ul>
 *
 * For example usages, see the unit tests of classes such as {@code AvroSource} or {@code
 * TextSource}.
 *
 * <p>Like {@link PAssert}, requires JUnit and Hamcrest to be present in the classpath.
 */
public class SourceTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SourceTestUtils.class);

  // A wrapper around a value of type T that compares according to the structural
  // value provided by a Coder<T>, but prints both the original and structural value,
  // to help get good error messages from JUnit equality assertion failures and such.
  private static class ReadableStructuralValue<T> {
    private T originalValue;
    private Object structuralValue;

    public ReadableStructuralValue(T originalValue, Object structuralValue) {
      this.originalValue = originalValue;
      this.structuralValue = structuralValue;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(structuralValue);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof ReadableStructuralValue)) {
        return false;
      }
      return Objects.equals(structuralValue, ((ReadableStructuralValue) obj).structuralValue);
    }

    @Override
    public String toString() {
      return String.format("[%s (structural %s)]", originalValue, structuralValue);
    }
  }

  /**
   * Testing utilities below depend on standard assertions and matchers to compare elements read by
   * sources. In general the elements may not implement {@code equals}/{@code hashCode} properly,
   * however every source has a {@link Coder} and every {@code Coder} can produce a {@link
   * Coder#structuralValue} whose {@code equals}/{@code hashCode} is consistent with equality of
   * encoded format. So we use this {@link Coder#structuralValue} to compare elements read by
   * sources.
   */
  public static <T> List<ReadableStructuralValue<T>> createStructuralValues(
      Coder<T> coder, List<T> list) throws Exception {
    List<ReadableStructuralValue<T>> result = new ArrayList<>();
    for (T elem : list) {
      result.add(new ReadableStructuralValue<>(elem, coder.structuralValue(elem)));
    }
    return result;
  }

  /** Reads all elements from the given {@link BoundedSource}. */
  public static <T> List<T> readFromSource(BoundedSource<T> source, PipelineOptions options)
      throws IOException {
    try (BoundedSource.BoundedReader<T> reader = source.createReader(options)) {
      return readFromUnstartedReader(reader);
    }
  }

  public static <T> List<T> readFromSplitsOfSource(
      BoundedSource<T> source, long desiredBundleSizeBytes, PipelineOptions options)
      throws Exception {
    List<T> res = Lists.newArrayList();
    for (BoundedSource<T> split : source.split(desiredBundleSizeBytes, options)) {
      res.addAll(readFromSource(split, options));
    }
    return res;
  }

  /** Reads all elements from the given unstarted {@link Source.Reader}. */
  public static <T> List<T> readFromUnstartedReader(Source.Reader<T> reader) throws IOException {
    return readRemainingFromReader(reader, false);
  }

  /** Reads all elements from the given started {@link Source.Reader}. */
  public static <T> List<T> readFromStartedReader(Source.Reader<T> reader) throws IOException {
    return readRemainingFromReader(reader, true);
  }

  /** Read elements from a {@link Source.Reader} until n elements are read. */
  public static <T> List<T> readNItemsFromUnstartedReader(Source.Reader<T> reader, int n)
      throws IOException {
    return readNItemsFromReader(reader, n, false);
  }

  /**
   * Read elements from a {@link Source.Reader} that has already had {@link Source.Reader#start}
   * called on it, until n elements are read.
   */
  public static <T> List<T> readNItemsFromStartedReader(Source.Reader<T> reader, int n)
      throws IOException {
    return readNItemsFromReader(reader, n, true);
  }

  /**
   * Read elements from a {@link Source.Reader} until n elements are read.
   *
   * <p>There must be at least n elements remaining in the reader, except for the case when n is
   * {@code Integer.MAX_VALUE}, which means "read all remaining elements".
   */
  private static <T> List<T> readNItemsFromReader(Source.Reader<T> reader, int n, boolean started)
      throws IOException {
    List<T> res = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      boolean shouldStart = (i == 0 && !started);
      boolean more = shouldStart ? reader.start() : reader.advance();
      if (n != Integer.MAX_VALUE) {
        assertTrue(more);
      }
      if (!more) {
        break;
      }
      res.add(reader.getCurrent());
    }
    return res;
  }

  /** Read all remaining elements from a {@link Source.Reader}. */
  public static <T> List<T> readRemainingFromReader(Source.Reader<T> reader, boolean started)
      throws IOException {
    return readNItemsFromReader(reader, Integer.MAX_VALUE, started);
  }

  /**
   * Given a reference {@code Source} and a list of {@code Source}s, assert that the union of the
   * records read from the list of sources is equal to the records read from the reference source.
   */
  public static <T> void assertSourcesEqualReferenceSource(
      BoundedSource<T> referenceSource,
      List<? extends BoundedSource<T>> sources,
      PipelineOptions options)
      throws Exception {
    Coder<T> coder = referenceSource.getOutputCoder();
    List<T> referenceRecords = readFromSource(referenceSource, options);
    List<T> bundleRecords = new ArrayList<>();
    for (BoundedSource<T> source : sources) {
      assertThat(
          "Coder type for source "
              + source
              + " is not compatible with Coder type for referenceSource "
              + referenceSource,
          source.getOutputCoder(),
          equalTo(coder));
      List<T> elems = readFromSource(source, options);
      bundleRecords.addAll(elems);
    }
    List<ReadableStructuralValue<T>> bundleValues = createStructuralValues(coder, bundleRecords);
    List<ReadableStructuralValue<T>> referenceValues =
        createStructuralValues(coder, referenceRecords);
    assertThat(bundleValues, containsInAnyOrder(referenceValues.toArray()));
  }

  /**
   * Assert that a {@code Reader} returns a {@code Source} that, when read from, produces the same
   * records as the reader.
   */
  public static <T> void assertUnstartedReaderReadsSameAsItsSource(
      BoundedSource.BoundedReader<T> reader, PipelineOptions options) throws Exception {
    Coder<T> coder = reader.getCurrentSource().getOutputCoder();
    List<T> expected = readFromUnstartedReader(reader);
    List<T> actual = readFromSource(reader.getCurrentSource(), options);
    List<ReadableStructuralValue<T>> expectedStructural = createStructuralValues(coder, expected);
    List<ReadableStructuralValue<T>> actualStructural = createStructuralValues(coder, actual);
    assertThat(actualStructural, containsInAnyOrder(expectedStructural.toArray()));
  }

  /**
   * Expected outcome of {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader#splitAtFraction}.
   */
  public enum ExpectedSplitOutcome {
    /** The operation must succeed and the results must be consistent. */
    MUST_SUCCEED_AND_BE_CONSISTENT,
    /** The operation must fail (return {@code null}). */
    MUST_FAIL,
    /** The operation must either fail, or succeed and the results be consistent. */
    MUST_BE_CONSISTENT_IF_SUCCEEDS
  }

  /**
   * Contains two values: the number of items in the primary source, and the number of items in the
   * residual source, -1 if split failed.
   */
  private static class SplitAtFractionResult {
    public int numPrimaryItems;
    public int numResidualItems;

    public SplitAtFractionResult(int numPrimaryItems, int numResidualItems) {
      this.numPrimaryItems = numPrimaryItems;
      this.numResidualItems = numResidualItems;
    }
  }

  /**
   * Asserts that the {@code source}'s reader either fails to {@code splitAtFraction(fraction)}
   * after reading {@code numItemsToReadBeforeSplit} items, or succeeds in a way that is consistent
   * according to {@link #assertSplitAtFractionSucceedsAndConsistent}.
   *
   * <p>Returns SplitAtFractionResult.
   */
  public static <T> SplitAtFractionResult assertSplitAtFractionBehavior(
      BoundedSource<T> source,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      ExpectedSplitOutcome expectedOutcome,
      PipelineOptions options)
      throws Exception {
    return assertSplitAtFractionBehaviorImpl(
        source,
        readFromSource(source, options),
        numItemsToReadBeforeSplit,
        splitFraction,
        expectedOutcome,
        options);
  }

  /**
   * Compares two lists elementwise and throws a detailed assertion failure optimized for human
   * reading in case they are unequal.
   */
  private static <T> void assertListsEqualInOrder(
      String message, String expectedLabel, List<T> expected, String actualLabel, List<T> actual) {
    int i = 0;
    for (; i < expected.size() && i < actual.size(); ++i) {
      if (!Objects.equals(expected.get(i), actual.get(i))) {
        Assert.fail(
            String.format(
                "%s: %s and %s have %d items in common and then differ. "
                    + "Item in %s (%d more): %s, item in %s (%d more): %s",
                message,
                expectedLabel,
                actualLabel,
                i,
                expectedLabel,
                expected.size() - i - 1,
                expected.get(i),
                actualLabel,
                actual.size() - i - 1,
                actual.get(i)));
      }
    }
    if (i < expected.size() /* but i == actual.size() */) {
      Assert.fail(
          String.format(
              "%s: %s has %d more items after matching all %d from %s. First 5: %s",
              message,
              expectedLabel,
              expected.size() - actual.size(),
              actual.size(),
              actualLabel,
              expected.subList(actual.size(), Math.min(expected.size(), actual.size() + 5))));
    } else if (i < actual.size() /* but i == expected.size() */) {
      Assert.fail(
          String.format(
              "%s: %s has %d more items after matching all %d from %s. First 5: %s",
              message,
              actualLabel,
              actual.size() - expected.size(),
              expected.size(),
              expectedLabel,
              actual.subList(expected.size(), Math.min(actual.size(), expected.size() + 5))));
    } else {
      // All is well.
    }
  }

  private static <T> SourceTestUtils.SplitAtFractionResult assertSplitAtFractionBehaviorImpl(
      BoundedSource<T> source,
      List<T> expectedItems,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      ExpectedSplitOutcome expectedOutcome,
      PipelineOptions options)
      throws Exception {
    try (BoundedSource.BoundedReader<T> reader = source.createReader(options)) {
      BoundedSource<T> originalSource = reader.getCurrentSource();
      List<T> currentItems = readNItemsFromUnstartedReader(reader, numItemsToReadBeforeSplit);
      BoundedSource<T> residual = reader.splitAtFraction(splitFraction);
      if (residual != null) {
        assertFalse(
            String.format(
                "Primary source didn't change after a successful split of %s at %f "
                    + "after reading %d items. "
                    + "Was the source object mutated instead of creating a new one? "
                    + "Source objects MUST be immutable.",
                source, splitFraction, numItemsToReadBeforeSplit),
            reader.getCurrentSource() == originalSource);
        assertFalse(
            String.format(
                "Residual source equal to original source after a successful split of %s at %f "
                    + "after reading %d items. "
                    + "Was the source object mutated instead of creating a new one? "
                    + "Source objects MUST be immutable.",
                source, splitFraction, numItemsToReadBeforeSplit),
            reader.getCurrentSource() == residual);
      }
      // Failure cases are: must succeed but fails; must fail but succeeds.
      switch (expectedOutcome) {
        case MUST_SUCCEED_AND_BE_CONSISTENT:
          assertNotNull(
              "Failed to split reader of source: "
                  + source
                  + " at "
                  + splitFraction
                  + " after reading "
                  + numItemsToReadBeforeSplit
                  + " items",
              residual);
          break;
        case MUST_FAIL:
          assertEquals(null, residual);
          break;
        case MUST_BE_CONSISTENT_IF_SUCCEEDS:
          // Nothing.
          break;
      }
      currentItems.addAll(readRemainingFromReader(reader, numItemsToReadBeforeSplit > 0));
      BoundedSource<T> primary = reader.getCurrentSource();
      return verifySingleSplitAtFractionResult(
          source,
          expectedItems,
          currentItems,
          primary,
          residual,
          numItemsToReadBeforeSplit,
          splitFraction,
          options);
    }
  }

  private static <T> SourceTestUtils.SplitAtFractionResult verifySingleSplitAtFractionResult(
      BoundedSource<T> source,
      List<T> expectedItems,
      List<T> currentItems,
      BoundedSource<T> primary,
      BoundedSource<T> residual,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      PipelineOptions options)
      throws Exception {
    List<T> primaryItems = readFromSource(primary, options);
    if (residual != null) {
      List<T> residualItems = readFromSource(residual, options);
      List<T> totalItems = new ArrayList<>();
      totalItems.addAll(primaryItems);
      totalItems.addAll(residualItems);
      String errorMsgForPrimarySourceComp =
          String.format(
              "Continued reading after split yielded different items than primary source: "
                  + "split at %s after reading %s items, original source: %s, primary source: %s",
              splitFraction, numItemsToReadBeforeSplit, source, primary);
      String errorMsgForTotalSourceComp =
          String.format(
              "Items in primary and residual sources after split do not add up to items "
                  + "in the original source. Split at %s after reading %s items; "
                  + "original source: %s, primary: %s, residual: %s",
              splitFraction, numItemsToReadBeforeSplit, source, primary, residual);
      Coder<T> coder = primary.getOutputCoder();
      List<ReadableStructuralValue<T>> primaryValues = createStructuralValues(coder, primaryItems);
      List<ReadableStructuralValue<T>> currentValues = createStructuralValues(coder, currentItems);
      List<ReadableStructuralValue<T>> expectedValues =
          createStructuralValues(coder, expectedItems);
      List<ReadableStructuralValue<T>> totalValues = createStructuralValues(coder, totalItems);
      assertListsEqualInOrder(
          errorMsgForPrimarySourceComp, "current", currentValues, "primary", primaryValues);
      assertListsEqualInOrder(
          errorMsgForTotalSourceComp, "total", expectedValues, "primary+residual", totalValues);
      return new SplitAtFractionResult(primaryItems.size(), residualItems.size());
    }
    return new SplitAtFractionResult(primaryItems.size(), -1);
  }

  /**
   * Verifies some consistency properties of {@link BoundedSource.BoundedReader#splitAtFraction} on
   * the given source. Equivalent to the following pseudocode:
   *
   * <pre>
   *   Reader reader = source.createReader();
   *   read N items from reader;
   *   Source residual = reader.splitAtFraction(splitFraction);
   *   Source primary = reader.getCurrentSource();
   *   assert: items in primary == items we read so far
   *                               + items we'll get by continuing to read from reader;
   *   assert: items in original source == items in primary + items in residual
   * </pre>
   */
  public static <T> void assertSplitAtFractionSucceedsAndConsistent(
      BoundedSource<T> source,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      PipelineOptions options)
      throws Exception {
    assertSplitAtFractionBehavior(
        source,
        numItemsToReadBeforeSplit,
        splitFraction,
        ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT,
        options);
  }

  /**
   * Asserts that the {@code source}'s reader fails to {@code splitAtFraction(fraction)} after
   * reading {@code numItemsToReadBeforeSplit} items.
   */
  public static <T> void assertSplitAtFractionFails(
      BoundedSource<T> source,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      PipelineOptions options)
      throws Exception {
    assertSplitAtFractionBehavior(
        source, numItemsToReadBeforeSplit, splitFraction, ExpectedSplitOutcome.MUST_FAIL, options);
  }

  private static class SplitFractionStatistics {
    List<Double> successfulFractions = new ArrayList<>();
    List<Double> nonTrivialFractions = new ArrayList<>();
  }

  /**
   * Asserts that given a start position, {@link BoundedSource.BoundedReader#splitAtFraction} at
   * every interesting fraction (halfway between two fractions that differ by at least one item) can
   * be called successfully and the results are consistent if a split succeeds.
   */
  private static <T> void assertSplitAtFractionBinary(
      BoundedSource<T> source,
      List<T> expectedItems,
      int numItemsToBeReadBeforeSplit,
      double leftFraction,
      SplitAtFractionResult leftResult,
      double rightFraction,
      SplitAtFractionResult rightResult,
      PipelineOptions options,
      SplitFractionStatistics stats)
      throws Exception {
    if (rightFraction - leftFraction < 0.001) {
      // Do not recurse too deeply. Otherwise we will end up in infinite
      // recursion, e.g., while trying to find the exact minimal fraction s.t.
      // split succeeds. A precision of 0.001 when looking for such a fraction
      // ought to be enough for everybody.
      return;
    }
    double middleFraction = (rightFraction + leftFraction) / 2;
    if (leftResult == null) {
      leftResult =
          assertSplitAtFractionBehaviorImpl(
              source,
              expectedItems,
              numItemsToBeReadBeforeSplit,
              leftFraction,
              ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS,
              options);
    }
    if (rightResult == null) {
      rightResult =
          assertSplitAtFractionBehaviorImpl(
              source,
              expectedItems,
              numItemsToBeReadBeforeSplit,
              rightFraction,
              ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS,
              options);
    }
    SplitAtFractionResult middleResult =
        assertSplitAtFractionBehaviorImpl(
            source,
            expectedItems,
            numItemsToBeReadBeforeSplit,
            middleFraction,
            ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS,
            options);
    if (middleResult.numResidualItems != -1) {
      stats.successfulFractions.add(middleFraction);
    }
    if (middleResult.numResidualItems > 0) {
      stats.nonTrivialFractions.add(middleFraction);
    }
    // Two split fractions are equivalent if they yield the same number of
    // items in primary vs. residual source. Left and right are already not
    // equivalent. Recurse into [left, middle) and [right, middle) respectively
    // if middle is not equivalent to left or right.
    if (leftResult.numPrimaryItems != middleResult.numPrimaryItems) {
      assertSplitAtFractionBinary(
          source,
          expectedItems,
          numItemsToBeReadBeforeSplit,
          leftFraction,
          leftResult,
          middleFraction,
          middleResult,
          options,
          stats);
    }
    if (rightResult.numPrimaryItems != middleResult.numPrimaryItems) {
      assertSplitAtFractionBinary(
          source,
          expectedItems,
          numItemsToBeReadBeforeSplit,
          middleFraction,
          middleResult,
          rightFraction,
          rightResult,
          options,
          stats);
    }
  }

  private static final int MAX_CONCURRENT_SPLITTING_TRIALS_PER_ITEM = 100;
  private static final int MAX_CONCURRENT_SPLITTING_TRIALS_TOTAL = 1000;

  /**
   * Asserts that for each possible start position, {@link
   * BoundedSource.BoundedReader#splitAtFraction} at every interesting fraction (halfway between two
   * fractions that differ by at least one item) can be called successfully and the results are
   * consistent if a split succeeds. Verifies multithreaded splitting as well.
   */
  public static <T> void assertSplitAtFractionExhaustive(
      BoundedSource<T> source, PipelineOptions options) throws Exception {
    List<T> expectedItems = readFromSource(source, options);
    assertFalse("Empty source", expectedItems.isEmpty());
    assertFalse("Source reads a single item", expectedItems.size() == 1);
    List<List<Double>> allNonTrivialFractions = new ArrayList<>();
    {
      boolean anySuccessfulFractions = false;
      boolean anyNonTrivialFractions = false;
      for (int i = 0; i < expectedItems.size(); i++) {
        SplitFractionStatistics stats = new SplitFractionStatistics();
        assertSplitAtFractionBinary(source, expectedItems, i, 0.0, null, 1.0, null, options, stats);
        if (!stats.successfulFractions.isEmpty()) {
          anySuccessfulFractions = true;
        }
        if (!stats.nonTrivialFractions.isEmpty()) {
          anyNonTrivialFractions = true;
        }
        allNonTrivialFractions.add(stats.nonTrivialFractions);
      }
      assertTrue(
          "splitAtFraction test completed vacuously: no successful split fractions found",
          anySuccessfulFractions);
      assertTrue(
          "splitAtFraction test completed vacuously: no non-trivial split fractions found",
          anyNonTrivialFractions);
    }
    {
      // Perform a stress test of "racy" concurrent splitting:
      // for every position (number of items read), try to split at the minimum nontrivial
      // split fraction for that position concurrently with reading the record at that position.
      // To ensure that the test is non-vacuous, make sure that the splitting succeeds
      // at least once and fails at least once.
      ExecutorService executor = Executors.newFixedThreadPool(2);
      int numTotalTrials = 0;
      for (int i = 0; i < expectedItems.size(); i++) {
        double minNonTrivialFraction = 2.0; // Greater than any possible fraction.
        for (double fraction : allNonTrivialFractions.get(i)) {
          minNonTrivialFraction = Math.min(minNonTrivialFraction, fraction);
        }
        if (minNonTrivialFraction == 2.0) {
          // This will not happen all the time because otherwise the test above would
          // detect vacuousness.
          continue;
        }
        int numTrials = 0;
        boolean haveSuccess = false, haveFailure = false;
        while (true) {
          ++numTrials;
          if (numTrials > MAX_CONCURRENT_SPLITTING_TRIALS_PER_ITEM) {
            LOG.warn(
                "After {} concurrent splitting trials at item #{}, observed only {}, "
                    + "giving up on this item",
                numTrials,
                i,
                haveSuccess ? "success" : "failure");
            break;
          }
          if (assertSplitAtFractionConcurrent(
              executor, source, expectedItems, i, minNonTrivialFraction, options)) {
            haveSuccess = true;
          } else {
            haveFailure = true;
          }
          if (haveSuccess && haveFailure) {
            LOG.info(
                "{} trials to observe both success and failure of concurrent splitting at item #{}",
                numTrials,
                i);
            break;
          }
        }
        numTotalTrials += numTrials;
        if (numTotalTrials > MAX_CONCURRENT_SPLITTING_TRIALS_TOTAL) {
          LOG.warn(
              "After {} total concurrent splitting trials, considered only {} items, giving up.",
              numTotalTrials,
              i);
          break;
        }
      }
      LOG.info(
          "{} total concurrent splitting trials for {} items",
          numTotalTrials,
          expectedItems.size());
    }
  }

  private static <T> boolean assertSplitAtFractionConcurrent(
      ExecutorService executor,
      BoundedSource<T> source,
      List<T> expectedItems,
      final int numItemsToReadBeforeSplitting,
      final double fraction,
      PipelineOptions options)
      throws Exception {
    @SuppressWarnings("resource") // Closed in readerThread
    final BoundedSource.BoundedReader<T> reader = source.createReader(options);
    final CountDownLatch unblockSplitter = new CountDownLatch(1);
    Future<List<T>> readerThread =
        executor.submit(
            () -> {
              try {
                List<T> items =
                    readNItemsFromUnstartedReader(reader, numItemsToReadBeforeSplitting);
                unblockSplitter.countDown();
                items.addAll(readRemainingFromReader(reader, numItemsToReadBeforeSplitting > 0));
                return items;
              } finally {
                reader.close();
              }
            });
    Future<KV<BoundedSource<T>, BoundedSource<T>>> splitterThread =
        executor.submit(
            () -> {
              unblockSplitter.await();
              BoundedSource<T> residual = reader.splitAtFraction(fraction);
              if (residual == null) {
                return null;
              }
              return KV.of(reader.getCurrentSource(), residual);
            });
    List<T> currentItems = readerThread.get();
    KV<BoundedSource<T>, BoundedSource<T>> splitSources = splitterThread.get();
    if (splitSources == null) {
      return false;
    }
    SplitAtFractionResult res =
        verifySingleSplitAtFractionResult(
            source,
            expectedItems,
            currentItems,
            splitSources.getKey(),
            splitSources.getValue(),
            numItemsToReadBeforeSplitting,
            fraction,
            options);
    return (res.numResidualItems > 0);
  }

  /**
   * Returns an equivalent unsplittable {@code BoundedSource<T>}.
   *
   * <p>It forwards most methods to the given {@code boundedSource}, except:
   *
   * <ol>
   *   <li>{@link BoundedSource#split} rejects initial splitting by returning itself in a list.
   *   <li>{@link BoundedReader#splitAtFraction} rejects dynamic splitting by returning null.
   * </ol>
   */
  public static <T> BoundedSource<T> toUnsplittableSource(BoundedSource<T> boundedSource) {
    return new UnsplittableSource<>(boundedSource);
  }

  private static class UnsplittableSource<T> extends BoundedSource<T> {

    private final BoundedSource<T> boundedSource;

    private UnsplittableSource(BoundedSource<T> boundedSource) {
      this.boundedSource = checkNotNull(boundedSource, "boundedSource");
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      this.boundedSource.populateDisplayData(builder);
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return boundedSource.getEstimatedSizeBytes(options);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return new UnsplittableReader<>(boundedSource, boundedSource.createReader(options));
    }

    @Override
    public void validate() {
      boundedSource.validate();
    }

    @Override
    public Coder<T> getOutputCoder() {
      return boundedSource.getOutputCoder();
    }

    private static class UnsplittableReader<T> extends BoundedReader<T> {

      private final BoundedSource<T> boundedSource;
      private final BoundedReader<T> boundedReader;

      private UnsplittableReader(BoundedSource<T> boundedSource, BoundedReader<T> boundedReader) {
        this.boundedSource = checkNotNull(boundedSource, "boundedSource");
        this.boundedReader = checkNotNull(boundedReader, "boundedReader");
      }

      @Override
      public BoundedSource<T> getCurrentSource() {
        return boundedSource;
      }

      @Override
      public boolean start() throws IOException {
        return boundedReader.start();
      }

      @Override
      public boolean advance() throws IOException {
        return boundedReader.advance();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return boundedReader.getCurrent();
      }

      @Override
      public void close() throws IOException {
        boundedReader.close();
      }

      @Override
      public @Nullable BoundedSource<T> splitAtFraction(double fraction) {
        return null;
      }

      @Override
      public @Nullable Double getFractionConsumed() {
        return boundedReader.getFractionConsumed();
      }

      @Override
      public long getSplitPointsConsumed() {
        return boundedReader.getSplitPointsConsumed();
      }

      @Override
      public long getSplitPointsRemaining() {
        return boundedReader.getSplitPointsRemaining();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return boundedReader.getCurrentTimestamp();
      }
    }
  }
}
