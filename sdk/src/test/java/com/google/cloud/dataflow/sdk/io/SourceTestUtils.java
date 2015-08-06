/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for testing {@link Source} classes.
 */
public class SourceTestUtils {
  /**
   * Testing utilities below depend on standard assertions and matchers to compare elements read by
   * sources. In general the elements may not implement {@link equals}/{@link hashCode} properly,
   * however every source has a {@link Coder} and every {@code Coder} can
   * produce a {@link Coder#structuralValue()} whose {@code equals}/{@code hashCode} is
   * consistent with equality of encoded format.
   * So we use this {@link Coder#structuralValue()} to compare elements read by sources.
   */
  private static <T> List<Object> createStructuralValues(Coder<T> coder, List<T> list)
      throws Exception {
    List<Object> result = new ArrayList<>();
    for (T elem : list) {
      result.add(coder.structuralValue(elem));
    }
    return result;
  }

  /**
   * Reads all elements from the given {@link BoundedSource}.
   */
  public static <T> List<T> readFromSource(BoundedSource<T> source, PipelineOptions options)
      throws IOException {
    try (BoundedSource.BoundedReader<T> reader = source.createReader(options)) {
      return readFromUnstartedReader(reader);
    }
  }

  /**
   * Reads all elements from the given unstarted {@link Source.Reader}.
   */
  public static <T> List<T> readFromUnstartedReader(BoundedSource.BoundedReader<T> reader)
      throws IOException {
    List<T> res = new ArrayList<>();
    for (boolean more = reader.start(); more; more = reader.advance()) {
      res.add(reader.getCurrent());
    }
    return res;
  }

  public static <T> List<T> readFromStartedReader(BoundedSource.BoundedReader<T> reader)
      throws IOException {
    List<T> res = new ArrayList<>();
    while (reader.advance()) {
      res.add(reader.getCurrent());
    }
    return res;
  }

  public static <T> List<T> readNItemsFromUnstartedReader(Source.Reader<T> reader, int n)
      throws IOException {
    List<T> res = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      assertTrue((i == 0) ? reader.start() : reader.advance());
      res.add(reader.getCurrent());
    }
    return res;
  }

  /**
   * Given a reference {@code Source} and a list of {@code Source}s, assert that the union of
   * the records read from the list of sources is equal to the records read from the reference
   * source.
   */
  public static <T> void assertSourcesEqualReferenceSource(
      BoundedSource<T> referenceSource,
      List<? extends BoundedSource<T>> sources,
      PipelineOptions options)
      throws Exception {
    Coder<T> coder = referenceSource.getDefaultOutputCoder();
    List<T> referenceRecords = readFromSource(referenceSource, options);
    List<T> bundleRecords = new ArrayList<>();
    for (BoundedSource<T> source : sources) {
      assertThat(
          "Coder type for source "
              + source
              + " is not compatible with Coder type for referenceSource "
              + referenceSource,
          source.getDefaultOutputCoder(),
          equalTo(coder));
      List<T> elems = readFromSource(source, options);
      bundleRecords.addAll(elems);
    }
    List<Object> bundleValues = createStructuralValues(coder, bundleRecords);
    List<Object> referenceValues = createStructuralValues(coder, referenceRecords);
    assertThat(bundleValues, containsInAnyOrder(referenceValues.toArray()));
  }

  /**
   * Assert that a {@code Reader} returns a {@code Source} that, when read from, produces the same
   * records as the reader.
   */
  public static <T> void assertUnstartedReaderReadsSameAsItsSource(
      BoundedSource.BoundedReader<T> reader, PipelineOptions options) throws Exception {
    Coder<T> coder = reader.getCurrentSource().getDefaultOutputCoder();
    List<T> expected = readFromUnstartedReader(reader);
    List<T> actual = readFromSource(reader.getCurrentSource(), options);
    List<Object> expectedValues = createStructuralValues(coder, expected);
    List<Object> actualValues = createStructuralValues(coder, actual);
    assertThat(actualValues, containsInAnyOrder(expectedValues.toArray()));
  }

  /**
   * Expected outcome of
   * {@link com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader#splitAtFraction}.
   */
  public enum ExpectedSplitOutcome {
    /**
     * The operation must succeed and the results must be consistent.
     */
    MUST_SUCCEED_AND_BE_CONSISTENT,
    /**
     * The operation must fail (return {@code null}).
     */
    MUST_FAIL,
    /**
     * The operation must either fail, or succeed and the results be consistent.
     */
    MUST_BE_CONSISTENT_IF_SUCCEEDS
  }

  /**
   * Contains two values: the number of items in the primary source, and the number of items in
   * the residual source, -1 if split failed.
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
   * after reading {@code numItemsToReadBeforeSplit} items, or succeeds in a way that is
   * consistent according to {@link #assertSplitAtFractionSucceedsAndConsistent}.
   * <p> Returns SplitAtFractionResult.
   */

  public static <T> SplitAtFractionResult assertSplitAtFractionBehavior(
      BoundedSource<T> source,
      int numItemsToReadBeforeSplit,
      double splitFraction,
      ExpectedSplitOutcome expectedOutcome,
      PipelineOptions options)
      throws Exception {
    List<T> expectedItems = readFromSource(source, options);
    try (BoundedSource.BoundedReader<T> reader = source.createReader(options)) {
      List<T> currentItems = new ArrayList<>();
      currentItems.addAll(readNItemsFromUnstartedReader(reader, numItemsToReadBeforeSplit));
      BoundedSource<T> residual = reader.splitAtFraction(splitFraction);
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
      BoundedSource<T> primary = reader.getCurrentSource();
      List<T> primaryItems = readFromSource(primary, options);
      if (residual != null) {
        List<T> residualItems = readFromSource(residual, options);
        List<T> totalItems = new ArrayList<>();
        totalItems.addAll(primaryItems);
        totalItems.addAll(residualItems);
        currentItems.addAll(
            numItemsToReadBeforeSplit > 0
                ? readFromStartedReader(reader)
                : readFromUnstartedReader(reader));
        String errorMsgForPrimarySourceComp =
            String.format(
                "Continued reading after split yielded different items than primary source: "
                    + "split at %s after reading %s items, original source: %s, primary source: %s",
                splitFraction,
                numItemsToReadBeforeSplit,
                source,
                primary);
        String errorMsgForTotalSourceComp =
            String.format(
                "Items in primary and residual sources after split do not add up to items "
                    + "in the original source. Split at %s after reading %s items; "
                    + "original source: %s, primary: %s, residual: %s",
                splitFraction,
                numItemsToReadBeforeSplit,
                source,
                primary,
                residual);
        Coder<T> coder = reader.getCurrentSource().getDefaultOutputCoder();
        List<Object> primaryValues = createStructuralValues(coder, primaryItems);
        List<Object> currentValues = createStructuralValues(coder, currentItems);
        List<Object> expectedValues = createStructuralValues(coder, expectedItems);
        List<Object> totalValues = createStructuralValues(coder, totalItems);
        assertThat(errorMsgForPrimarySourceComp, currentValues, contains(primaryValues.toArray()));
        assertThat(errorMsgForTotalSourceComp, totalValues, contains(expectedValues.toArray()));
        return new SplitAtFractionResult(primaryItems.size(), residualItems.size());
      }
      return new SplitAtFractionResult(primaryItems.size(), -1);
    }
  }

  /**
   * Verifies some consistency properties of
   * {@link BoundedSource.BoundedReader#splitAtFraction} on the given source. Equivalent to
   * the following pseudocode:
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
   * Asserts that the {@code source}'s reader fails to {@code splitAtFraction(fraction)}
   * after reading {@code numItemsToReadBeforeSplit} items.
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

  /**
   * Asserts that given a start position,
   * {@link BoundedSource.BoundedReader#splitAtFraction} at every interesting fraction (halfway
   * between two fractions that differ by at least one item) can be called successfully and the
   * results are consistent if a split succeeds.
   */
  public static <T> void assertSplitAtFractionBinary(
      BoundedSource<T> source,
      int numItemsToBeReadBeforeSplit,
      double firstSplitFraction,
      double secondSplitFraction,
      PipelineOptions options)
      throws Exception {
    if (secondSplitFraction - firstSplitFraction < 0.0001) {
      return;
    }
    double middleSplitFraction = ((secondSplitFraction - firstSplitFraction)
        / 2) + firstSplitFraction;
    SplitAtFractionResult splitAtFirst = assertSplitAtFractionBehavior(
        source, numItemsToBeReadBeforeSplit, firstSplitFraction,
        ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);
    SplitAtFractionResult splitAtMiddle = assertSplitAtFractionBehavior(
        source, numItemsToBeReadBeforeSplit, middleSplitFraction,
        ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);
    SplitAtFractionResult splitAtSecond = assertSplitAtFractionBehavior(
        source, numItemsToBeReadBeforeSplit, secondSplitFraction,
        ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS, options);
    if (splitAtFirst.numPrimaryItems != splitAtMiddle.numPrimaryItems
        || splitAtFirst.numResidualItems != splitAtMiddle.numResidualItems) {
      assertSplitAtFractionBinary(source, numItemsToBeReadBeforeSplit, firstSplitFraction,
          middleSplitFraction, options);
    }
    if (splitAtSecond.numPrimaryItems != splitAtMiddle.numPrimaryItems
        || splitAtSecond.numResidualItems != splitAtMiddle.numResidualItems) {
      assertSplitAtFractionBinary(source, numItemsToBeReadBeforeSplit, middleSplitFraction,
          secondSplitFraction, options);
    }
  }

  /**
   * Asserts that for each possible start position,
   * {@link BoundedSource.BoundedReader#splitAtFraction} at every interesting fraction (halfway
   * between two fractions that differ by at least one item) can be called successfully and the
   * results are consistent if a split succeeds.
   */
  public static <T> void assertSplitAtFractionExhaustive(
      BoundedSource<T> source, PipelineOptions options) throws Exception {
    List<T> expectedItems = readFromSource(source, options);
    for (int i = 0; i < expectedItems.size(); i++) {
      assertSplitAtFractionBinary(source, i, 0.0, 1.0, options);
    }
  }
}
