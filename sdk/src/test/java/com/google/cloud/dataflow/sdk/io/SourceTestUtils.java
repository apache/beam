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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for testing {@link Source} classes.
 */
public class SourceTestUtils {
  /**
   * Reads all elements from the given {@link Source}.
   */
  public static <T> List<T> readFromSource(Source<T> source, PipelineOptions options)
      throws IOException {
    try (Source.Reader<T> reader = source.createReader(options, null)) {
      return readFromUnstartedReader(reader);
    }
  }

  /**
   * Reads all elements from the given unstarted {@link Source.Reader}.
   */
  public static <T> List<T> readFromUnstartedReader(Source.Reader<T> reader) throws IOException {
    List<T> res = new ArrayList<>();
    for (boolean more = reader.start(); more; more = reader.advance()) {
      res.add(reader.getCurrent());
    }
    return res;
  }

  public static <T> List<T> readFromStartedReader(Source.Reader<T> reader) throws IOException {
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
  public static <T> void assertSourcesEqualReferenceSource(Source<T> referenceSource,
      List<? extends Source<T>> sources, PipelineOptions options) throws IOException {
    List<T> referenceRecords = readFromSource(referenceSource, options);
    List<T> bundleRecords = new ArrayList<>();
    for (Source<T> source : sources) {
      List<T> elems = readFromSource(source, options);
      bundleRecords.addAll(elems);
    }
    assertThat(bundleRecords, containsInAnyOrder(referenceRecords.toArray()));
  }

  /**
   * Assert that a {@code Reader} returns a {@code Source} that, when read from, produces the same
   * records as the reader.
   */
  public static <T> void assertUnstartedReaderReadsSameAsItsSource(
      Source.Reader<T> reader, PipelineOptions options) throws IOException {
    List<T> expected = readFromUnstartedReader(reader);
    List<T> actual = readFromSource(reader.getCurrentSource(), options);
    assertEquals(expected, actual);
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
      BoundedSource<T> source, int numItemsToReadBeforeSplit, double splitFraction,
      ExpectedSplitOutcome expectedOutcome, PipelineOptions options) throws IOException {
    List<T> expectedItems = readFromSource(source, options);
    try (BoundedSource.BoundedReader<T> reader = source.createReader(options, null)) {
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
        assertEquals(
            "Continued reading after split yielded different items than primary source: "
                + " split at "
                + splitFraction
                + " after reading "
                + numItemsToReadBeforeSplit
                + " items, original source: "
                + source
                + ", primary source: "
                + primary,
            primaryItems,
            currentItems);
        assertEquals(
            "Items in primary and residual sources after split do not add up "
                + "to items in the original source. "
                + "Split at "
                + splitFraction
                + " after reading "
                + numItemsToReadBeforeSplit
                + " items; original source: "
                + source
                + ", primary: "
                + primary
                + ", residual: "
                + residual,
            expectedItems,
            totalItems);
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
  public static <T> void assertSplitAtFractionSucceedsAndConsistent(BoundedSource<T> source,
      int numItemsToReadBeforeSplit, double splitFraction, PipelineOptions options)
      throws IOException {
    assertSplitAtFractionBehavior(source, numItemsToReadBeforeSplit, splitFraction,
        ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT, options);
  }

  /**
   * Asserts that the {@code source}'s reader fails to {@code splitAtFraction(fraction)}
   * after reading {@code numItemsToReadBeforeSplit} items.
   */
  public static <T> void assertSplitAtFractionFails(BoundedSource<T> source,
      int numItemsToReadBeforeSplit, double splitFraction, PipelineOptions options)
      throws IOException {
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
      BoundedSource<T> source, int numItemsToBeReadBeforeSplit, double firstSplitFraction,
      double secondSplitFraction, PipelineOptions options) throws IOException {
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
      BoundedSource<T> source, PipelineOptions options) throws IOException {
    List<T> expectedItems = readFromSource(source, options);
    for (int i = 0; i < expectedItems.size(); i++) {
      assertSplitAtFractionBinary(source, i, 0.0, 1.0, options);
    }
  }
}
