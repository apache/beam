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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

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
  public static <T> List<T> readFromSource(Source<T> source) throws IOException {
    return readFromUnstartedReader(source.createReader(
        PipelineOptionsFactory.create(), null));
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
   * Asserts that the {@code source}'s reader either fails to {@code splitAtFraction(fraction)}
   * after reading {@code numItemsToReadBeforeSplit} items, or succeeds in a way that is
   * consistent according to {@link #assertSplitAtFractionSucceedsAndConsistent}.
   */
  public static <T> void assertSplitAtFractionBehavior(
      BoundedSource<T> source, int numItemsToReadBeforeSplit, double splitFraction,
      ExpectedSplitOutcome expectedOutcome) throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<T> expectedItems = readFromSource(source);
    BoundedSource.BoundedReader<T> reader = source.createReader(options, null);
    List<T> currentItems = new ArrayList<>();
    currentItems.addAll(readNItemsFromUnstartedReader(reader, numItemsToReadBeforeSplit));
    BoundedSource<T> residual = reader.splitAtFraction(splitFraction);
    // Failure cases are: must succeed but fails; must fail but succeeds.
    switch(expectedOutcome) {
      case MUST_SUCCEED_AND_BE_CONSISTENT:
        assertNotNull(
            "Failed to split reader of source: " + source + " at " + splitFraction
                + " after reading " + numItemsToReadBeforeSplit + " items", residual);
        break;
      case MUST_FAIL:
        assertEquals(null, residual);
        break;
      case MUST_BE_CONSISTENT_IF_SUCCEEDS:
        // Nothing.
        break;
    }
    if (residual != null) {
      BoundedSource<T> primary = reader.getCurrentSource();
      List<T> primaryItems = readFromSource(primary);
      List<T> residualItems = readFromSource(residual);
      List<T> totalItems = new ArrayList<>();
      totalItems.addAll(primaryItems);
      totalItems.addAll(residualItems);
      currentItems.addAll(numItemsToReadBeforeSplit > 0
          ? readFromStartedReader(reader) : readFromUnstartedReader(reader));
      assertEquals(
          "Continued reading after split yielded different items than primary source: "
            + " split at " + splitFraction + " after reading " + numItemsToReadBeforeSplit
            + " items, original source: " + source + ", primary source: " + primary,
          primaryItems, currentItems);
      assertEquals(
          "Items in primary and residual sources after split do not add up "
            + "to items in the original source. "
            + "Split at " + splitFraction + " after reading " + numItemsToReadBeforeSplit
            + " items; original source: " + source + ", primary: " + primary
            + ", residual: " + residual,
          expectedItems, totalItems);
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
      BoundedSource<T> source, int numItemsToReadBeforeSplit, double splitFraction)
      throws IOException {
    assertSplitAtFractionBehavior(
        source, numItemsToReadBeforeSplit, splitFraction,
        ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT);
  }

  /**
   * Asserts that the {@code source}'s reader fails to {@code splitAtFraction(fraction)}
   * after reading {@code numItemsToReadBeforeSplit} items.
   */
  public static <T> void assertSplitAtFractionFails(
      BoundedSource<T> source, int numItemsToReadBeforeSplit, double splitFraction)
      throws IOException {
    assertSplitAtFractionBehavior(
        source, numItemsToReadBeforeSplit, splitFraction,
        ExpectedSplitOutcome.MUST_FAIL);
  }
}
