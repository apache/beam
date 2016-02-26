/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static com.google.cloud.dataflow.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests code common to all offset-based sources.
 */
@RunWith(JUnit4.class)
public class OffsetBasedSourceTest {
  // An offset-based source with 4 bytes per offset that yields its own current offset
  // and rounds the start and end offset to the nearest multiple of a given number,
  // e.g. reading [13, 48) with granularity 10 gives records with values [20, 50).
  private static class CoarseRangeSource extends OffsetBasedSource<Integer> {
    private long granularity;

    public CoarseRangeSource(
        long startOffset, long endOffset, long minBundleSize, long granularity) {
      super(startOffset, endOffset, minBundleSize);
      this.granularity = granularity;
    }

    @Override
    public OffsetBasedSource<Integer> createSourceForSubrange(long start, long end) {
      return new CoarseRangeSource(start, end, getMinBundleSize(), granularity);
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return BigEndianIntegerCoder.of();
    }

    @Override
    public long getBytesPerOffset() {
      return 4;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) {
      return getEndOffset();
    }

    @Override
    public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
      return new CoarseRangeReader(this);
    }
  }

  private static class CoarseRangeReader
      extends OffsetBasedSource.OffsetBasedReader<Integer> {
    private long current = -1;
    private long granularity;

    public CoarseRangeReader(CoarseRangeSource source) {
      super(source);
      this.granularity = source.granularity;
    }

    @Override
    protected long getCurrentOffset() {
      return current;
    }

    @Override
    public boolean startImpl() throws IOException {
      current = getCurrentSource().getStartOffset();
      while (current % granularity != 0) {
        ++current;
      }
      return true;
    }

    @Override
    public boolean advanceImpl() throws IOException {
      ++current;
      return true;
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      return (int) current;
    }

    @Override
    public boolean isAtSplitPoint() {
      return current % granularity == 0;
    }

    @Override
    public void close() throws IOException { }
  }

  public static void assertSplitsAre(List<? extends OffsetBasedSource<?>> splits,
      long[] expectedBoundaries) {
    assertEquals(splits.size(), expectedBoundaries.length - 1);
    int i = 0;
    for (OffsetBasedSource<?> split : splits) {
      assertEquals(split.getStartOffset(), expectedBoundaries[i]);
      assertEquals(split.getEndOffset(), expectedBoundaries[i + 1]);
      i++;
    }
  }

  @Test
  public void testSplitPositionsZeroStart() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {0, 150, 300, 450, 600, 750, 900, 1000};
    assertSplitsAre(
        testSource.splitIntoBundles(150 * testSource.getBytesPerOffset(), null),
        boundaries);
  }

  @Test
  public void testSplitPositionsNonZeroStart() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 50;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {300, 450, 600, 750, 900, 1000};
    assertSplitsAre(
        testSource.splitIntoBundles(150 * testSource.getBytesPerOffset(), null),
        boundaries);
  }

  @Test
  public void testEstimatedSizeBytes() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 150;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    PipelineOptions options = PipelineOptionsFactory.create();
    assertEquals(
        (end - start) * testSource.getBytesPerOffset(), testSource.getEstimatedSizeBytes(options));
  }

  @Test
  public void testMinBundleSize() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 150;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {300, 450, 600, 750, 1000};
    assertSplitsAre(
        testSource.splitIntoBundles(100 * testSource.getBytesPerOffset(), null),
        boundaries);
  }

  @Test
  public void testSplitPositionsCollapseEndBundle() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    // Last 10 bytes should collapse to the previous bundle.
    long[] boundaries = {0, 110, 220, 330, 440, 550, 660, 770, 880, 1000};
    assertSplitsAre(
        testSource.splitIntoBundles(110 * testSource.getBytesPerOffset(), null),
        boundaries);
  }

  @Test
  public void testReadingGranularityAndFractionConsumed() throws IOException {
    // Tests that the reader correctly snaps to multiples of the given granularity
    // (note: this is testing test code), and that getFractionConsumed works sensibly
    // in the face of that.
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 35, 1, 10);
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options)) {
      List<Integer> items = new ArrayList<>();

      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertTrue(reader.start());
      do {
        Double fraction = reader.getFractionConsumed();
        assertNotNull(fraction);
        assertTrue(fraction.toString(), fraction > 0.0);
        assertTrue(fraction.toString(), fraction <= 1.0);
        items.add(reader.getCurrent());
      } while (reader.advance());
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);

      assertEquals(20, items.size());
      assertEquals(20, items.get(0).intValue());
      assertEquals(39, items.get(items.size() - 1).intValue());

      source = new CoarseRangeSource(13, 17, 1, 10);
    }
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options)) {
      assertFalse(reader.start());
    }
  }

  @Test
  public void testSplitAtFraction() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 35, 1, 10);
    try (CoarseRangeReader reader = (CoarseRangeReader) source.createReader(options)) {
      List<Integer> originalItems = new ArrayList<>();
      assertTrue(reader.start());
      originalItems.add(reader.getCurrent());
      assertTrue(reader.advance());
      originalItems.add(reader.getCurrent());
      assertTrue(reader.advance());
      originalItems.add(reader.getCurrent());
      assertTrue(reader.advance());
      originalItems.add(reader.getCurrent());
      assertNull(reader.splitAtFraction(0.0));
      assertNull(reader.splitAtFraction(reader.getFractionConsumed() - 0.1));

      BoundedSource<Integer> residual = reader.splitAtFraction(reader.getFractionConsumed() + 0.1);
      BoundedSource<Integer> primary = reader.getCurrentSource();
      List<Integer> primaryItems = readFromSource(primary, options);
      List<Integer> residualItems = readFromSource(residual, options);
      for (Integer item : residualItems) {
        assertTrue(item > reader.getCurrentOffset());
      }
      assertFalse(primaryItems.isEmpty());
      assertFalse(residualItems.isEmpty());
      assertTrue(primaryItems.get(primaryItems.size() - 1) <= residualItems.get(0));

      while (reader.advance()) {
        originalItems.add(reader.getCurrent());
      }
      assertEquals(originalItems, primaryItems);
    }
  }

  @Test
  public void testSplitAtFractionExhaustive() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource original = new CoarseRangeSource(13, 35, 1, 10);
    assertSplitAtFractionExhaustive(original, options);
  }
}
