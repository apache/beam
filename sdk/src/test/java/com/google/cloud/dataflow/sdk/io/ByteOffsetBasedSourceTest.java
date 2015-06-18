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

import static com.google.cloud.dataflow.sdk.io.SourceTestUtils.assertSplitAtFractionExhaustive;
import static com.google.cloud.dataflow.sdk.io.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Tests code common to all offset-based sources.
 */
@RunWith(JUnit4.class)
public class ByteOffsetBasedSourceTest {

  // A byte-offset based source that yields its own current offset
  // and rounds the start and end offset to the nearest multiple of a given number,
  // e.g. reading [13, 48) with granularity 10 gives records with values [20, 50).
  private static class CoarseByteRangeSource extends ByteOffsetBasedSource<Integer> {
    private static final long serialVersionUID = 0L;
    private long granularity;

    public CoarseByteRangeSource(
        long startOffset, long endOffset, long minBundleSize, long granularity) {
      super(startOffset, endOffset, minBundleSize);
      this.granularity = granularity;
    }

    @Override
    public ByteOffsetBasedSource<Integer> createSourceForSubrange(long start, long end) {
      return new CoarseByteRangeSource(start, end, getMinBundleSize(), granularity);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<Integer> getDefaultOutputCoder() {
      return null;
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) {
      return getEndOffset();
    }

    @Override
    public BoundedReader<Integer> createReader(
        PipelineOptions options, @Nullable ExecutionContext executionContext)
        throws IOException {
      return new CoarseByteRangeReader(this);
    }
  }

  private static class CoarseByteRangeReader
      extends ByteOffsetBasedSource.ByteOffsetBasedReader<Integer> {
    private long current = -1;
    private long granularity;

    public CoarseByteRangeReader(CoarseByteRangeSource source) {
      super(source);
      this.granularity = source.granularity;
    }

    @Override
    protected long getCurrentOffset() {
      return current;
    }

    @Override
    public boolean start() throws IOException {
      current = getCurrentSource().getStartOffset();
      while (true) {
        if (current >= getCurrentSource().getEndOffset()) {
          return false;
        }
        if (current % granularity == 0) {
          return true;
        }
        ++current;
      }
    }

    @Override
    public boolean advance() throws IOException {
      ++current;
      return !(current >= getCurrentSource().getEndOffset() && current % granularity == 0);
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      return (int) current;
    }

    @Override
    public void close() throws IOException { }
  }

  public static void assertSplitsAre(List<? extends ByteOffsetBasedSource<?>> splits,
      long[] expectedBoundaries) {
    assertEquals(splits.size(), expectedBoundaries.length - 1);
    int i = 0;
    for (ByteOffsetBasedSource<?> split : splits) {
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
    long desiredBundleSize = 150;
    CoarseByteRangeSource testSource = new CoarseByteRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {0, 150, 300, 450, 600, 750, 900, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testSplitPositionsNonZeroStart() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 50;
    long desiredBundleSize = 150;
    CoarseByteRangeSource testSource = new CoarseByteRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {300, 450, 600, 750, 900, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testMinBundleSize() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 150;
    long desiredBundleSize = 100;
    CoarseByteRangeSource testSource = new CoarseByteRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {300, 450, 600, 750, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testSplitPositionsCollapseEndBundle() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    long desiredBundleSize = 110;
    CoarseByteRangeSource testSource = new CoarseByteRangeSource(start, end, minBundleSize, 1);
    // Last 10 bytes should collapse to the previous bundle.
    long[] boundaries = {0, 110, 220, 330, 440, 550, 660, 770, 880, 1000};
    assertSplitsAre(testSource.splitIntoBundles(desiredBundleSize, null), boundaries);
  }

  @Test
  public void testReadingGranularityAndFractionConsumed() throws IOException {
    // Tests that the reader correctly snaps to multiples of the given granularity
    // (note: this is testing test code), and that getFractionConsumed works sensibly
    // in the face of that.
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseByteRangeSource source = new CoarseByteRangeSource(13, 35, 1, 10);
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options, null)) {
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

      source = new CoarseByteRangeSource(13, 17, 1, 10);
    }
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options, null)) {
      assertFalse(reader.start());
    }
  }

  @Test
  public void testSplitAtFraction() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseByteRangeSource source = new CoarseByteRangeSource(13, 35, 1, 10);
    try (CoarseByteRangeReader reader =
            (CoarseByteRangeReader) source.createReader(options, null)) {
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
      assertNull(reader.splitAtFraction(reader.getFractionConsumed()));

      Source<Integer> residual = reader.splitAtFraction(reader.getFractionConsumed() + 0.1);
      Source<Integer> primary = reader.getCurrentSource();
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
  public void testSplitAtFractionExhaustive() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseByteRangeSource original = new CoarseByteRangeSource(13, 35, 1, 10);
    assertSplitAtFractionExhaustive(original, options);
  }
}
