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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.OffsetBasedSource.OffsetBasedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests code common to all offset-based sources. */
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
    public Coder<Integer> getOutputCoder() {
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
    public CoarseRangeReader createReader(PipelineOptions options) {
      return new CoarseRangeReader(this);
    }
  }

  private static class CoarseRangeReader extends OffsetBasedReader<Integer> {
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
    public boolean startImpl() {
      current = getCurrentSource().getStartOffset();
      while (current % granularity != 0) {
        ++current;
      }
      return true;
    }

    @Override
    public boolean advanceImpl() {
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
    public void close() {}
  }

  public static void assertSplitsAre(
      List<? extends OffsetBasedSource<?>> splits, long[] expectedBoundaries) {
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
    assertSplitsAre(testSource.split(150 * testSource.getBytesPerOffset(), null), boundaries);
  }

  @Test
  public void testSplitPositionsNonZeroStart() throws Exception {
    long start = 300;
    long end = 1000;
    long minBundleSize = 50;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    long[] boundaries = {300, 450, 600, 750, 900, 1000};
    assertSplitsAre(testSource.split(150 * testSource.getBytesPerOffset(), null), boundaries);
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
    assertSplitsAre(testSource.split(100 * testSource.getBytesPerOffset(), null), boundaries);
  }

  @Test
  public void testSplitPositionsCollapseEndBundle() throws Exception {
    long start = 0;
    long end = 1000;
    long minBundleSize = 50;
    CoarseRangeSource testSource = new CoarseRangeSource(start, end, minBundleSize, 1);
    // Last 10 bytes should collapse to the previous bundle.
    long[] boundaries = {0, 110, 220, 330, 440, 550, 660, 770, 880, 1000};
    assertSplitsAre(testSource.split(110 * testSource.getBytesPerOffset(), null), boundaries);
  }

  @Test
  public void testReadingGranularityAndFractionConsumed() throws IOException {
    // Tests that the reader correctly snaps to multiples of the given granularity
    // (note: this is testing test code), and that getFractionConsumed works sensibly
    // in the face of that.
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 35, 1, 10);
    try (CoarseRangeReader reader = source.createReader(options)) {
      List<Integer> items = new ArrayList<>();

      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertTrue(reader.start());
      items.add(reader.getCurrent());
      while (reader.advance()) {
        Double fraction = reader.getFractionConsumed();
        assertNotNull(fraction);
        assertTrue(fraction.toString(), fraction > 0.0);
        assertTrue(fraction.toString(), fraction <= 1.0);
        items.add(reader.getCurrent());
      }
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
  public void testProgress() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 17, 1, 2);
    try (OffsetBasedReader<Integer> reader = source.createReader(options)) {
      // Unstarted reader
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Start and produce the element 14 since granularity is 2.
      assertTrue(reader.start());
      assertTrue(reader.isAtSplitPoint());
      assertEquals(14, reader.getCurrent().intValue());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());
      // Advance and produce the element 15, not a split point.
      assertTrue(reader.advance());
      assertEquals(15, reader.getCurrent().intValue());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Advance and produce the element 16, is a split point. Since the next offset (17) is
      // outside the range [13, 17), remaining parallelism should become 1 from UNKNOWN.
      assertTrue(reader.advance());
      assertTrue(reader.isAtSplitPoint());
      assertEquals(16, reader.getCurrent().intValue());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining()); // The next offset is outside the range.
      // Advance and produce the element 17, not a split point.
      assertTrue(reader.advance());
      assertEquals(17, reader.getCurrent().intValue());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(1, reader.getSplitPointsRemaining());

      // Advance and reach the end of the reader.
      assertFalse(reader.advance());
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(2, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testProgressEmptySource() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 17, 1, 100);
    try (OffsetBasedReader<Integer> reader = source.createReader(options)) {
      // before starting
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // confirm empty
      assertFalse(reader.start());

      // after reading empty source
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
    }
  }

  @Test
  public void testSplitAtFraction() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    CoarseRangeSource source = new CoarseRangeSource(13, 35, 1, 10);
    try (CoarseRangeReader reader = source.createReader(options)) {
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

  @Test
  public void testEmptyOffsetRange() throws Exception {
    CoarseRangeSource empty = new CoarseRangeSource(0, 0, 1, 1);
    try (CoarseRangeReader reader = empty.createReader(PipelineOptionsFactory.create())) {
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(OffsetBasedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());
      assertEquals(0.0, reader.getFractionConsumed(), 0.0001);

      assertFalse(reader.start());

      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(0, reader.getSplitPointsRemaining());
      assertEquals(1.0, reader.getFractionConsumed(), 0.0001);
    }
  }
}
