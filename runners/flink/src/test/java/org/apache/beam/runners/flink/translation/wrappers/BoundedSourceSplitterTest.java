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
package org.apache.beam.runners.flink.translation.wrappers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.BoundedSourceSplitter.Assignment;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Test;

/** Tests for {@link BoundedSourceSplitter}. */
public class BoundedSourceSplitterTest {

  private static final long MIB = BoundedSourceSplitter.MEBIBYTE;
  private static final long GIB = 1024 * MIB;

  private final FlinkPipelineOptions options = FlinkPipelineOptions.defaults();

  @Test
  public void testResolvesAssignment() {
    FlinkPipelineOptions batch = FlinkPipelineOptions.defaults();
    assertEquals(Assignment.STATIC, BoundedSourceSplitter.assignment(batch));

    FlinkPipelineOptions streaming = FlinkPipelineOptions.defaults();
    streaming.setStreaming(true);
    assertEquals(Assignment.LAZY, BoundedSourceSplitter.assignment(streaming));

    batch.setSourceStaticSplitThresholdMb(0L);
    assertEquals(Assignment.LAZY, BoundedSourceSplitter.assignment(batch));
    batch.setSourceStaticSplitThresholdMb(-1L);
    assertEquals(Assignment.STATIC, BoundedSourceSplitter.assignment(batch));
    batch.setSourceStaticSplitThresholdMb(10L);
    assertEquals(Assignment.SIZE_BASED, BoundedSourceSplitter.assignment(batch));
  }

  @Test
  public void testResplitsWhenTooFewSplits() throws Exception {
    // The first split ignores the desired size and only produces two splits.
    BoundedSource<Long> source = new CoarseSource(Collections.singletonList(range(0, 8000)));

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 8, 8000);

    // Two splits per reader.
    assertEquals(16, splits.size());
    assertEquals(8000, totalSize(splits));
    for (BoundedSource<Long> split : splits) {
      assertEquals(500, split.getEstimatedSizeBytes(options));
    }
  }

  @Test
  public void testResplitsLargeSplits() throws Exception {
    // One large file and many small ones.
    List<RangeSource> files = new ArrayList<>();
    files.add(range(0, 10_000));
    for (int i = 0; i < 12; i++) {
      files.add(range(10_000 + i * 100, 10_100 + i * 100));
    }
    BoundedSource<Long> source = new CoarseSource(files);

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 4, 11_200);

    assertEquals(11_200, totalSize(splits));
    long[] perReader = perReaderSizes(splits, 4);
    long max = Arrays.stream(perReader).max().getAsLong();
    long min = Arrays.stream(perReader).min().getAsLong();
    assertTrue("Unbalanced readers: " + Arrays.toString(perReader), max - min <= 11_200 / 4 / 2);
  }

  @Test
  public void testResplitsFilesToTwoSplitsPerReader() throws Exception {
    // Each 1 GiB file is re-split to the 640 MiB target (2 splits per reader): 10 splits of 640 and
    // 384 MiB. The busiest of the 4 readers gets 1408 MiB for an average of 1280 MiB, within 10%,
    // so no split is halved.
    List<BoundedSource<Long>> splits =
        BoundedSourceSplitter.split(equalFiles(5, GIB), options, 4, 5 * GIB);

    assertEquals(10, splits.size());
    assertEquals(5 * GIB, totalSize(splits));
    assertEquals(1408 * MIB, Arrays.stream(perReaderSizes(splits, 4)).max().getAsLong());
  }

  @Test
  public void testAcceptsSplitCountWithManySplitsPerReader() throws Exception {
    // 39 splits for 4 readers: the busiest reader gets 10 splits for an average of 9.75.
    List<BoundedSource<Long>> splits =
        BoundedSourceSplitter.split(equalFiles(39, 1000), options, 4, 39_000);

    assertEquals(39, splits.size());
  }

  @Test
  public void testHalvesSplitsWithFewSplitsPerReader() throws Exception {
    // Each 1 GiB file is re-split to the ~563 MiB target (2 splits per reader): 22 splits, so the
    // busiest of the 10 readers gets 3 splits for an average of 2.2. Halving stops at 28 splits: 3
    // splits for an average of 2.8.
    List<BoundedSource<Long>> splits =
        BoundedSourceSplitter.split(equalFiles(11, GIB), options, 10, 11 * GIB);

    assertEquals(28, splits.size());
    assertEquals(11 * GIB, totalSize(splits));
  }

  @Test
  public void testSkipsSplitCountBalancingWhenBytesAreBalanced() throws Exception {
    // Like 4 files re-split into 10 full pieces and a smaller remainder each. 44 splits for 21
    // readers is uneven by count (3 splits for an average of 2.1), but the round-robin ordering
    // gives the readers with 3 splits the small remainders, so bytes are balanced.
    List<RangeSource> pieces = new ArrayList<>();
    long offset = 0;
    for (int i = 0; i < 44; i++) {
      long size = (i % 11 == 10 ? 845 : 1790) * MIB;
      pieces.add(range(offset, offset + size));
      offset += size;
    }
    BoundedSource<Long> source = new CoarseSource(pieces);

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 21, offset);

    assertEquals(44, splits.size());
    long[] perReader = perReaderSizes(splits, 21);
    long max = Arrays.stream(perReader).max().getAsLong();
    assertTrue("Unbalanced readers: " + Arrays.toString(perReader), max <= 1.1 * offset / 21);
  }

  @Test
  public void testDoesNotHalveBelowMinSplitSize() throws Exception {
    // The 11 x 100 MiB splits are below the 110 MiB target for 5 readers, but the busiest reader
    // gets 3 splits for an average of 2.2. Halving would create splits below the 64 MiB minimum.
    List<BoundedSource<Long>> splits =
        BoundedSourceSplitter.split(equalFiles(11, 100 * MIB), options, 5, 1100 * MIB);

    assertEquals(11, splits.size());
  }

  @Test
  public void testSmallInputStillGetsTwoSplitsPerReader() throws Exception {
    // A 10 MiB input is below the minimum split size, but may feed transforms generating a lot
    // more data, so it is still spread over all readers.
    BoundedSource<Long> source = new CoarseSource(Collections.singletonList(range(0, 10 * MIB)));

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 10, 10 * MIB);

    assertEquals(20, splits.size());
  }

  private static BoundedSource<Long> equalFiles(int count, long sizeBytes) {
    List<RangeSource> files = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      files.add(range(i * sizeBytes, (i + 1) * sizeBytes));
    }
    return new CoarseSource(files);
  }

  @Test
  public void testOrdersSplitsForParallelismRatherThanNumSplits() throws Exception {
    BoundedSource<Long> source =
        new CoarseSource(
            Arrays.asList(
                new UnsplittableSource(4),
                new UnsplittableSource(3),
                new UnsplittableSource(2),
                new UnsplittableSource(1)));

    // 4 splits requested, but assigned round-robin to 2 readers.
    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 4, 2, 10);

    assertArrayEquals(new long[] {5, 5}, perReaderSizes(splits, 2));
  }

  @Test
  public void testKeepsUnsplittableSources() throws Exception {
    BoundedSource<Long> source =
        new CoarseSource(Arrays.asList(new UnsplittableSource(1), new UnsplittableSource(1)));

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 4, 2);

    assertEquals(2, splits.size());
  }

  private static RangeSource range(long start, long end) {
    return new RangeSource(start, end);
  }

  private long totalSize(List<BoundedSource<Long>> splits) throws Exception {
    long total = 0;
    for (BoundedSource<Long> split : splits) {
      total += split.getEstimatedSizeBytes(options);
    }
    return total;
  }

  private long[] perReaderSizes(List<BoundedSource<Long>> splits, int readers) throws Exception {
    long[] sizes = new long[readers];
    for (int i = 0; i < splits.size(); i++) {
      sizes[i % readers] += splits.get(i).getEstimatedSizeBytes(options);
    }
    return sizes;
  }

  private abstract static class TestSource extends BoundedSource<Long> {
    @Override
    public BoundedReader<Long> createReader(PipelineOptions options) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }
  }

  /** Splits into its children regardless of the desired size, like a multi-file source. */
  private static class CoarseSource extends TestSource {
    private final List<? extends BoundedSource<Long>> children;

    CoarseSource(List<? extends BoundedSource<Long>> children) {
      this.children = children;
    }

    @Override
    public List<? extends BoundedSource<Long>> split(long desiredSize, PipelineOptions options)
        throws Exception {
      if (children.size() == 1) {
        RangeSource only = (RangeSource) children.get(0);
        long middle = (only.start + only.end) / 2;
        return Arrays.asList(range(only.start, middle), range(middle, only.end));
      }
      return children;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      long size = 0;
      for (BoundedSource<Long> child : children) {
        size += child.getEstimatedSizeBytes(options);
      }
      return size;
    }
  }

  /** One byte per element, splits honoring the desired size. */
  private static class RangeSource extends TestSource {
    private final long start;
    private final long end;

    RangeSource(long start, long end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public List<RangeSource> split(long desiredSize, PipelineOptions options) {
      List<RangeSource> splits = new ArrayList<>();
      long step = Math.max(1, desiredSize);
      for (long s = start; s < end; s += step) {
        splits.add(range(s, Math.min(end, s + step)));
      }
      return splits;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return end - start;
    }
  }

  private static class UnsplittableSource extends TestSource {
    private final long sizeBytes;

    UnsplittableSource(long sizeBytes) {
      this.sizeBytes = sizeBytes;
    }

    @Override
    public List<UnsplittableSource> split(long desiredSize, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      return sizeBytes;
    }
  }
}
