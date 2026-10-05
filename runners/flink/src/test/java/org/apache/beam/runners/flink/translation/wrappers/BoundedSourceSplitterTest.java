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

    assertEquals(8, splits.size());
    assertEquals(8000, totalSize(splits));
    for (BoundedSource<Long> split : splits) {
      assertEquals(1000, split.getEstimatedSizeBytes(options));
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
  public void testRoundsSplitCountToMultipleOfParallelism() throws Exception {
    List<RangeSource> files = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      files.add(range(i * 1000, (i + 1) * 1000));
    }
    BoundedSource<Long> source = new CoarseSource(files);

    List<BoundedSource<Long>> splits = BoundedSourceSplitter.split(source, options, 4, 5000);

    assertEquals(0, splits.size() % 4);
    assertEquals(5000, totalSize(splits));
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
    // 11 splits for 10 readers: the busiest reader would get 2 splits for an average of 1.1.
    // Halving stops at 19 splits: 2 splits for an average of 1.9.
    List<BoundedSource<Long>> splits =
        BoundedSourceSplitter.split(equalFiles(11, 1000), options, 10, 11_000);

    assertEquals(19, splits.size());
    assertEquals(11_000, totalSize(splits));
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
