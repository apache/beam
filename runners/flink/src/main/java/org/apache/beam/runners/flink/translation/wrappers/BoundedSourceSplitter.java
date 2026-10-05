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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits a {@link BoundedSource} into splits that can be evenly distributed over the source
 * readers. Shared by the DataSet and DataStream translations.
 *
 * <p>Beam does not require a source to be fully split in one pass, so the initial splits are
 * refined in several bounded passes:
 *
 * <ol>
 *   <li>Split the source using {@code estimatedSize / numSplits} as the desired split size, capped
 *       by {@link FlinkPipelineOptions#getFileInputSplitMaxSizeMB()} for file sources.
 *   <li>Re-split the splits that are much larger than {@code estimatedSize / (2 x parallelism)}, so
 *       that each reader gets at least {@link #MIN_SPLITS_PER_READER} splits, and as some sources
 *       return splits way larger than requested.
 *   <li>While the split sizes vary a lot (high coefficient of variation), re-split the larger
 *       splits.
 *   <li>Halve the largest splits until round-robin assignment gives each reader about the same
 *       number of splits.
 *   <li>Order the splits so that round-robin assignment gives each reader a similar amount of data.
 * </ol>
 *
 * <p>Steps 3 to 5 need per-split size estimates and are skipped when they are not available.
 */
public final class BoundedSourceSplitter {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceSplitter.class);

  static final long MEBIBYTE = 1024L * 1024L;

  /** Maximum number of re-split passes per refinement step. */
  static final int MAX_RESPLIT_ROUNDS = 3;

  /** Coefficient of variation of split sizes above which larger splits are re-split. */
  static final double MAX_SIZE_COEFFICIENT_OF_VARIATION = 0.5;

  /** Splits larger than this factor times the target size are re-split during balancing. */
  static final double OVERSIZED_SPLIT_FACTOR = 1.5;

  /**
   * How many more splits than the average the busiest reader may get with round-robin assignment,
   * as a fraction of the average. Re-splitting introduces some skew anyway, so the split count does
   * not need to be an exact multiple of the parallelism.
   */
  static final double SPLIT_COUNT_IMBALANCE_TOLERANCE = 0.1;

  /** Lower bound of the balancing target size, relative to {@code estimatedSize/parallelism}. */
  static final int MAX_SPLITS_PER_READER_FOR_BALANCING = 4;

  /**
   * Splits are not re-split below this size by the refinement steps (the initial split is not
   * affected), as the fixed cost of opening and reading a split would dominate. Small inputs still
   * get {@link #MIN_SPLITS_PER_READER} splits per reader, as they may feed transforms that generate
   * a lot more data: the floor is {@code min(MIN_SPLIT_SIZE_BYTES, estimatedSize /
   * (MIN_SPLITS_PER_READER x parallelism))}.
   */
  static final long MIN_SPLIT_SIZE_BYTES = 64 * MEBIBYTE;

  /**
   * Number of splits each reader should get. With static round-robin assignment, two splits per
   * reader let the ordering pair large splits with small ones, while a single split per reader
   * passes any size difference straight to the slowest reader.
   */
  static final int MIN_SPLITS_PER_READER = 2;

  /** How bounded source splits are handed out to readers. */
  public enum Assignment {
    /** Splits are assigned to readers up front, round-robin. */
    STATIC,
    /** Readers pull one split at a time. */
    LAZY,
    /** Static or lazy, depending on the estimated size per reader. */
    SIZE_BASED
  }

  private BoundedSourceSplitter() {}

  /**
   * Resolves the split assignment from {@link
   * FlinkPipelineOptions#getSourceStaticSplitThresholdMb}. When unset, batch pipelines use static
   * assignment and streaming pipelines lazy assignment.
   */
  public static Assignment assignment(PipelineOptions options) {
    @Nullable Long thresholdMb =
        options.as(FlinkPipelineOptions.class).getSourceStaticSplitThresholdMb();
    if (thresholdMb == null) {
      return options.as(StreamingOptions.class).isStreaming() ? Assignment.LAZY : Assignment.STATIC;
    }
    if (thresholdMb < 0) {
      return Assignment.STATIC;
    }
    return thresholdMb == 0 ? Assignment.LAZY : Assignment.SIZE_BASED;
  }

  /** Returns the configured static split threshold in bytes, or 0 when unset. */
  public static long staticSplitThresholdBytes(PipelineOptions options) {
    @Nullable Long thresholdMb =
        options.as(FlinkPipelineOptions.class).getSourceStaticSplitThresholdMb();
    return thresholdMb == null ? 0L : mebibytesToBytes(thresholdMb);
  }

  public static long mebibytesToBytes(long mebibytes) {
    return mebibytes > Long.MAX_VALUE / MEBIBYTE ? Long.MAX_VALUE : mebibytes * MEBIBYTE;
  }

  /** Splits {@code source} for {@code parallelism} readers. */
  public static <T> List<BoundedSource<T>> split(
      BoundedSource<T> source, PipelineOptions options, int parallelism, long estimatedSizeBytes)
      throws Exception {
    return split(source, options, parallelism, parallelism, estimatedSizeBytes);
  }

  /**
   * Splits {@code source} into about {@code numSplits} splits for {@code parallelism} readers.
   * {@code numSplits} sets the desired split size, while {@code parallelism} must be the number of
   * readers splits are assigned to, as split {@code i} is assumed to go to reader {@code i %
   * parallelism}.
   */
  public static <T> List<BoundedSource<T>> split(
      BoundedSource<T> source,
      PipelineOptions options,
      int numSplits,
      int parallelism,
      long estimatedSizeBytes)
      throws Exception {
    int readers = Math.max(1, parallelism);
    long maxSplitSizeBytes = maxSplitSizeBytes(source, options);
    long perReaderTargetBytes =
        Math.max(1L, estimatedSizeBytes / ((long) readers * MIN_SPLITS_PER_READER));
    long minSplitSizeBytes = Math.min(MIN_SPLIT_SIZE_BYTES, perReaderTargetBytes);
    long desiredSizeBytes =
        Math.min(Math.max(1L, estimatedSizeBytes / Math.max(1, numSplits)), maxSplitSizeBytes);

    List<SizedSource<T>> splits = sized(source.split(desiredSizeBytes, options), options);
    int initialSplits = splits.size();

    // Aim for MIN_SPLITS_PER_READER splits per reader. This also handles sources (e.g. BigQuery)
    // that return splits way larger than requested that can still be split further.
    long targetSizeBytes =
        Math.max(Math.min(desiredSizeBytes, perReaderTargetBytes), minSplitSizeBytes);
    splits = resplitLargerThan(splits, options, targetSizeBytes);
    if (allSizesKnown(splits)) {
      long minTargetBytes =
          Math.max(
              minSplitSizeBytes,
              estimatedSizeBytes / ((long) readers * MAX_SPLITS_PER_READER_FOR_BALANCING));
      splits = balanceSizes(splits, options, minTargetBytes, maxSplitSizeBytes, MAX_RESPLIT_ROUNDS);
      splits = balanceSplitCount(splits, options, readers, minSplitSizeBytes);
      splits = orderForRoundRobin(splits, readers);
    }

    LOG.info(
        "Split bounded source {} in {} splits (initially {}; estimated size {} bytes, desired "
            + "split size {} bytes, parallelism {}, split size coefficient of variation {})",
        source,
        splits.size(),
        initialSplits,
        estimatedSizeBytes,
        desiredSizeBytes,
        readers,
        allSizesKnown(splits) ? format(coefficientOfVariation(splits)) : "unknown");

    List<BoundedSource<T>> result = new ArrayList<>(splits.size());
    for (SizedSource<T> split : splits) {
      result.add(split.source);
    }
    return result;
  }

  private static long maxSplitSizeBytes(BoundedSource<?> source, PipelineOptions options) {
    @Nullable Long maxSplitSizeMb =
        options.as(FlinkPipelineOptions.class).getFileInputSplitMaxSizeMB();
    if (source instanceof FileBasedSource && maxSplitSizeMb != null && maxSplitSizeMb > 0) {
      return mebibytesToBytes(maxSplitSizeMb);
    }
    return Long.MAX_VALUE;
  }

  /**
   * Re-splits the splits larger than the mean size (bounded by {@code minTargetBytes} and {@code
   * maxSplitSizeBytes}) while the split sizes vary a lot, at most {@code roundsLeft} times.
   */
  private static <T> List<SizedSource<T>> balanceSizes(
      List<SizedSource<T>> splits,
      PipelineOptions options,
      long minTargetBytes,
      long maxSplitSizeBytes,
      int roundsLeft) {
    int round = MAX_RESPLIT_ROUNDS - roundsLeft + 1;
    double cv = coefficientOfVariation(splits);
    if (cv <= MAX_SIZE_COEFFICIENT_OF_VARIATION) {
      LOG.info(
          "Size balancing round {}: split size coefficient of variation {} is at most {}, done: {}",
          round,
          format(cv),
          MAX_SIZE_COEFFICIENT_OF_VARIATION,
          describe(splits));
      return splits;
    }
    if (roundsLeft == 0) {
      LOG.info(
          "Size balancing: stopping after {} rounds with split size coefficient of variation {}: "
              + "{}",
          MAX_RESPLIT_ROUNDS,
          format(cv),
          describe(splits));
      return splits;
    }
    long targetBytes = Math.min(Math.max((long) mean(splits), minTargetBytes), maxSplitSizeBytes);
    LOG.info(
        "Size balancing round {}: split size coefficient of variation {} is above {}, re-splitting "
            + "towards {} bytes",
        round,
        format(cv),
        MAX_SIZE_COEFFICIENT_OF_VARIATION,
        targetBytes);
    List<SizedSource<T>> next = resplitLargerThan(splits, options, targetBytes);
    if (next.size() > splits.size() && allSizesKnown(next)) {
      return balanceSizes(next, options, minTargetBytes, maxSplitSizeBytes, roundsLeft - 1);
    } else {
      LOG.info(
          "Size balancing round {}: no progress (or unknown sizes), keeping: {}",
          round,
          describe(splits));
      return splits;
    }
  }

  /** Re-splits, once, each split larger than {@link #OVERSIZED_SPLIT_FACTOR} x the target. */
  private static <T> List<SizedSource<T>> resplitLargerThan(
      List<SizedSource<T>> splits, PipelineOptions options, long targetBytes) {
    long thresholdBytes = (long) (OVERSIZED_SPLIT_FACTOR * targetBytes);
    long oversized = splits.stream().filter(split -> split.sizeBytes > thresholdBytes).count();
    List<SizedSource<T>> result =
        splits.stream()
            .flatMap(
                split -> {
                  if (split.sizeBytes > OVERSIZED_SPLIT_FACTOR * targetBytes) {
                    return resplit(split, options, targetBytes).stream();
                  } else {
                    return Stream.of(split);
                  }
                })
            .collect(Collectors.toList());
    LOG.info(
        "Re-split {} of {} splits larger than {} bytes towards {} bytes: {} -> {}",
        oversized,
        splits.size(),
        thresholdBytes,
        targetBytes,
        describe(splits),
        describe(result));
    return result;
  }

  /**
   * Halves the largest splits until round-robin assignment gives each reader about the same number
   * of splits. Splits that cannot be halved are set aside so they are not retried. The result is
   * not ordered.
   */
  private static <T> List<SizedSource<T>> balanceSplitCount(
      List<SizedSource<T>> splits, PipelineOptions options, int readers, long minSplitSizeBytes) {
    PriorityQueue<SizedSource<T>> candidates = new PriorityQueue<>(bySizeDescending());
    candidates.addAll(splits);
    List<SizedSource<T>> unsplittable = new ArrayList<>();
    for (int attempt = 0;
        attempt < 2 * readers
            && !candidates.isEmpty()
            && !isBalancedEnough(candidates.size() + unsplittable.size(), readers);
        attempt++) {
      SizedSource<T> largest = candidates.remove();
      List<SizedSource<T>> halves = halve(largest, options, minSplitSizeBytes);
      if (halves.size() > 1) {
        candidates.addAll(halves);
      } else {
        unsplittable.add(largest);
      }
    }
    List<SizedSource<T>> result = new ArrayList<>(candidates);
    result.addAll(unsplittable);
    LOG.info(
        "Split count balancing for {} readers: {} -> {} splits ({} could not be halved), busiest "
            + "reader gets {} splits for an average of {}",
        readers,
        splits.size(),
        result.size(),
        unsplittable.size(),
        ((long) result.size() + readers - 1) / readers,
        format((double) result.size() / readers));
    return result;
  }

  /**
   * Whether round-robin assignment of {@code count} splits gives the busiest reader at most {@link
   * #SPLIT_COUNT_IMBALANCE_TOLERANCE} more splits than the average. For example 1.9x the
   * parallelism is fine (2 / 1.9 = 1.05), but 1.1x is not (2 / 1.1 = 1.82).
   */
  private static boolean isBalancedEnough(int count, int readers) {
    long busiest = ((long) count + readers - 1) / readers;
    return busiest * readers <= (1 + SPLIT_COUNT_IMBALANCE_TOLERANCE) * count;
  }

  /**
   * Splits {@code split} in two, or returns it alone if that is not possible or would create splits
   * smaller than {@code minSplitSizeBytes}.
   */
  private static <T> List<SizedSource<T>> halve(
      SizedSource<T> split, PipelineOptions options, long minSplitSizeBytes) {
    if (split.sizeBytes <= 1 || split.sizeBytes / 2 < minSplitSizeBytes) {
      return Collections.singletonList(split);
    }
    List<SizedSource<T>> parts = resplit(split, options, split.sizeBytes / 2);
    if (allSizesKnown(parts)) {
      return parts;
    } else {
      return Collections.singletonList(split);
    }
  }

  private static <T> Comparator<SizedSource<T>> bySizeDescending() {
    return Comparator.comparingLong((SizedSource<T> s) -> s.sizeBytes).reversed();
  }

  /**
   * Sorts splits by decreasing size in a "snake" order (0..p-1, p-1..0, ...) so that assigning
   * split {@code i} to reader {@code i % p} spreads the large splits across readers.
   */
  private static <T> List<SizedSource<T>> orderForRoundRobin(
      List<SizedSource<T>> splits, int readers) {
    List<SizedSource<T>> sorted = new ArrayList<>(splits);
    sorted.sort(bySizeDescending());
    List<SizedSource<T>> ordered = new ArrayList<>(sorted.size());
    for (int start = 0; start < sorted.size(); start += readers) {
      int end = Math.min(start + readers, sorted.size());
      List<SizedSource<T>> row = new ArrayList<>(sorted.subList(start, end));
      if ((start / readers) % 2 == 1) {
        Collections.reverse(row);
      }
      ordered.addAll(row);
    }
    LOG.info(
        "Ordered {} splits for round-robin over {} readers: busiest reader load / average went "
            + "from {} to {}",
        ordered.size(),
        readers,
        format(busiestReaderLoad(splits, readers)),
        format(busiestReaderLoad(ordered, readers)));
    return ordered;
  }

  /** Bytes of the busiest reader relative to the average, with split i on reader i % readers. */
  private static double busiestReaderLoad(List<? extends SizedSource<?>> splits, int readers) {
    long[] loads = new long[readers];
    long total = 0;
    int index = 0;
    for (SizedSource<?> split : splits) {
      loads[index++ % readers] += split.sizeBytes;
      total += split.sizeBytes;
    }
    if (total <= 0) {
      return 1.0;
    }
    return Arrays.stream(loads).max().getAsLong() / ((double) total / readers);
  }

  private static String format(double value) {
    return String.format("%.3f", value);
  }

  /** Short description of the splits for logging. */
  private static String describe(List<? extends SizedSource<?>> splits) {
    if (!allSizesKnown(splits)) {
      return splits.size() + " splits (sizes unknown)";
    }
    LongSummaryStatistics sizes = new LongSummaryStatistics();
    for (SizedSource<?> split : splits) {
      sizes.accept(split.sizeBytes);
    }
    return String.format(
        "%d splits, size min/mean/max %d/%.0f/%d bytes, coefficient of variation %.3f",
        sizes.getCount(),
        sizes.getMin(),
        sizes.getAverage(),
        sizes.getMax(),
        coefficientOfVariation(splits));
  }

  private static <T> List<SizedSource<T>> resplit(
      SizedSource<T> split, PipelineOptions options, long desiredSizeBytes) {
    try {
      List<? extends BoundedSource<T>> parts = split.source.split(desiredSizeBytes, options);
      if (parts.isEmpty()) {
        return Collections.singletonList(split);
      }
      return sized(parts, options);
    } catch (Exception e) {
      LOG.debug("Could not re-split source {}. Keeping it as is.", split.source, e);
      return Collections.singletonList(split);
    }
  }

  private static <T> List<SizedSource<T>> sized(
      List<? extends BoundedSource<T>> sources, PipelineOptions options) {
    List<SizedSource<T>> result = new ArrayList<>(sources.size());
    for (BoundedSource<T> source : sources) {
      long size;
      try {
        size = source.getEstimatedSizeBytes(options);
      } catch (Exception e) {
        size = -1L;
      }
      result.add(new SizedSource<>(source, size));
    }
    return result;
  }

  private static boolean allSizesKnown(List<? extends SizedSource<?>> splits) {
    for (SizedSource<?> split : splits) {
      if (split.sizeBytes < 0 || split.sizeBytes == Long.MAX_VALUE) {
        return false;
      }
    }
    return !splits.isEmpty();
  }

  private static double mean(List<? extends SizedSource<?>> splits) {
    double sum = 0;
    for (SizedSource<?> split : splits) {
      sum += split.sizeBytes;
    }
    return sum / splits.size();
  }

  static double coefficientOfVariation(List<? extends SizedSource<?>> splits) {
    double mean = mean(splits);
    if (mean <= 0) {
      return 0;
    }
    double variance = 0;
    for (SizedSource<?> split : splits) {
      double delta = split.sizeBytes - mean;
      variance += delta * delta;
    }
    return Math.sqrt(variance / splits.size()) / mean;
  }

  private static final class SizedSource<T> {
    private final BoundedSource<T> source;
    private final long sizeBytes;

    private SizedSource(BoundedSource<T> source, long sizeBytes) {
      this.source = source;
      this.sizeBytes = sizeBytes;
    }
  }
}
