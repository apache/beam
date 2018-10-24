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
package org.apache.beam.sdk.io.synthetic;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.math3.stat.StatUtils.sum;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link SyntheticBoundedIO} class provides a parameterizable batch custom source that is
 * deterministic.
 *
 * <p>The {@link SyntheticBoundedSource} generates a {@link PCollection} of {@code KV<byte[],
 * byte[]>}. A fraction of the generated records {@code KV<byte[], byte[]>} are associated with
 * "hot" keys, which are uniformly distributed over a fixed number of hot keys. The remaining
 * generated records are associated with "random" keys. Each record will be slowed down by a certain
 * sleep time generated based on the specified sleep time distribution when the {@link
 * SyntheticSourceReader} reads each record. The record {@code KV<byte[], byte[]>} is generated
 * deterministically based on the record's position in the source, which enables repeatable
 * execution for debugging. The SyntheticBoundedInput configurable parameters are defined in {@link
 * SyntheticBoundedIO.SyntheticSourceOptions}.
 *
 * <p>To read a {@link PCollection} of {@code KV<byte[], byte[]>} from {@link SyntheticBoundedIO},
 * use {@link SyntheticBoundedIO#readFrom} to construct the synthetic source with synthetic source
 * options. See {@link SyntheticBoundedIO.SyntheticSourceOptions} for how to construct an instance.
 * An example is below:
 *
 * <pre>{@code
 * Pipeline p = ...;
 * SyntheticBoundedInput.SourceOptions sso = ...;
 *
 * // Construct the synthetic input with synthetic source options.
 * PCollection<KV<byte[], byte[]>> input = p.apply(SyntheticBoundedInput.readFrom(sso));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class SyntheticBoundedIO {
  /** Read from the synthetic source options. */
  public static Read.Bounded<KV<byte[], byte[]>> readFrom(SyntheticSourceOptions options) {
    checkNotNull(options, "Input synthetic source options should not be null.");
    return Read.from(new SyntheticBoundedSource(options));
  }

  /** A {@link SyntheticBoundedSource} that reads {@code KV<byte[], byte[]>}. */
  public static class SyntheticBoundedSource extends OffsetBasedSource<KV<byte[], byte[]>> {
    private static final long serialVersionUID = 0;
    private static final Logger LOG = LoggerFactory.getLogger(SyntheticBoundedSource.class);

    private final SyntheticSourceOptions sourceOptions;

    public SyntheticBoundedSource(SyntheticSourceOptions sourceOptions) {
      this(0, sourceOptions.numRecords, sourceOptions);
    }

    SyntheticBoundedSource(long startOffset, long endOffset, SyntheticSourceOptions sourceOptions) {
      super(startOffset, endOffset, 1);
      this.sourceOptions = sourceOptions;
      LOG.debug("Constructing {}", toString());
    }

    @Override
    public Coder<KV<byte[], byte[]>> getDefaultOutputCoder() {
      return KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of());
    }

    @Override
    // TODO: test cases where the source size could not be estimated (i.e., return 0).
    // TODO: test cases where the key size and value size might differ from record to record.
    // The key size and value size might have their own distributions.
    public long getBytesPerOffset() {
      return sourceOptions.bytesPerRecord >= 0
          ? sourceOptions.bytesPerRecord
          : sourceOptions.keySizeBytes + sourceOptions.valueSizeBytes;
    }

    @Override
    public void validate() {
      super.validate();
      sourceOptions.validate();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("options", sourceOptions)
          .add("offsetRange", "[" + getStartOffset() + ", " + getEndOffset() + ")")
          .toString();
    }

    @Override
    public final SyntheticBoundedSource createSourceForSubrange(long start, long end) {
      checkArgument(
          start >= getStartOffset(),
          "Start offset value "
              + start
              + " of the subrange cannot be smaller than the start offset value "
              + getStartOffset()
              + " of the parent source");
      checkArgument(
          end <= getEndOffset(),
          "End offset value "
              + end
              + " of the subrange cannot be larger than the end offset value "
              + getEndOffset()
              + " of the parent source");

      return new SyntheticBoundedSource(start, end, sourceOptions);
    }

    @Override
    public long getMaxEndOffset(PipelineOptions options) {
      return getEndOffset();
    }

    @Override
    public SyntheticSourceReader createReader(PipelineOptions pipelineOptions) {
      return new SyntheticSourceReader(this);
    }

    @Override
    public List<SyntheticBoundedSource> split(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      // Choose number of bundles either based on explicit parameter,
      // or based on size and hints.
      int desiredNumBundles =
          (sourceOptions.forceNumInitialBundles == null)
              ? ((int) Math.ceil(1.0 * getEstimatedSizeBytes(options) / desiredBundleSizeBytes))
              : sourceOptions.forceNumInitialBundles;

      List<SyntheticBoundedSource> res =
          generateBundleSizes(desiredNumBundles)
              .stream()
              .map(
                  offsetRange ->
                      createSourceForSubrange(offsetRange.getFrom(), offsetRange.getTo()))
              .collect(Collectors.toList());
      LOG.info("Split into {} bundles of sizes: {}", res.size(), res);
      return res;
    }

    private List<OffsetRange> generateBundleSizes(int desiredNumBundles) {
      List<OffsetRange> result = new ArrayList<>();

      // Generate relative bundle sizes using the given distribution.
      double[] relativeSizes = new double[desiredNumBundles];
      for (int i = 0; i < relativeSizes.length; ++i) {
        relativeSizes[i] =
            sourceOptions.bundleSizeDistribution.sample(
                sourceOptions.hashFunction().hashInt(i).asLong());
      }

      // Generate offset ranges proportional to the relative sizes.
      double s = sum(relativeSizes);
      long startOffset = getStartOffset();
      double sizeSoFar = 0;
      for (int i = 0; i < relativeSizes.length; ++i) {
        sizeSoFar += relativeSizes[i];
        long endOffset =
            (i == relativeSizes.length - 1)
                ? getEndOffset()
                : (long) (getStartOffset() + sizeSoFar * (getEndOffset() - getStartOffset()) / s);
        if (startOffset != endOffset) {
          result.add(new OffsetRange(startOffset, endOffset));
        }
        startOffset = endOffset;
      }
      return result;
    }
  }

  /**
   * Shape of the progress reporting curve as a function of the current offset in the {@link
   * SyntheticBoundedSource}.
   */
  public enum ProgressShape {
    /** Reported progress grows linearly from 0 to 1. */
    LINEAR,
    /** Reported progress decreases linearly from 0.9 to 0.1. */
    LINEAR_REGRESSING,
  }

  /**
   * Synthetic bounded source options. These options are all JSON, see documentations of individual
   * fields for details. {@code SyntheticSourceOptions} uses jackson annotations which
   * PipelineOptionsFactory can use to parse and construct an instance.
   */
  public static class SyntheticSourceOptions extends SyntheticOptions {
    private static final long serialVersionUID = 0;

    /** Total number of generated records. */
    @JsonProperty public long numRecords;

    /**
     * Only records whose index is a multiple of this will be split points. 0 means the source is
     * not dynamically splittable (but is perfectly statically splittable). In that case it also
     * doesn't report progress at all.
     */
    @JsonProperty public long splitPointFrequencyRecords = 1;

    /**
     * Distribution for generating initial split bundles.
     *
     * <p>When splitting into "desiredBundleSizeBytes", we'll compute the desired number of bundles
     * N, then sample this many numbers from this distribution, normalize their sum to 1, and use
     * that as the boundaries of generated bundles.
     *
     * <p>The Zipf distribution is expected to be particularly useful here.
     *
     * <p>E.g., empirically, with 100 bundles, the Zipf distribution with a parameter of 3.5 will
     * generate bundles where the largest is about 3x-10x larger than the median; with a parameter
     * of 3.0 this ratio will be about 5x-50x; with 2.5, 5x-100x (i.e. 1 bundle can be as large as
     * all others combined).
     */
    @JsonDeserialize(using = SamplerDeserializer.class)
    public Sampler bundleSizeDistribution = fromRealDistribution(new ConstantRealDistribution(1));

    /**
     * If specified, this source will split into exactly this many bundles regardless of the hints
     * provided by the service.
     */
    @JsonProperty public Integer forceNumInitialBundles;

    /** See {@link ProgressShape}. */
    @JsonProperty public ProgressShape progressShape = ProgressShape.LINEAR;

    /**
     * The distribution for the delay when reading from synthetic source starts. This delay is
     * independent of the per-record delay and uses the same types of distributions as {@link
     * #delayDistribution}.
     */
    @JsonDeserialize(using = SamplerDeserializer.class)
    final Sampler initializeDelayDistribution =
        fromRealDistribution(new ConstantRealDistribution(0));

    /**
     * Generates a random delay value for the synthetic source initialization using the distribution
     * defined by {@link #initializeDelayDistribution}.
     */
    Duration nextInitializeDelay(long seed) {
      return Duration.millis((long) initializeDelayDistribution.sample(seed));
    }

    @Override
    public void validate() {
      super.validate();
      checkArgument(
          numRecords >= 0, "numRecords should be a non-negative number, but found %s.", numRecords);
      checkNotNull(bundleSizeDistribution, "bundleSizeDistribution");
      checkArgument(
          forceNumInitialBundles == null || forceNumInitialBundles > 0,
          "forceNumInitialBundles, if specified, must be positive, but found %s",
          forceNumInitialBundles);
      checkArgument(
          splitPointFrequencyRecords >= 0,
          "splitPointFrequencyRecords must be non-negative, but found %s",
          splitPointFrequencyRecords);
    }

    public Record genRecord(long position) {
      // This method is supposed to generate random records deterministically,
      // so that results can be reproduced by running the same scenario a second time.
      // We need to initiate a Random object for each position to make the record deterministic
      // because liquid sharding could split the Source at any position.
      // And we also need a seed to initiate a Random object. The mapping from the position to
      // the seed should be fixed. Using the position as seed to feed Random objects will cause the
      // generated values to not be random enough because the position values are
      // close to each other. To make seeds fed into the Random objects unrelated,
      // we use a hashing function to map the position to its corresponding hashcode,
      // and use the hashcode as a seed to feed into the Random object.
      long hashCodeOfPosition = hashFunction().hashLong(position).asLong();
      return new Record(genKvPair(hashCodeOfPosition), nextDelay(hashCodeOfPosition));
    }

    /** Record generated by {@link #genRecord}. */
    public static class Record {
      public final KV<byte[], byte[]> kv;
      public final Duration sleepMsec;

      Record(KV<byte[], byte[]> kv, long sleepMsec) {
        this.kv = kv;
        this.sleepMsec = new Duration(sleepMsec);
      }
    }
  }

  /**
   * A reader over the {@link PCollection} of {@code KV<byte[], byte[]>} from the synthetic source.
   *
   * <p>The random but deterministic record at position "i" in the range [A, B) is generated by
   * using {@link SyntheticSourceOptions#genRecord}. Reading each record sleeps according to the
   * sleep time distribution in {@code SyntheticOptions}.
   */
  private static class SyntheticSourceReader
      extends OffsetBasedSource.OffsetBasedReader<KV<byte[], byte[]>> {
    private final long splitPointFrequencyRecords;

    private KV<byte[], byte[]> currentKvPair;
    private long currentOffset;
    private boolean isAtSplitPoint;

    SyntheticSourceReader(SyntheticBoundedSource source) {
      super(source);
      this.currentKvPair = null;
      this.splitPointFrequencyRecords = source.sourceOptions.splitPointFrequencyRecords;
    }

    @Override
    public synchronized SyntheticBoundedSource getCurrentSource() {
      return (SyntheticBoundedSource) super.getCurrentSource();
    }

    @Override
    protected long getCurrentOffset() throws IllegalStateException {
      return currentOffset;
    }

    @Override
    public KV<byte[], byte[]> getCurrent() throws NoSuchElementException {
      if (currentKvPair == null) {
        throw new NoSuchElementException(
            "The current element is unavailable because either the reader is "
                + "at the beginning of the input and start() or advance() wasn't called, "
                + "or the last start() or advance() returned false.");
      }
      return currentKvPair;
    }

    @Override
    public boolean allowsDynamicSplitting() {
      return splitPointFrequencyRecords > 0;
    }

    @Override
    protected final boolean startImpl() throws IOException {
      this.currentOffset = getCurrentSource().getStartOffset();
      if (splitPointFrequencyRecords > 0) {
        while (currentOffset % splitPointFrequencyRecords != 0) {
          ++currentOffset;
        }
      }

      SyntheticSourceOptions options = getCurrentSource().sourceOptions;
      SyntheticUtils.delay(
          options.nextInitializeDelay(this.currentOffset),
          options.cpuUtilizationInMixedDelay,
          options.delayType,
          new Random(this.currentOffset));

      isAtSplitPoint = true;
      --currentOffset;
      return advanceImpl();
    }

    @Override
    protected boolean advanceImpl() {
      currentOffset++;
      isAtSplitPoint =
          (splitPointFrequencyRecords == 0) || (currentOffset % splitPointFrequencyRecords == 0);

      SyntheticSourceOptions options = getCurrentSource().sourceOptions;
      SyntheticSourceOptions.Record record = options.genRecord(currentOffset);
      currentKvPair = record.kv;
      // TODO: add a separate distribution for the sleep time of reading the first record
      // (e.g.,"open" the files).
      long hashCodeOfVal = options.hashFunction().hashBytes(currentKvPair.getValue()).asLong();
      Random random = new Random(hashCodeOfVal);
      SyntheticUtils.delay(
          record.sleepMsec, options.cpuUtilizationInMixedDelay, options.delayType, random);

      return true;
    }

    @Override
    public void close() {
      // Nothing
    }

    @Override
    public Double getFractionConsumed() {
      double realFractionConsumed = super.getFractionConsumed();
      ProgressShape shape = getCurrentSource().sourceOptions.progressShape;
      switch (shape) {
        case LINEAR:
          return realFractionConsumed;
        case LINEAR_REGRESSING:
          return 0.9 - 0.8 * realFractionConsumed;
        default:
          throw new AssertionError("Unexpected progress shape: " + shape);
      }
    }

    @Override
    protected boolean isAtSplitPoint() throws NoSuchElementException {
      return isAtSplitPoint;
    }
  }
}
