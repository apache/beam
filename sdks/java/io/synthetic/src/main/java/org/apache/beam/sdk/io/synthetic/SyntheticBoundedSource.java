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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.OffsetBasedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions.Record;
import org.apache.beam.sdk.io.synthetic.delay.ReaderDelay;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SyntheticBoundedSource} that reads {@code KV<byte[], byte[]>}.
 *
 * <p>The {@link SyntheticBoundedSource} generates a {@link PCollection} of {@code KV<byte[],
 * byte[]>}. A fraction of the generated records {@code KV<byte[], byte[]>} are associated with
 * "hot" keys, which are uniformly distributed over a fixed number of hot keys. The remaining
 * generated records are associated with "random" keys. Each record will be slowed down by a certain
 * sleep time generated based on the specified sleep time distribution when the {@link
 * SyntheticBoundedSource.SyntheticSourceReader} reads each record. The record {@code KV<byte[],
 * byte[]>} is generated deterministically based on the record's position in the source, which
 * enables repeatable execution for debugging. The SyntheticBoundedInput configurable parameters are
 * defined in {@link SyntheticSourceOptions}.*
 */
@Experimental(Kind.SOURCE_SINK)
public class SyntheticBoundedSource extends OffsetBasedSource<KV<byte[], byte[]>> {

  private static final long serialVersionUID = 0;

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticBoundedSource.class);

  private final SyntheticSourceOptions sourceOptions;

  private final BundleSplitter bundleSplitter;

  public SyntheticBoundedSource(SyntheticSourceOptions sourceOptions) {
    this(0, sourceOptions.numRecords, sourceOptions);
  }

  public SyntheticBoundedSource(
      long startOffset, long endOffset, SyntheticSourceOptions sourceOptions) {
    super(startOffset, endOffset, 1);
    this.sourceOptions = sourceOptions;
    this.bundleSplitter = new BundleSplitter(this.sourceOptions);
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
        bundleSplitter.getBundleSizes(desiredNumBundles, this.getStartOffset(), this.getEndOffset())
            .stream()
            .map(offsetRange -> createSourceForSubrange(offsetRange.getFrom(), offsetRange.getTo()))
            .collect(Collectors.toList());
    LOG.info("Split into {} bundles of sizes: {}", res.size(), res);
    return res;
  }

  /**
   * A reader over the {@link PCollection} of {@code KV<byte[], byte[]>} from the synthetic source.
   *
   * <p>The random but deterministic record at position "i" in the range [A, B) is generated by
   * using {@link SyntheticSourceOptions#genRecord}. Reading each record sleeps according to the
   * sleep time distribution in {@code SyntheticOptions}.
   */
  private static class SyntheticSourceReader extends OffsetBasedReader<KV<byte[], byte[]>> {

    private final long splitPointFrequencyRecords;

    private KV<byte[], byte[]> currentKvPair;

    private long currentOffset;

    private boolean isAtSplitPoint;

    private ReaderDelay readerDelay;

    SyntheticSourceReader(SyntheticBoundedSource source) {
      super(source);
      this.readerDelay = new ReaderDelay(source.sourceOptions);
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

      readerDelay.delayStart(currentOffset);
      isAtSplitPoint = true;
      --currentOffset;
      return advanceImpl();
    }

    @Override
    protected boolean advanceImpl() {
      currentOffset++;
      isAtSplitPoint = shouldSourceSplit();
      Record record = getCurrentSource().sourceOptions.genRecord(currentOffset);
      currentKvPair = record.kv;
      readerDelay.delayRecord(record);

      return true;
    }

    private boolean shouldSourceSplit() {
      return (splitPointFrequencyRecords == 0) || (currentOffset % splitPointFrequencyRecords == 0);
    }

    @Override
    public Double getFractionConsumed() {
      double realFractionConsumed = super.getFractionConsumed();
      SyntheticSourceOptions.ProgressShape shape = getCurrentSource().sourceOptions.progressShape;
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

    @Override
    public void close() {
      // Nothing
    }
  }
}
