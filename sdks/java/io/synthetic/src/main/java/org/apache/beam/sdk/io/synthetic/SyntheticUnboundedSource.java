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

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.synthetic.delay.ReaderDelay;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link SyntheticUnboundedSource} that reads {@code KV<byte[], byte[]>}. */
@Experimental(Kind.SOURCE_SINK)
public class SyntheticUnboundedSource
    extends UnboundedSource<KV<byte[], byte[]>, SyntheticRecordsCheckpoint> {

  private static final long serialVersionUID = 0;

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticUnboundedSource.class);

  private final SyntheticSourceOptions sourceOptions;

  private final BundleSplitter bundleSplitter;

  private final long startOffset;

  private final long endOffset;

  public SyntheticUnboundedSource(SyntheticSourceOptions sourceOptions) {
    this(0, sourceOptions.numRecords, sourceOptions);
  }

  private SyntheticUnboundedSource(
      long startOffset, long endOffset, SyntheticSourceOptions sourceOptions) {
    this.sourceOptions = sourceOptions;
    this.bundleSplitter = new BundleSplitter(sourceOptions);
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  @Override
  public Coder<KV<byte[], byte[]>> getOutputCoder() {
    return KvCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of());
  }

  @Override
  public void validate() {
    super.validate();
    sourceOptions.validate();
  }

  @Override
  public Coder<SyntheticRecordsCheckpoint> getCheckpointMarkCoder() {
    return SyntheticRecordsCheckpoint.CODER;
  }

  @Override
  public UnboundedReader<KV<byte[], byte[]>> createReader(
      PipelineOptions options, @Nullable SyntheticRecordsCheckpoint checkpoint) {

    if (checkpoint == null) {
      return new SyntheticUnboundedReader(this);
    } else {
      return new SyntheticUnboundedReader(
          new SyntheticUnboundedSource(
              checkpoint.getStartPosition(), checkpoint.getEndPosition(), sourceOptions));
    }
  }

  @Override
  public List<SyntheticUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    int desiredNumBundles =
        sourceOptions.forceNumInitialBundles != null
            ? sourceOptions.forceNumInitialBundles
            : desiredNumSplits;

    List<SyntheticUnboundedSource> splits =
        bundleSplitter.getBundleSizes(desiredNumBundles, startOffset, endOffset).stream()
            .map(
                offsetRange ->
                    new SyntheticUnboundedSource(
                        offsetRange.getFrom(), offsetRange.getTo(), sourceOptions))
            .collect(Collectors.toList());
    LOG.info("Split into {} bundles of sizes: {}", splits.size(), splits);

    return splits;
  }

  private class SyntheticUnboundedReader extends UnboundedReader<KV<byte[], byte[]>> {

    private final SyntheticUnboundedSource source;

    private KV<byte[], byte[]> currentKVPair;

    private long currentOffset;

    private Instant processingTime;

    private Instant eventTime;

    private SyntheticWatermark syntheticWatermark;

    private ReaderDelay delay;

    public SyntheticUnboundedReader(SyntheticUnboundedSource source) {
      this.currentKVPair = null;
      this.delay = new ReaderDelay(sourceOptions);
      this.source = source;
      this.currentOffset = 0;
      this.syntheticWatermark = new SyntheticWatermark(sourceOptions, source.endOffset);
    }

    @Override
    public SyntheticUnboundedSource getCurrentSource() {
      return source;
    }

    @Override
    public KV<byte[], byte[]> getCurrent() throws NoSuchElementException {
      if (currentKVPair == null) {
        throw new NoSuchElementException(
            "Current record is unavailable because either the reader is "
                + "at the beginning of the input and start() or advance() wasn't called, "
                + "or the last start() or advance() returned false.");
      }
      return currentKVPair;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (eventTime == null) {
        throw new NoSuchElementException(
            "Current timestamp is unavailable because either the reader is "
                + "at the beginning of the input and start() or advance() wasn't called, "
                + "or the last start() or advance() returned false.");
      }
      return eventTime;
    }

    @Override
    public boolean start() {
      currentOffset = SyntheticUnboundedSource.this.startOffset;
      delay.delayStart(currentOffset);
      return advance();
    }

    @Override
    public boolean advance() {
      currentOffset++;

      processingTime = new Instant();
      eventTime = processingTime.minus(sourceOptions.nextProcessingTimeDelay(currentOffset));

      SyntheticSourceOptions.Record record =
          getCurrentSource().sourceOptions.genRecord(currentOffset);
      currentKVPair = record.kv;

      delay.delayRecord(record);

      return currentOffset < source.endOffset;
    }

    @Override
    public Instant getWatermark() {
      return syntheticWatermark.calculateNew(currentOffset, processingTime);
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new SyntheticRecordsCheckpoint(source.startOffset, source.endOffset);
    }

    @Override
    public void close() {
      // Nothing
    }
  }
}
