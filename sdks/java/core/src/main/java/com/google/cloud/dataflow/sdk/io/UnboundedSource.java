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

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A {@link Source} that reads an unbounded amount of input and, because of that, supports
 * some additional operations such as checkpointing, watermarks, and record ids.
 *
 * <ul>
 * <li> Checkpointing allows sources to not re-read the same data again in the case of failures.
 * <li> Watermarks allow for downstream parts of the pipeline to know up to what point
 *   in time the data is complete.
 * <li> Record ids allow for efficient deduplication of input records; many streaming sources
 *   do not guarantee that a given record will only be read a single time.
 * </ul>
 *
 * <p>See {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window} and
 * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Trigger} for more information on
 * timestamps and watermarks.
 *
 * @param <OutputT> Type of records output by this source.
 * @param <CheckpointMarkT> Type of checkpoint marks used by the readers of this source.
 */
public abstract class UnboundedSource<
        OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark> extends Source<OutputT> {
  /**
   * Returns a list of {@code UnboundedSource} objects representing the instances of this source
   * that should be used when executing the workflow.  Each split should return a separate partition
   * of the input data.
   *
   * <p>For example, for a source reading from a growing directory of files, each split
   * could correspond to a prefix of file names.
   *
   * <p>Some sources are not splittable, such as reading from a single TCP stream.  In that
   * case, only a single split should be returned.
   *
   * <p>Some data sources automatically partition their data among readers.  For these types of
   * inputs, {@code n} identical replicas of the top-level source can be returned.
   *
   * <p>The size of the returned list should be as close to {@code desiredNumSplits}
   * as possible, but does not have to match exactly.  A low number of splits
   * will limit the amount of parallelism in the source.
   */
  public abstract List<? extends UnboundedSource<OutputT, CheckpointMarkT>> generateInitialSplits(
      int desiredNumSplits, PipelineOptions options) throws Exception;

  /**
   * Create a new {@link UnboundedReader} to read from this source, resuming from the given
   * checkpoint if present.
   */
  public abstract UnboundedReader<OutputT> createReader(
      PipelineOptions options, @Nullable CheckpointMarkT checkpointMark);

  /**
   * Returns a {@link Coder} for encoding and decoding the checkpoints for this source, or
   * null if the checkpoints do not need to be durably committed.
   */
  @Nullable
  public abstract Coder<CheckpointMarkT> getCheckpointMarkCoder();

  /**
   * Returns whether this source requires explicit deduping.
   *
   * <p>This is needed if the underlying data source can return the same record multiple times,
   * such a queuing system with a pull-ack model.  Sources where the records read are uniquely
   * identified by the persisted state in the CheckpointMark do not need this.
   */
  public boolean requiresDeduping() {
    return false;
  }

  /**
   * A marker representing the progress and state of an
   * {@link com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader}.
   *
   * <p>For example, this could be offsets in a set of files being read.
   */
  public interface CheckpointMark {
    /**
     * Perform any finalization that needs to happen after a bundle of data read from
     * the source has been processed and committed.
     *
     * <p>For example, this could be sending acknowledgement requests to an external
     * data source such as Pub/Sub.
     *
     * <p>This may be called from any thread, potentially at the same time as calls to the
     * {@code UnboundedReader} that created it.
     */
    void finalizeCheckpoint() throws IOException;
  }

  /**
   * A {@code Reader} that reads an unbounded amount of input.
   *
   * <p>A given {@code UnboundedReader} object will only be accessed by a single thread at once.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public abstract static class UnboundedReader<OutputT> extends Source.Reader<OutputT> {
    private static final byte[] EMPTY = new byte[0];

    /**
     * Initializes the reader and advances the reader to the first record.
     *
     * <p>This method should be called exactly once. The invocation should occur prior to calling
     * {@link #advance} or {@link #getCurrent}. This method may perform expensive operations that
     * are needed to initialize the reader.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * currently available.  Future calls to {@link #advance} may return {@code true} once more data
     * is available. Regardless of the return value of {@code start}, {@code start} will not be
     * called again on the same {@code UnboundedReader} object; it will only be called again when a
     * new reader object is constructed for the same source, e.g. on recovery.
     */
    @Override
    public abstract boolean start() throws IOException;

    /**
     * Advances the reader to the next valid record.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * available. Future calls to {@link #advance} may return {@code true} once more data is
     * available.
     */
    @Override
    public abstract boolean advance() throws IOException;

    /**
     * Returns a unique identifier for the current record.  This should be the same for each
     * instance of the same logical record read from the underlying data source.
     *
     * <p>It is only necessary to override this if {@link #requiresDeduping} has been overridden to
     * return true.
     *
     * <p>For example, this could be a hash of the record contents, or a logical ID present in
     * the record.  If this is generated as a hash of the record contents, it should be at least 16
     * bytes (128 bits) to avoid collisions.
     *
     * <p>This method has the same restrictions on when it can be called as {@link #getCurrent} and
     * {@link #getCurrentTimestamp}.
     *
     * @throws NoSuchElementException if the reader is at the beginning of the input and
     *         {@link #start} or {@link #advance} wasn't called, or if the last {@link #start} or
     *         {@link #advance} returned {@code false}.
     */
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      if (getCurrentSource().requiresDeduping()) {
        throw new IllegalStateException(
            "getCurrentRecordId() must be overridden if requiresDeduping returns true()");
      }
      return EMPTY;
    }

    /**
     * Returns a timestamp before or at the timestamps of all future elements read by this reader.
     *
     * <p>This can be approximate.  If records are read that violate this guarantee, they will be
     * considered late, which will affect how they will be processed.  See
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window} for more information on
     * late data and how to handle it.
     *
     * <p>However, this value should be as late as possible. Downstream windows may not be able
     * to close until this watermark passes their end.
     *
     * <p>For example, a source may know that the records it reads will be in timestamp order.  In
     * this case, the watermark can be the timestamp of the last record read.  For a
     * source that does not have natural timestamps, timestamps can be set to the time of
     * reading, in which case the watermark is the current clock time.
     *
     * <p>See {@link com.google.cloud.dataflow.sdk.transforms.windowing.Window} and
     * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Trigger} for more
     * information on timestamps and watermarks.
     *
     * <p>May be called after {@link #advance} or {@link #start} has returned false, but not before
     * {@link #start} has been called.
     */
    public abstract Instant getWatermark();

    /**
     * Returns a {@link CheckpointMark} representing the progress of this {@code UnboundedReader}.
     *
     * <p>The elements read up until this is called will be processed together as a bundle. Once
     * the result of this processing has been durably committed,
     * {@link CheckpointMark#finalizeCheckpoint} will be called on the {@link CheckpointMark}
     * object.
     *
     * <p>The returned object should not be modified.
     *
     * <p>May be called after {@link #advance} or {@link #start} has returned false, but not before
     * {@link #start} has been called.
     */
    public abstract CheckpointMark getCheckpointMark();

    /**
     * Constant representing an unknown amount of backlog.
     */
    public static final long BACKLOG_UNKNOWN = -1L;

    /**
     * Returns the size of the backlog of unread data in the underlying data source represented by
     * this split of this source.
     *
     * <p>One of this or {@link #getTotalBacklogBytes} should be overridden in order to allow the
     * runner to scale the amount of resources allocated to the pipeline.
     */
    public long getSplitBacklogBytes() {
      return BACKLOG_UNKNOWN;
    }

    /**
     * Returns the size of the backlog of unread data in the underlying data source represented by
     * all splits of this source.
     *
     * <p>One of this or {@link #getSplitBacklogBytes} should be overridden in order to allow the
     * runner to scale the amount of resources allocated to the pipeline.
     */
    public long getTotalBacklogBytes() {
      return BACKLOG_UNKNOWN;
    }

    /**
     * Returns the {@link UnboundedSource} that created this reader.  This will not change over the
     * life of the reader.
     */
    @Override
    public abstract UnboundedSource<OutputT, ?> getCurrentSource();
  }
}
