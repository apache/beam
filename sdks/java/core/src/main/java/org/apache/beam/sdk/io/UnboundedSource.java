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

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link Source} that reads an unbounded amount of input and, because of that, supports some
 * additional operations such as checkpointing, watermarks, and record ids.
 *
 * <ul>
 *   <li>Checkpointing allows sources to not re-read the same data again in the case of failures.
 *   <li>Watermarks allow for downstream parts of the pipeline to know up to what point in time the
 *       data is complete.
 *   <li>Record ids allow for efficient deduplication of input records; many streaming sources do
 *       not guarantee that a given record will only be read a single time.
 * </ul>
 *
 * <p>See {@link org.apache.beam.sdk.transforms.windowing.Window} and {@link
 * org.apache.beam.sdk.transforms.windowing.Trigger} for more information on timestamps and
 * watermarks.
 *
 * @param <OutputT> Type of records output by this source.
 * @param <CheckpointMarkT> Type of checkpoint marks used by the readers of this source.
 */
public abstract class UnboundedSource<
        OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends Source<OutputT> {
  /**
   * Returns a list of {@code UnboundedSource} objects representing the instances of this source
   * that should be used when executing the workflow. Each split should return a separate partition
   * of the input data.
   *
   * <p>For example, for a source reading from a growing directory of files, each split could
   * correspond to a prefix of file names.
   *
   * <p>Some sources are not splittable, such as reading from a single TCP stream. In that case,
   * only a single split should be returned.
   *
   * <p>Some data sources automatically partition their data among readers. For these types of
   * inputs, {@code n} identical replicas of the top-level source can be returned.
   *
   * <p>The size of the returned list should be as close to {@code desiredNumSplits} as possible,
   * but does not have to match exactly. A low number of splits will limit the amount of parallelism
   * in the source.
   */
  public abstract List<? extends UnboundedSource<OutputT, CheckpointMarkT>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception;

  /**
   * Create a new {@link UnboundedReader} to read from this source, resuming from the given
   * checkpoint if present.
   */
  public abstract UnboundedReader<OutputT> createReader(
      PipelineOptions options, @Nullable CheckpointMarkT checkpointMark) throws IOException;

  /** Returns a {@link Coder} for encoding and decoding the checkpoints for this source. */
  public abstract Coder<CheckpointMarkT> getCheckpointMarkCoder();

  /**
   * Returns whether this source requires explicit deduping.
   *
   * <p>This is needed if the underlying data source can return the same record multiple times, such
   * a queuing system with a pull-ack model. Sources where the records read are uniquely identified
   * by the persisted state in the CheckpointMark do not need this.
   *
   * <p>Generally, if {@link CheckpointMark#finalizeCheckpoint()} is overridden, this method should
   * return true. Checkpoint finalization is best-effort, and readers can be resumed from a
   * checkpoint that has not been finalized.
   */
  public boolean requiresDeduping() {
    return false;
  }

  /**
   * A marker representing the progress and state of an {@link
   * org.apache.beam.sdk.io.UnboundedSource.UnboundedReader}.
   *
   * <p>For example, this could be offsets in a set of files being read.
   */
  public interface CheckpointMark {
    /**
     * Called by the system to signal that this checkpoint mark has been committed along with all
     * the records which have been read from the {@link UnboundedReader} since the previous
     * checkpoint was taken.
     *
     * <p>For example, this method could send acknowledgements to an external data source such as
     * Pubsub.
     *
     * <p>Note that:
     *
     * <ul>
     *   <li>This finalize method may be called from any thread, concurrently with calls to the
     *       {@link UnboundedReader} it was created from.
     *   <li>Checkpoints will not necessarily be finalized as soon as they are created. A checkpoint
     *       may be taken while a previous checkpoint from the same {@link UnboundedReader} has not
     *       yet be finalized.
     *   <li>In the absence of failures, all checkpoints will be finalized and they will be
     *       finalized in the same order they were taken from the {@link UnboundedReader}.
     *   <li>It is possible for a checkpoint to be taken but this method never called. This method
     *       will never be called if the checkpoint could not be committed, and other failures may
     *       cause this method to never be called.
     *   <li>It is not safe to assume the {@link UnboundedReader} from which this checkpoint was
     *       created still exists at the time this method is called.
     * </ul>
     */
    void finalizeCheckpoint() throws IOException;

    NoopCheckpointMark NOOP_CHECKPOINT_MARK = new NoopCheckpointMark();

    /** A checkpoint mark that does nothing when finalized. */
    final class NoopCheckpointMark implements UnboundedSource.CheckpointMark {
      @Override
      public void finalizeCheckpoint() throws IOException {
        // nothing to do
      }
    }
  }

  /**
   * A {@code Reader} that reads an unbounded amount of input.
   *
   * <p>A given {@code UnboundedReader} object will only be accessed by a single thread at once.
   */
  @Experimental(Kind.SOURCE_SINK)
  public abstract static class UnboundedReader<OutputT> extends Source.Reader<OutputT> {
    private static final byte[] EMPTY = new byte[0];

    /**
     * Initializes the reader and advances the reader to the first record. If the reader has been
     * restored from a checkpoint then it should advance to the next unread record at the point the
     * checkpoint was taken.
     *
     * <p>This method will be called exactly once. The invocation will occur prior to calling {@link
     * #advance} or {@link #getCurrent}. This method may perform expensive operations that are
     * needed to initialize the reader.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * currently available. Future calls to {@link #advance} may return {@code true} once more data
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
     * Returns a unique identifier for the current record. This should be the same for each instance
     * of the same logical record read from the underlying data source.
     *
     * <p>It is only necessary to override this if {@link #requiresDeduping} has been overridden to
     * return true.
     *
     * <p>For example, this could be a hash of the record contents, or a logical ID present in the
     * record. If this is generated as a hash of the record contents, it should be at least 16 bytes
     * (128 bits) to avoid collisions.
     *
     * <p>This method has the same restrictions on when it can be called as {@link #getCurrent} and
     * {@link #getCurrentTimestamp}.
     *
     * @throws NoSuchElementException if the reader is at the beginning of the input and {@link
     *     #start} or {@link #advance} wasn't called, or if the last {@link #start} or {@link
     *     #advance} returned {@code false}.
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
     * <p>This can be approximate. If records are read that violate this guarantee, they will be
     * considered late, which will affect how they will be processed. See {@link
     * org.apache.beam.sdk.transforms.windowing.Window} for more information on late data and how to
     * handle it.
     *
     * <p>However, this value should be as late as possible. Downstream windows may not be able to
     * close until this watermark passes their end.
     *
     * <p>For example, a source may know that the records it reads will be in timestamp order. In
     * this case, the watermark can be the timestamp of the last record read. For a source that does
     * not have natural timestamps, timestamps can be set to the time of reading, in which case the
     * watermark is the current clock time.
     *
     * <p>See {@link org.apache.beam.sdk.transforms.windowing.Window} and {@link
     * org.apache.beam.sdk.transforms.windowing.Trigger} for more information on timestamps and
     * watermarks.
     *
     * <p>May be called after {@link #advance} or {@link #start} has returned false, but not before
     * {@link #start} has been called.
     */
    public abstract Instant getWatermark();

    /**
     * Returns a {@link CheckpointMark} representing the progress of this {@code UnboundedReader}.
     *
     * <p>If this {@code UnboundedReader} does not support checkpoints, it may return a
     * CheckpointMark which does nothing, like:
     *
     * <pre>{@code
     * public UnboundedSource.CheckpointMark getCheckpointMark() {
     *   return new UnboundedSource.CheckpointMark() {
     *     public void finalizeCheckpoint() throws IOException {
     *       // nothing to do
     *     }
     *   };
     * }
     * }</pre>
     *
     * <p>All elements read between the last time this method was called (or since this reader was
     * created, if this method has not been called on this reader) until this method is called will
     * be processed together as a bundle. (An element is considered 'read' if it could be returned
     * by a call to {@link #getCurrent}.)
     *
     * <p>Once the result of processing those elements and the returned checkpoint have been durably
     * committed, {@link CheckpointMark#finalizeCheckpoint} will be called at most once at some
     * later point on the returned {@link CheckpointMark} object. Checkpoint finalization is
     * best-effort, and checkpoints may not be finalized. If duplicate elements may be produced if
     * checkpoints are not finalized in a timely manner, {@link UnboundedSource#requiresDeduping()}
     * should be overridden to return true, and {@link UnboundedReader#getCurrentRecordId()} should
     * be overridden to return unique record IDs.
     *
     * <p>A checkpoint will be committed to durable storage only if all all previous checkpoints
     * produced by the same reader have also been committed.
     *
     * <p>The returned object should not be modified.
     *
     * <p>May not be called before {@link #start} has been called.
     */
    public abstract CheckpointMark getCheckpointMark();

    /** Constant representing an unknown amount of backlog. */
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
     * Returns the {@link UnboundedSource} that created this reader. This will not change over the
     * life of the reader.
     */
    @Override
    public abstract UnboundedSource<OutputT, ?> getCurrentSource();
  }
}
