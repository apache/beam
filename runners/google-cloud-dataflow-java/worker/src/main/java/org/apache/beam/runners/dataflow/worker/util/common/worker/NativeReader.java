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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Observable;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Abstract base class for native readers in the Dataflow runner.
 *
 * <p>A {@link com.google.api.services.dataflow.model.Source} is read from by getting an {@code
 * Iterator}-like value and iterating through it.
 *
 * <p>This class is intended for formats that have built-in support on the Dataflow service. <b>Do
 * not introduce new implementations:</b> for creating new input formats, use {@code
 * Bounded/UnboundedSource} instead.
 *
 * @param <T> the type of the elements read from the source
 */
public abstract class NativeReader<T> extends Observable {

  /** Returns a ReaderIterator that allows reading from this source. */
  public abstract NativeReaderIterator<T> iterator() throws IOException;

  /**
   * A stateful iterator over the data in a {@link NativeReader}.
   *
   * <p>This class is only partially thread safe. Methods {@link #start}, {@link #getCurrent},
   * {@link #advance}, and {@link #close} must be called serially. Methods {@link
   * #requestCheckpoint} and {@link #requestDynamicSplit} may be called asynchronously to those
   * methods but must not be called concurrently to themselves or each other. Methods {@link
   * #getProgress} and {@link #getRemainingParallelism} may be called asynchronously to any of the
   * methods including themselves.
   */
  public abstract static class NativeReaderIterator<T> implements AutoCloseable {

    /** A default value to return from {@link #getRemainingParallelism()}. */
    public static final double REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION = Double.NaN;

    /**
     * Returns a representation of how far this iterator is through the source.
     *
     * @return the progress, or {@code null} if no progress measure can be provided (implementors
     *     are discouraged from throwing {@code UnsupportedOperationException} in this case). By
     *     default, returns {@code null}.
     */
    public @Nullable Progress getProgress() {
      return null;
    }

    /**
     * Like {@link #requestDynamicSplit}, but rather than taking the split point, makes the
     * "primary" part as small as possible, so that this iterator will produce a minimal number of
     * (ideally no) further records.
     */
    public @Nullable DynamicSplitResult requestCheckpoint() {
      return null;
    }

    /**
     * Attempts to split the input in two parts: the "primary" part and the "residual" part. The
     * current {@link NativeReaderIterator} keeps processing the primary part, while the residual
     * part will be processed elsewhere (e.g. perhaps on a different worker).
     *
     * <p>The primary and residual parts, if concatenated, must represent the same input as the
     * current input of this {@link NativeReaderIterator} before this call.
     *
     * <p>The boundary between the primary part and the residual part is specified in a
     * framework-specific way using {@link NativeReader.DynamicSplitRequest}: e.g., if the framework
     * supports the notion of positions, it might be a position at which the input is asked to split
     * itself (which is not necessarily the same position at which it <i>will</i> split itself); it
     * might be an approximate fraction of input, or something else.
     *
     * <p>{@link NativeReader.DynamicSplitResult} encodes, in a framework-specific way, the
     * information sufficient to construct a description of the resulting primary and residual
     * inputs. For example, it might, again, be a position demarcating these parts, or it might be a
     * pair of fully-specified input descriptions, or something else.
     *
     * <p>After a successful call to {@link #requestDynamicSplit}, subsequent calls should be
     * interpreted relative to the new primary.
     *
     * <p>This call should not affect the range of input represented by the {@link NativeReader}
     * that produced this {@link NativeReaderIterator}.
     *
     * @return {@code null} if the {@link NativeReader.DynamicSplitRequest} cannot be honored (in
     *     that case the input represented by this {@link NativeReaderIterator} stays the same), or
     *     a {@link NativeReader.DynamicSplitResult} describing how the input was split into a
     *     primary and residual part. By default, returns {@code null}.
     */
    public @Nullable DynamicSplitResult requestDynamicSplit(DynamicSplitRequest request) {
      return null;
    }

    /**
     * Returns an estimate of the degree of parallelism that could be achieved by {@link
     * #requestDynamicSplit} taking into account what has already been consumed. E.g., if the reader
     * has just returned the last record in the source, the remaining parallelism is 1 because it
     * can't be split up any further. If the reader just returned the 3rd record in a perfectly
     * parallelizable source with 5 records, the remaining parallelism is 3 because it could be
     * processed in parallel by this worker and two others. If the reader does not support dynamic
     * splitting, the remaining parallelism is always 1.
     *
     * <p>An exact number isn't required, mostly we want to be able to distinguish between many,
     * few, or one. Should not block.
     *
     * <p>An implementor may return {@link #REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION}, in which
     * case the remaining parallelism will be interpolated from consumed parallelism, if available,
     * and progress fraction. {@link Double#POSITIVE_INFINITY} may also be returned (indicating no
     * known bound on parallelism), as may fractional estimates (in which case the sum over all
     * shards is taken).
     *
     * <p>By default, returns {@link #REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION}.
     */
    public double getRemainingParallelism() {
      return REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION;
    }

    /**
     * Initializes the reader and advances the reader to the first record.
     *
     * <p>This method should be called exactly once. The invocation should occur prior to calling
     * {@link #advance} or {@link #getCurrent}. This method may perform expensive operations that
     * are needed to initialize the reader.
     *
     * @return {@code true} if a record was read, {@code false} if there is no more input available.
     */
    public abstract boolean start() throws IOException;

    /**
     * Advances the reader to the next valid record.
     *
     * <p>It is an error to call this without having called {@link #start} first.
     *
     * @return {@code true} if a record was read, {@code false} if there is no more input available.
     */
    public abstract boolean advance() throws IOException;

    /**
     * Returns the value of the data item that was read by the last {@link #start} or {@link
     * #advance} call. The returned value must be effectively immutable and remain valid
     * indefinitely.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws NoSuchElementException if {@link #start} was never called, or if the last {@link
     *     #start} or {@link #advance} returned {@code false}
     */
    public abstract T getCurrent() throws NoSuchElementException;

    @Override
    public void close() throws IOException {
      // By default, do nothing.
    }

    public void abort() throws IOException {
      // By default, just close
      close();
    }
  }

  /**
   * A representation of how far a {@code ReaderIterator} is through a {@code Reader}.
   *
   * <p>The common worker framework does not interpret instances of this interface. But a
   * tool-specific framework can make assumptions about the implementation, and so the concrete
   * Reader subclasses used by a tool-specific framework should match.
   */
  public interface Progress {}

  /**
   * A representation of a position in an iteration through a {@code Reader}.
   *
   * <p>See the comment on {@link Progress} for how instances of this interface are used by the rest
   * of the framework.
   */
  public interface Position {}

  /**
   * A framework-specific way to specify how {@link NativeReaderIterator#requestDynamicSplit} should
   * split the input into a primary and residual part.
   */
  public interface DynamicSplitRequest {}

  /**
   * A framework-specific way to specify how {@link NativeReaderIterator#requestDynamicSplit} has
   * split the input into a primary and residual part.
   */
  public interface DynamicSplitResult {}

  /**
   * A {@link NativeReader.DynamicSplitResult} that specifies the boundary between the primary and
   * residual parts of the input using a {@link Position}.
   */
  public static final class DynamicSplitResultWithPosition implements DynamicSplitResult {
    private final Position acceptedPosition;

    public DynamicSplitResultWithPosition(Position acceptedPosition) {
      this.acceptedPosition = acceptedPosition;
    }

    public Position getAcceptedPosition() {
      return acceptedPosition;
    }

    @Override
    public String toString() {
      return String.valueOf(acceptedPosition);
    }

    @Override
    public int hashCode() {
      return acceptedPosition.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof DynamicSplitResultWithPosition)) {
        return false;
      } else {
        return Objects.equals(
            acceptedPosition, ((DynamicSplitResultWithPosition) obj).acceptedPosition);
      }
    }
  }

  /**
   * Utility method to notify observers about a new element, which has been read by this Reader, and
   * its size in bytes. Normally, there is only one observer, which is a ReadOperation that
   * encapsules this Reader. Derived classes must call this method whenever they read additional
   * data, even if that element may never be returned from the corresponding source iterator.
   */
  protected void notifyElementRead(long byteSize) {
    setChanged();
    notifyObservers(byteSize);
  }

  /** Returns whether this Reader can be restarted. */
  public boolean supportsRestart() {
    return false;
  }
}
