/*******************************************************************************
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
 ******************************************************************************/
package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.StateKind;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Observable;

/**
 * Abstract base class for native readers in the Dataflow runner.
 *
 * <p>A {@link com.google.api.services.dataflow.model.Source} is read from by getting an
 * {@code Iterator}-like value and iterating through it.
 *
 * <p>This class is intended for formats that have built-in support on the Dataflow service.
 * <b>Do not introduce new implementations:</b> for creating new input formats, use
 * {@link com.google.cloud.dataflow.sdk.io.Source} instead.
 *
 * @param <T> the type of the elements read from the source
 */
public abstract class NativeReader<T> extends Observable {
  /**
   * StateSampler object for readers interested in further breaking
   * down of the state space at a finer granularity.
   */
  protected StateSampler stateSampler = null;

  /**
   * Name to be used as a prefix with {@code stateSampler}.
   */
  protected String stateSamplerOperationName = null;

  /**
   * Sets the state sampler and the state sampler operation name.
   *
   * @param stateSampler the {@link StateSampler} object
   * @param stateSamplerOperationName the operation name to be used by
   * the state sampler
   */
  public void setStateSamplerAndOperationName(
      StateSampler stateSampler, String stateSamplerOperationName) {
    this.stateSampler = stateSampler;
    this.stateSamplerOperationName = stateSamplerOperationName;
  }

  /**
   * Returns a ReaderIterator that allows reading from this source.
   */
  public abstract NativeReaderIterator<T> iterator() throws IOException;

  /**
   * A stateful iterator over the data in a {@link NativeReader}.
   *
   * <p>Partially thread-safe: methods {@link #start}, {@link #advance}, {@link #getCurrent},
   * {@link #close} are called serially, but {@link #requestDynamicSplit} can be called
   * asynchronously to those.
   *
   * <p>There will not be multiple concurrent calls to {@link #requestDynamicSplit}).
   * {@link #getProgress} can be called concurrently to any other call, including itself, if
   * {@link #requestDynamicSplit} is implemented.
   */
  public abstract static class NativeReaderIterator<T> implements AutoCloseable {

    /**
     * A value to return from {@link #getRemainingParallelism()} when remaining parallelism
     * can be interpolated from {@link NativeReader#getTotalParallelism} and the progress fraction.
     */
    public static final double REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION = Double.NaN;

    /**
     * Returns a representation of how far this iterator is through the source.
     * @return the progress, or {@code null} if no progress measure
     * can be provided (implementors are discouraged from throwing
     * {@code UnsupportedOperationException} in this case).
     */
    public abstract Progress getProgress();

    /**
     * Attempts to split the input in two parts: the "primary" part and the "residual" part.
     * The current {@link NativeReaderIterator} keeps processing the primary part, while the
     * residual part will be processed elsewhere (e.g. perhaps on a different worker).
     *
     * <p>The primary and residual parts, if concatenated, must represent the same input as the
     * current input of this {@link NativeReaderIterator} before this call.
     *
     * <p>The boundary between the primary part and the residual part is specified in
     * a framework-specific way using {@link NativeReader.DynamicSplitRequest}: e.g., if the
     * framework supports the notion of positions, it might be a position at which the input is
     * asked to split itself (which is not necessarily the same position at which it <i>will</i>
     * split itself); it might be an approximate fraction of input, or something else.
     *
     * <p>{@link NativeReader.DynamicSplitResult} encodes, in a framework-specific way, the
     * information sufficient to construct a description of the resulting primary and
     * residual inputs.
     * For example, it might, again, be a position demarcating these parts, or it might be a pair of
     * fully-specified input descriptions, or something else.
     *
     * <p>After a successful call to {@link #requestDynamicSplit}, subsequent calls should be
     * interpreted relative to the new primary.
     *
     * <p>This call should not affect the range of input represented by the {@link NativeReader}
     * that produced this {@link NativeReaderIterator}.
     *
     * @return {@code null} if the {@link NativeReader.DynamicSplitRequest} cannot be honored
     *   (in that case the input represented by this {@link NativeReaderIterator} stays the same),
     *   or a {@link NativeReader.DynamicSplitResult} describing how the input was split into
     *   a primary and residual part.
     */
    public abstract DynamicSplitResult requestDynamicSplit(DynamicSplitRequest request);

    /**
     * Returns an estimate of the degree of parallelism that could be achieved by
     * {@link #requestDynamicSplit} taking into account what has already been consumed.
     * E.g., if the reader has just returned the last record in the source, the remaining
     * parallelism is 1 because it can't be split up any further. If the reader just
     * returned the 3rd record in a perfectly parallelizable source with 5 records,
     * the remaining parallelism is 3 because it could be processed in parallel by this
     * worker and two others.  If the reader does not support dynamic splitting,
     * the remaining parallelism is always 1.
     *
     * <p>An exact number isn't required, mostly we want to be able to distinguish
     * between many, few, or one. Should not block.
     *
     * <p>An implementor may return {@link #REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION},
     * in which case the remaining parallelism will be interpolated from
     * {@link NativeReader#getTotalParallelism} using the current progress fraction.
     * Infinity may also be returned (indicating no known bound on parallelism),
     * as may fractional estimates (in which case the sum over all shards is taken).
     */
    public abstract double getRemainingParallelism();

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
     * Returns the value of the data item that was read by the last {@link #start} or
     * {@link #advance} call. The returned value must be effectively immutable and remain valid
     * indefinitely.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws NoSuchElementException if {@link #start} was never called, or if
     *         the last {@link #start} or {@link #advance} returned {@code false}
     */
    public abstract T getCurrent() throws NoSuchElementException;

    /**
     * @inheritDoc
     */
    @Override
    public abstract void close() throws IOException;
  }

  /** An abstract base class for ReaderIterator implementations. */
  public abstract static class AbstractReaderIterator<T> extends NativeReaderIterator<T> {

    @Override
    public void close() throws IOException {
      // By default, nothing is needed for close.
    }

    @Override
    public Progress getProgress() {
      return null;
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      return null;
    }

    @Override
    public double getRemainingParallelism() {
      return REMAINING_PARALLELISM_FROM_PROGRESS_FRACTION;
    }
  }

  /**
   * Adapter from old-style reader interface ({@link #hasNext}, {@link #next}) to new-style
   * iteration interface ({@link NativeReaderIterator#start}, {@link NativeReaderIterator#advance},
   * {@link NativeReaderIterator#getCurrent}).
   *
   * This class is temporary and the intention is to get rid of its subclasses one by one,
   * converting them to use the new-style interface directly, and then remove this class.
   */
  public abstract static class LegacyReaderIterator<T> extends AbstractReaderIterator<T> {
    private T current;
    private boolean hasCurrent;

    /**
     * Returns whether the source has any more elements. Some sources,
     * such as GroupingShuffleReader, invalidate the return value of
     * the previous next() call during the call to hasNext().
     */
    public abstract boolean hasNext() throws IOException;

    /**
     * Returns the next element.
     *
     * @throws IOException if attempting to access an element involves IO that fails
     * @throws NoSuchElementException if there are no more elements
     */
    public abstract T next() throws IOException, NoSuchElementException;

    @Override
    public boolean start() throws IOException {
      hasCurrent = advance();
      return hasCurrent;
    }

    @Override
    public boolean advance() throws IOException {
      if (!hasNext()) {
        hasCurrent = false;
        return false;
      }
      current = next();
      hasCurrent = true;
      return true;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (!hasCurrent) {
        throw new NoSuchElementException();
      }
      return current;
    }
  }

  /**
   * A representation of how far a {@code ReaderIterator} is through a
   * {@code Reader}.
   *
   * <p>The common worker framework does not interpret instances of
   * this interface.  But a tool-specific framework can make assumptions
   * about the implementation, and so the concrete Reader subclasses used
   * by a tool-specific framework should match.
   */
  public interface Progress {}

  /**
   * A representation of a position in an iteration through a
   * {@code Reader}.
   *
   * <p>See the comment on {@link Progress} for how instances of this
   * interface are used by the rest of the framework.
   */
  public interface Position {}

  /**
   * A framework-specific way to specify how {@link NativeReaderIterator#requestDynamicSplit}
   * should split the input into a primary and residual part.
   */
  public interface DynamicSplitRequest {}

  /**
   * A framework-specific way to specify how {@link NativeReaderIterator#requestDynamicSplit}
   * has split the input into a primary and residual part.
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
  }

  /**
   * Utility method to notify observers about a new element, which has
   * been read by this Reader, and its size in bytes. Normally, there
   * is only one observer, which is a ReadOperation that encapsules
   * this Reader. Derived classes must call this method whenever they
   * read additional data, even if that element may never be returned
   * from the corresponding source iterator.
   */
  protected void notifyElementRead(long byteSize) {
    setChanged();
    notifyObservers(byteSize);
  }

  /**
   * Returns whether this Reader can be restarted.
   */
  public boolean supportsRestart() {
    return false;
  }

  /**
   * Returns an estimate of the parallelism of the source being read by this reader, i.e.
   * the number of bundles it could be split into.  An exact number isn't required, mostly
   * we want to be able to distinguish between many, few, or one.  Used to cap the parallelism
   * Dataflow will allocate for this part of the pipeline.  Should not block.
   *
   * <p>Defaults to positive infinity, indicating unbounded parallelism.  An unsplittable source
   * would have parallelism exactly 1.
   *
   * <p>See also {@link NativeReaderIterator#getRemainingParallelism} which may be implemented to
   * complement this method if a better-than-linear estimate of remaining parallelism can be
   * obtained (e.g. it is easy to detect when one is at the last record).
   */
  public double getTotalParallelism() {
    // By default, don't assume any limitations.
    return Double.POSITIVE_INFINITY;
  }

  /**
   * The default state kind of all the states reported in this reader.
   * Defaults to {@link StateKind#USER}.
   */
  protected StateKind getStateSamplerStateKind() {
    return StateKind.USER;
  }
}
