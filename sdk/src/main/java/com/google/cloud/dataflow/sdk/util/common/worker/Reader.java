/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Observable;

/**
 * Abstract base class for readers.
 *
 * <p> A Source is read from by getting an Iterator-like value and
 * iterating through it.
 *
 * @param <T> the type of the elements read from the source
 */
public abstract class Reader<T> extends Observable {
  /**
   * Returns a ReaderIterator that allows reading from this source.
   */
  public abstract ReaderIterator<T> iterator() throws IOException;

  /**
   * A stateful iterator over the data in a Reader.
   */
  public interface ReaderIterator<T> extends AutoCloseable {
    /**
     * Returns whether the source has any more elements. Some sources,
     * such as GroupingShuffleReader, invalidate the return value of
     * the previous next() call during the call to hasNext().
     */
    public boolean hasNext() throws IOException;

    /**
     * Returns the next element.
     *
     * @throws NoSuchElementException if there are no more elements
     */
    public T next() throws IOException;

    /**
     * Copies the current ReaderIterator.
     *
     * @throws UnsupportedOperationException if the particular implementation
     * does not support copy
     * @throws IOException if copying the iterator involves IO that fails
     */
    public ReaderIterator<T> copy() throws IOException;

    @Override
    public void close() throws IOException;

    /**
     * Returns a representation of how far this iterator is through the source.
     *
     * <p> This method is not required to be thread-safe, and it will not be
     * called concurrently to any other methods.
     *
     * @return the progress, or {@code null} if no progress measure
     * can be provided (implementors are discouraged from throwing
     * {@code UnsupportedOperationException} in this case).
     */
    public Progress getProgress();

    /**
     * Attempts to split the input in two parts: the "primary" part and the "residual" part.
     * The current {@link ReaderIterator} keeps processing the primary part, while the residual part
     * will be processed elsewhere (e.g. perhaps on a different worker).
     * <p>
     * The primary and residual parts, if concatenated, must represent the same input as the
     * current input of this {@link ReaderIterator} before this call.
     * <p>
     * The boundary between the primary part and the residual part is specified in
     * a framework-specific way using {@link ForkRequest}: e.g., if the framework supports the
     * notion of positions, it might be a position at which the input is asked to split itself
     * (which is not necessarily the same position at which it <i>will</i> split itself); it might
     * be an approximate fraction of input, or something else.
     * <p>
     * {@link ForkResult} encodes, in a framework-specific way, the information sufficient to
     * construct a description of the resulting primary and residual inputs. For example, it might,
     * again, be a position demarcating these parts, or it might be a pair of fully-specified input
     * descriptions, or something else.
     * <p>
     * After a successful call to {@link #requestFork}, subsequent calls should be interpreted
     * relative to the new primary.
     * <p>
     * This method is not required to be thread-safe, and it will not be
     * called concurrently to any other methods.
     * <p>
     * This call should not affect the range of input represented by the {@link Reader} which
     * produced this {@link ReaderIterator}.
     *
     * @return {@code null} if the {@link ForkRequest} cannot be honored (in that case the input
     *   represented by this {@link ReaderIterator} stays the same), or a {@link ForkResult}
     *   describing how the input was split into a primary and residual part.
     */
    public ForkResult requestFork(ForkRequest request);
  }

  /** An abstract base class for ReaderIterator implementations. */
  public abstract static class AbstractReaderIterator<T> implements ReaderIterator<T> {
    @Override
    public ReaderIterator<T> copy() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      // By default, nothing is needed for close.
    }

    @Override
    public Progress getProgress() {
      return null;
    }

    @Override
    public ForkResult requestFork(ForkRequest forkRequest) {
      return null;
    }
  }

  /**
   * A representation of how far a {@code ReaderIterator} is through a
   * {@code Reader}.
   *
   * <p> The common worker framework does not interpret instances of
   * this interface.  But a tool-specific framework can make assumptions
   * about the implementation, and so the concrete Reader subclasses used
   * by a tool-specific framework should match.
   */
  public interface Progress {}

  /**
   * A representation of a position in an iteration through a
   * {@code Reader}.
   *
   * <p> See the comment on {@link Progress} for how instances of this
   * interface are used by the rest of the framework.
   */
  public interface Position {}

  /**
   * A framework-specific way to specify how {@link ReaderIterator#requestFork} should split
   * the input into a primary and residual part.
   */
  public interface ForkRequest {}

  /**
   * A framework-specific way to specify how {@link ReaderIterator#requestFork} has split
   * the input into a primary and residual part.
   */
  public interface ForkResult {}

  /**
   * A {@link ForkResult} which specifies the boundary between the primary and residual parts
   * of the input using a {@link Position}.
   */
  public static final class ForkResultWithPosition implements ForkResult {
    private final Position acceptedPosition;

    public ForkResultWithPosition(Position acceptedPosition) {
      this.acceptedPosition = acceptedPosition;
    }

    public Position getAcceptedPosition() {
      return acceptedPosition;
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
}
