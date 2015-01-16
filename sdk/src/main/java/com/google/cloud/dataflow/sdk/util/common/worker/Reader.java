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
     * Attempts to update the stop position of the task with the proposed stop
     * position and returns the actual new stop position.
     *
     * <p> If the source finds the proposed one is not a convenient position to
     * stop, it can pick a different stop position. The {@code ReaderIterator}
     * should start returning {@code false} from {@code hasNext()} once it has
     * passed its stop position. Subsequent stop position updates must be in
     * non-increasing order within a task.
     *
     * <p> This method is not required to be thread-safe, and it will not be
     * called concurrently to any other methods.
     *
     * @param proposedStopPosition a proposed position to stop
     * iterating through the source
     * @return the new stop position, or {@code null} on failure if the
     * implementation does not support position updates(implementors are discouraged
     * from throwing {@code UnsupportedOperationException} in this case).
     */
    public Position updateStopPosition(Progress proposedStopPosition);
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
    public Position updateStopPosition(Progress proposedStopPosition) {
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
