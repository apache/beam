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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Base class for defining input formats, with custom logic for splitting the input
 * into bundles (parts of the input, each of which may be processed on a different worker)
 * and creating a {@code Source} for reading the input.
 *
 * <p> This class is not intended to be subclassed directly. Instead, to define
 * a bounded source (a source which produces a finite amount of input), subclass
 * {@link BoundedSource}; user-defined unbounded sources are currently not supported.
 *
 * <p> A {@code Source} passed to a {@code Read} transform must be
 * {@code Serializable}.  This allows the {@code Source} instance
 * created in this "main program" to be sent (in serialized form) to
 * remote worker machines and reconstituted for each batch of elements
 * of the input {@code PCollection} being processed or for each source splitting
 * operation. A {@code Source} can have instance variable state, and
 * non-transient instance variable state will be serialized in the main program
 * and then deserialized on remote worker machines.
 *
 * <p> {@code Source} classes MUST be effectively immutable. The only acceptable use of
 * mutable fields is to cache the results of expensive operations, and such fields MUST be
 * marked {@code transient}.
 *
 * <p> {@code Source} objects should override {@link Object#toString}, as it will be
 * used in important error and debugging messages.
 *
 * @param <T> Type of elements read by the source.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public abstract class Source<T> implements Serializable {
  private static final long serialVersionUID = 0;

  /**
   * Creates a reader for this source.
   */
  public abstract Reader<T> createReader(
      PipelineOptions options, @Nullable ExecutionContext executionContext) throws IOException;

  /**
   * Checks that this source is valid, before it can be used in a pipeline.
   *
   * <p>It is recommended to use {@link com.google.common.base.Preconditions} for implementing
   * this method.
   */
  public abstract void validate();

  /**
   * Returns the default {@code Coder} to use for the data read from this source.
   */
  public abstract Coder<T> getDefaultOutputCoder();

  /**
   * The interface that readers of custom input sources must implement.
   * <p>
   * This interface is deliberately distinct from {@link java.util.Iterator} because
   * the current model tends to be easier to program and more efficient in practice
   * for iterating over sources such as files, databases etc. (rather than pure collections).
   * <p>
   * To read a {@code Reader}:
   * <pre>
   * for (boolean available = reader.start(); available; available = reader.advance()) {
   *   T item = reader.getCurrent();
   *   ...
   * }
   * </pre>
   * <p>
   * Note: this interface is a work-in-progress and may change.
   */
  public interface Reader<T> extends AutoCloseable {
    /**
     * Initializes the reader and advances the reader to the first record.
     *
     * <p> This method should be called exactly once. The invocation should occur prior to calling
     * {@link #advance} or {@link #getCurrent}. This method may perform expensive operations that
     * are needed to initialize the reader.
     *
     * @return {@code true} if a record was read, {@code false} if we're at the end of input.
     */
    public boolean start() throws IOException;

    /**
     * Advances the reader to the next valid record.
     *
     * @return {@code true} if a record was read, {@code false} if we're at the end of input.
     */
    public boolean advance() throws IOException;

    /**
     * Returns the value of the data item that was read by the last {@link #start} or
     * {@link #advance} call. The returned value must be effectively immutable and remain valid
     * indefinitely.
     *
     * @throws java.util.NoSuchElementException if the reader is at the beginning of the input and
     *         {@link #start} or {@link #advance} wasn't called, or if the last {@link #start} or
     *         {@link #advance} returned {@code false}.
     */
    public T getCurrent() throws NoSuchElementException;

    /**
     * Returns the timestamp associated with the current data item.
     * <p>
     * If the source does not support timestamps, this should return
     * {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
     *
     * @throws NoSuchElementException
     */
    public Instant getCurrentTimestamp() throws NoSuchElementException;

    /**
     * Closes the reader. The reader cannot be used after this method is called.
     */
    @Override
    public void close() throws IOException;

    /**
     * Returns a {@code Source} describing the same input that this {@code Reader} reads
     * (including items already read).
     * <p>
     * A reader created from the result of {@code getCurrentSource}, if consumed, MUST
     * return the same data items as the current reader.
     */
    public Source<T> getCurrentSource();
  }

  /**
   * A base class implementing optional methods of {@link Reader} in a default way:
   * <ul>
   *   <li>All values have the timestamp of {@code BoundedWindow.TIMESTAMP_MIN_VALUE}.
   * </ul>
   * @param <T>
   */
  public abstract static class AbstractReader<T> implements Reader<T> {
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }
}
