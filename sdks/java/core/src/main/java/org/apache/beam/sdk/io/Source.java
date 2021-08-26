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
import java.io.Serializable;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.joda.time.Instant;

/**
 * Base class for defining input formats and creating a {@code Source} for reading the input.
 *
 * <p>This class is not intended to be subclassed directly. Instead, to define a bounded source (a
 * source which produces a finite amount of input), subclass {@link BoundedSource}; to define an
 * unbounded source, subclass {@link UnboundedSource}.
 *
 * <p>A {@code Source} passed to a {@code Read} transform must be {@code Serializable}. This allows
 * the {@code Source} instance created in this "main program" to be sent (in serialized form) to
 * remote worker machines and reconstituted for each batch of elements of the input {@code
 * PCollection} being processed or for each source splitting operation. A {@code Source} can have
 * instance variable state, and non-transient instance variable state will be serialized in the main
 * program and then deserialized on remote worker machines.
 *
 * <p>{@code Source} classes MUST be effectively immutable. The only acceptable use of mutable
 * fields is to cache the results of expensive operations, and such fields MUST be marked {@code
 * transient}.
 *
 * <p>{@code Source} objects should override {@link Object#toString}, as it will be used in
 * important error and debugging messages.
 *
 * @param <T> Type of elements read by the source.
 */
@Experimental(Kind.SOURCE_SINK)
public abstract class Source<T> implements Serializable, HasDisplayData {
  /**
   * Checks that this source is valid, before it can be used in a pipeline.
   *
   * <p>It is recommended to use {@link Preconditions} for implementing this method.
   */
  public void validate() {}

  /** @deprecated Override {@link #getOutputCoder()} instead. */
  @Deprecated
  public Coder<T> getDefaultOutputCoder() {
    // If the subclass doesn't override getDefaultOutputCoder(), hopefully it overrides the proper
    // version - getOutputCoder(). Check that it does, before calling the method (if subclass
    // doesn't override it, we'll call the default implementation and get infinite recursion).
    try {
      if (getClass().getMethod("getOutputCoder").getDeclaringClass().equals(Source.class)) {
        throw new UnsupportedOperationException(
            getClass() + " needs to override getOutputCoder().");
      }
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    return getOutputCoder();
  }

  /** Returns the {@code Coder} to use for the data read from this source. */
  public Coder<T> getOutputCoder() {
    // Call the old method for compatibility.
    return getDefaultOutputCoder();
  }

  /**
   * {@inheritDoc}
   *
   * <p>By default, does not register any display data. Implementors may override this method to
   * provide their own display data.
   */
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {}

  /**
   * The interface that readers of custom input sources must implement.
   *
   * <p>This interface is deliberately distinct from {@link java.util.Iterator} because the current
   * model tends to be easier to program and more efficient in practice for iterating over sources
   * such as files, databases etc. (rather than pure collections).
   *
   * <p>Reading data from the {@link Reader} must obey the following access pattern:
   *
   * <ul>
   *   <li>One call to {@link #start}
   *       <ul>
   *         <li>If {@link #start} returned true, any number of calls to {@code getCurrent}* methods
   *       </ul>
   *   <li>Repeatedly, a call to {@link #advance}. This may be called regardless of what the
   *       previous {@link #start}/{@link #advance} returned.
   *       <ul>
   *         <li>If {@link #advance} returned true, any number of calls to {@code getCurrent}*
   *             methods
   *       </ul>
   * </ul>
   *
   * <p>For example, if the reader is reading a fixed set of data:
   *
   * <pre>
   *   try {
   *     for (boolean available = reader.start(); available; available = reader.advance()) {
   *       T item = reader.getCurrent();
   *       Instant timestamp = reader.getCurrentTimestamp();
   *       ...
   *     }
   *   } finally {
   *     reader.close();
   *   }
   * </pre>
   *
   * <p>If the set of data being read is continually growing:
   *
   * <pre>
   *   try {
   *     boolean available = reader.start();
   *     while (true) {
   *       if (available) {
   *         T item = reader.getCurrent();
   *         Instant timestamp = reader.getCurrentTimestamp();
   *         ...
   *         resetExponentialBackoff();
   *       } else {
   *         exponentialBackoff();
   *       }
   *       available = reader.advance();
   *     }
   *   } finally {
   *     reader.close();
   *   }
   * </pre>
   *
   * <p>Note: this interface is a work-in-progress and may change.
   *
   * <p>All {@code Reader} functions except {@link #getCurrentSource} do not need to be thread-safe;
   * they may only be accessed by a single thread at once. However, {@link #getCurrentSource} needs
   * to be thread-safe, and other functions should assume that its returned value can change
   * asynchronously.
   */
  public abstract static class Reader<T> implements AutoCloseable {
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
     * @throws java.util.NoSuchElementException if {@link #start} was never called, or if the last
     *     {@link #start} or {@link #advance} returned {@code false}.
     */
    public abstract T getCurrent() throws NoSuchElementException;

    /**
     * Returns the timestamp associated with the current data item.
     *
     * <p>If the source does not support timestamps, this should return {@code
     * BoundedWindow.TIMESTAMP_MIN_VALUE}.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws NoSuchElementException if the reader is at the beginning of the input and {@link
     *     #start} or {@link #advance} wasn't called, or if the last {@link #start} or {@link
     *     #advance} returned {@code false}.
     */
    public abstract Instant getCurrentTimestamp() throws NoSuchElementException;

    /** Closes the reader. The reader cannot be used after this method is called. */
    @Override
    public abstract void close() throws IOException;

    /**
     * Returns a {@code Source} describing the same input that this {@code Reader} currently reads
     * (including items already read).
     *
     * <p>Usually, an implementation will simply return the immutable {@link Source} object from
     * which the current {@link Reader} was constructed, or delegate to the base class. However,
     * when using or implementing this method on a {@link BoundedSource.BoundedReader}, special
     * considerations apply, see documentation for {@link
     * BoundedSource.BoundedReader#getCurrentSource}.
     */
    public abstract Source<T> getCurrentSource();
  }
}
