/*
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
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.WindowedValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * Base class for defining input formats, with custom logic for splitting the input
 * into shards (parts of the input, each of which may be processed on a different worker)
 * and creating a {@code Source} for reading the input.
 *
 * <p> To use this class for supporting your custom input type, derive your class
 * class from it, and override the abstract methods. Also override either
 * {@link #createWindowedReader} if your source supports timestamps and windows,
 * or {@link #createBasicReader} otherwise. For an example, see {@link DatastoreIO}.
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
 * <p> This API is experimental and subject to change.
 *
 * @param <T> Type of elements read by the source.
 */
public abstract class Source<T> implements Serializable {
  /**
   * Splits the source into shards.
   *
   * <p> {@code PipelineOptions} can be used to get information such as
   * credentials for accessing an external storage.
   */
  public abstract List<? extends Source<T>> splitIntoShards(
      long desiredShardSizeBytes, PipelineOptions options) throws Exception;

  /**
   * An estimate of the total size (in bytes) of the data that would be read from this source.
   * This estimate is in terms of external storage size, before any decompression or other
   * processing done by the reader.
   */
  public abstract long getEstimatedSizeBytes(PipelineOptions options) throws Exception;

  /**
   * Whether this source is known to produce key/value pairs with the (encoded) keys in
   * lexicographically sorted order.
   */
  public abstract boolean producesSortedKeys(PipelineOptions options) throws Exception;

  /**
   * Creates a windowed reader for this source. The default implementation wraps
   * {@link #createBasicReader}. Override this function if your reader supports timestamps
   * and windows; otherwise, override {@link #createBasicReader} instead.
   */
  public Reader<WindowedValue<T>> createWindowedReader(PipelineOptions options,
      Coder<WindowedValue<T>> coder, @Nullable ExecutionContext executionContext)
      throws IOException {
    return new WindowedReaderWrapper<T>(createBasicReader(
        options, ((WindowedValue.WindowedValueCoder<T>) coder).getValueCoder(), executionContext));
  }

  /**
   * Creates a basic (non-windowed) reader for this source. If you override this method, each value
   * returned by this reader will be wrapped into the global window.
   */
  protected Reader<T> createBasicReader(PipelineOptions options, Coder<T> coder,
      @Nullable ExecutionContext executionContext) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Checks that this source is valid, before it can be used into a pipeline.
   * It is recommended to use {@link com.google.common.base.Preconditions} for implementing
   * this method.
   */
  public abstract void validate();

  /**
   * Returns the default {@code Coder} to use for the data read from this source.
   */
  public abstract Coder<T> getDefaultOutputCoder();

  /**
   * The interface which readers of custom input sources must implement.
   * <p>
   * This interface is deliberately distinct from {@link java.util.Iterator} because
   * the current model tends to be easier to program and more efficient in practice
   * for iterating over sources such as files, databases etc. (rather than pure collections).
   * <p>
   * To read a {@code SourceIterator}:
   * <pre>
   * while (iterator.advance()) {
   *   T item = iterator.getCurrent();
   *   ...
   * }
   * </pre>
   * <p>
   * Note: this interface is work-in-progress and may change.
   */
  public interface Reader<T> extends AutoCloseable {
    /**
     * Advances the iterator to the next valid record.
     * Invalidates the result of the previous {@link #getCurrent} call.
     * @return {@code true} if a record was read, {@code false} if we're at the end of input.
     */
    public boolean advance() throws IOException;

    /**
     * Returns the value of the data item which was read by the last {@link #advance} call.
     * @throws java.util.NoSuchElementException if the iterator is at the beginning of the input
     *   and {@link #advance} wasn't called, or if the last {@link #advance} returned {@code false}.
     */
    public T getCurrent() throws NoSuchElementException;

    /**
     * Closes the iterator. The iterator cannot be used after this method was called.
     */
    @Override
    public void close() throws IOException;
  }

  /**
   * An adapter from {@code SourceIterator<T>} to {@code SourceIterator<WindowedValue<T>>}.
   */
  private static class WindowedReaderWrapper<T> implements Reader<WindowedValue<T>> {
    private final Reader<T> reader;

    public WindowedReaderWrapper(Reader<T> reader) {
      this.reader = reader;
    }

    @Override
    public boolean advance() throws IOException {
      return reader.advance();
    }

    @Override
    public WindowedValue<T> getCurrent() throws NoSuchElementException {
      return WindowedValue.valueInGlobalWindow(reader.getCurrent());
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
