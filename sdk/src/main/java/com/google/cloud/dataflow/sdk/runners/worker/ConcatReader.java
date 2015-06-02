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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.client.util.Preconditions;
import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link Reader} that reads elements from a given set of encoded {@code Source}s. Creates {@code
 * Reader}s for sources lazily, i.e. only when elements from the particular {@code Reader} are about
 * to be read.
 *
 * <p> This class does does not cache {@code Reader}s and creates a new {@code Reader} every time a
 * new {@code ReaderIterator} has to be created. Because of this, multiple iterators created using
 * the same {@code ConcatReader} will not be able to share any state between each other. This design
 * was chosen since keeping a large number of {@code Reader} objects alive within a single
 * {@code ConcatReader} could be highly memory consuming.
 *
 * @param <T> Type of the elements read by the {@code Reader}s.
 */
public class ConcatReader<T> extends Reader<T> {
  public static final String SOURCE_NAME = "ConcatSource";

  private final List<Source> sources;
  private final PipelineOptions options;
  private final ExecutionContext executionContext;

  /**
   * Create a {@code ConcatReader} using a given list of encoded {@code Source}s.
   */
  public ConcatReader(
      PipelineOptions options, ExecutionContext executionContext, List<Source> sources) {
    Preconditions.checkNotNull(sources);
    this.sources = sources;
    this.options = options;
    this.executionContext = executionContext;
  }

  public Iterator<Source> getSources() {
    return sources.iterator();
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    return new ConcatIterator<T>(options, executionContext, sources);
  }

  private static class ConcatIterator<T> extends AbstractReaderIterator<T> {
    private int currentIteratorIndex = -1;
    private ReaderIterator<T> currentIterator = null;
    private final List<Source> sources;
    private final PipelineOptions options;
    private final ExecutionContext executionContext;

    public ConcatIterator(
        PipelineOptions options, ExecutionContext executionContext, List<Source> sources) {
      this.sources = sources;
      this.options = options;
      this.executionContext = executionContext;
    }

    @Override
    public boolean hasNext() throws IOException {
      for (;;) {
        if (currentIterator != null && currentIterator.hasNext()) {
          return true;
        }

        if (currentIterator != null) {
          currentIterator.close();
        }

        currentIteratorIndex++;
        if (currentIteratorIndex == sources.size()) {
          // All sources were read.
          return false;
        }

        Source currentSource = sources.get(currentIteratorIndex);
        try {
          @SuppressWarnings("unchecked")
          Reader<T> currentReader =
              (Reader<T>) ReaderFactory.create(options, currentSource, executionContext);
          currentIterator = currentReader.iterator();
        } catch (Exception e) {
          throw new IOException("Failed to create a reader for source: " + currentSource, e);
        }
      }
    }

    @Override
    public T next() throws IOException, NoSuchElementException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      } else {
        return currentIterator.next();
      }
    }

    @Override
    public void close() throws IOException {
      if (currentIterator != null) {
        currentIterator.close();
      }
    }
  }
}
