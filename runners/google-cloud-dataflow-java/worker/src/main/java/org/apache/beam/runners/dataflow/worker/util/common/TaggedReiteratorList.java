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
package org.apache.beam.runners.dataflow.worker.util.common;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.PeekingReiterator;
import org.apache.beam.sdk.util.common.Reiterator;

/**
 * Provides a view a of re-iterable of tagged values, with monotonically increasing tags, as a list
 * of tagged re-iterables.
 *
 * <p>This class, and the returned iterators, are not threadsafe.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TaggedReiteratorList extends AbstractList<Reiterator<Object>> {

  /** Interface for extracting the tag and value from an opaque element. */
  public interface TagExtractor<T> {
    public int getTag(T elem);

    public Object getValue(T elem);
  }

  private final TagExtractor<Object> extractor;

  private final List<PeekingReiterator<Object>> starts;

  private final int size;

  public <T> TaggedReiteratorList(Reiterator<T> taggedReiterator, TagExtractor<T> extractor) {
    this(taggedReiterator, extractor, -1);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> TaggedReiteratorList(
      Reiterator<T> taggedReiterator, TagExtractor<T> extractor, int size) {
    starts = new ArrayList<>();
    starts.add(new PeekingReiterator<Object>((Reiterator) taggedReiterator));
    this.extractor = (TagExtractor) extractor;
    this.size = size;
  }

  @Override
  public Reiterator<Object> get(int tag) {
    return new SubIterator(tag);
  }

  @Override
  public int size() {
    if (size == -1) {
      throw new UnsupportedOperationException();
    } else {
      return size;
    }
  }

  private PeekingReiterator<Object> getStart(int tag) {
    if (tag >= starts.size()) {
      PeekingReiterator<Object> start = getStart(tag - 1);
      while (start.hasNext() && extractor.getTag(start.peek()) < tag) {
        start.next();
      }
      starts.add(start);
    }
    // Use the stored value, store a copy.
    return starts.set(tag, starts.get(tag).copy());
  }

  private static final PeekingReiterator<Object> EMPTY_TAIL =
      new PeekingReiterator<Object>(
          new Reiterator<Object>() {
            @Override
            public boolean hasNext() {
              return false;
            }

            @Override
            public Object next() {
              throw new NoSuchElementException();
            }

            @Override
            public void remove() {
              throw new IllegalArgumentException();
            }

            @Override
            public Reiterator<Object> copy() {
              throw new IllegalArgumentException();
            }
          });

  private class SubIterator implements Reiterator<Object> {

    private final int tag;
    private PeekingReiterator<Object> iterator;

    private SubIterator(int tag) {
      this(tag, null);
    }

    private SubIterator(int tag, PeekingReiterator<Object> iterator) {
      this.tag = tag;
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      if (iterator == null) {
        iterator = getStart(tag);
      }
      if (iterator.hasNext() && extractor.getTag(iterator.peek()) == tag) {
        return true;
      } else {
        if (iterator != EMPTY_TAIL) {
          // Set up for the common case that we're iterating over the
          // next tag soon.
          if (starts.size() > tag + 1) {
            starts.set(tag + 1, iterator);
          } else {
            starts.add(tag + 1, iterator);
          }
          iterator = EMPTY_TAIL;
        }
        return false;
      }
    }

    @Override
    public Object next() {
      if (hasNext()) {
        return extractor.getValue(iterator.next());
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove() {
      throw new IllegalArgumentException();
    }

    @Override
    public Reiterator<Object> copy() {
      return new SubIterator(
          tag, iterator == null || iterator == EMPTY_TAIL ? iterator : iterator.copy());
    }
  }
}
