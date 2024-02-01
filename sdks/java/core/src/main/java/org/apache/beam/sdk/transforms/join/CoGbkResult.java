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
package org.apache.beam.sdk.transforms.join;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A row result of a {@link CoGroupByKey}. This is a tuple of {@link Iterable}s produced for a given
 * key, and these can be accessed in different ways.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class CoGbkResult {
  /**
   * A map of integer union tags to a list of union objects. Note: the key and the embedded union
   * tag are the same, so it is redundant to store it multiple times, but for now it makes encoding
   * easier.
   */
  private final List<Iterable<?>> valueMap;

  private final CoGbkResultSchema schema;

  private static final int DEFAULT_IN_MEMORY_ELEMENT_COUNT = 10_000;

  /**
   * Always try to cache at least this many elements per tag, even if it requires caching more than
   * the total in memory count.
   */
  private static final int DEFAULT_MIN_ELEMENTS_PER_TAG = 100;

  private static final Logger LOG = LoggerFactory.getLogger(CoGbkResult.class);

  /**
   * A row in the {@link PCollection} resulting from a {@link CoGroupByKey} transform. Currently,
   * this row must fit into memory.
   *
   * @param schema the set of tuple tags used to refer to input tables and result values
   * @param taggedValues the raw results from a group-by-key
   */
  public CoGbkResult(CoGbkResultSchema schema, Iterable<RawUnionValue> taggedValues) {
    this(schema, taggedValues, DEFAULT_IN_MEMORY_ELEMENT_COUNT, DEFAULT_MIN_ELEMENTS_PER_TAG);
  }

  @SuppressWarnings("unchecked")
  public CoGbkResult(
      CoGbkResultSchema schema,
      Iterable<RawUnionValue> taggedValues,
      int inMemoryElementCount,
      int minElementsPerTag) {
    this.schema = schema;
    List<List<Object>> valuesByTag = new ArrayList<>();
    for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
      valuesByTag.add(new ArrayList<>());
    }

    // Demultiplex the first imMemoryElementCount tagged union values
    // according to their tag.
    final Iterator<RawUnionValue> taggedIter = taggedValues.iterator();
    int elementCount = 0;
    boolean isReiterator = taggedIter instanceof Reiterator;
    while (taggedIter.hasNext()) {
      if (isReiterator && elementCount++ >= inMemoryElementCount) {
        // Let the tails be lazy.
        break;
      }
      RawUnionValue value = taggedIter.next();
      // Make sure the given union tag has a corresponding tuple tag in the
      // schema.
      int unionTag = value.getUnionTag();
      if (schema.size() <= unionTag) {
        throw new IllegalStateException(
            "union tag " + unionTag + " has no corresponding tuple tag in the result schema");
      }
      valuesByTag.get(unionTag).add(value.getValue());
    }

    if (!taggedIter.hasNext()) {
      valueMap = (List) valuesByTag;
      return;
    }

    // If we get here, there were more elements than we can afford to
    // keep in memory, so we copy the re-iterable of remaining items
    // and append filtered views to each of the sorted lists computed earlier.
    LOG.info(
        "CoGbkResult has more than {} elements, reiteration (which may be slow) is required.",
        inMemoryElementCount);
    final Reiterator<RawUnionValue> tail = (Reiterator<RawUnionValue>) taggedIter;

    // As we iterate over this re-iterable (e.g. while iterating for one tag) we populate values
    // for other observed tags, if any.
    ObservingReiterator<RawUnionValue> tip =
        new ObservingReiterator<>(
            tail,
            new ObservingReiterator.Observer<RawUnionValue>() {
              @Override
              public void observeAt(ObservingReiterator<RawUnionValue> reiterator) {
                ((TagIterable<?>) valueMap.get(reiterator.peek().getUnionTag())).offer(reiterator);
              }

              @Override
              public void done() {
                // Inform all tags that we have reached the end of the iterable, so anything that
                // can be observed has been observed.
                for (Iterable<?> iter : valueMap) {
                  ((TagIterable<?>) iter).finish();
                }
              }
            });

    valueMap = new ArrayList<>();
    for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
      valueMap.add(
          new TagIterable<Object>(valuesByTag.get(unionTag), unionTag, minElementsPerTag, tip));
    }
  }

  public boolean isEmpty() {
    for (Iterable<?> tagValues : valueMap) {
      if (tagValues.iterator().hasNext()) {
        return false;
      }
    }
    return true;
  }

  /** Returns the schema used by this {@link CoGbkResult}. */
  public CoGbkResultSchema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return valueMap.toString();
  }

  /**
   * Returns the values from the table represented by the given {@code TupleTag<V>} as an {@code
   * Iterable<V>} (which may be empty if there are no results).
   *
   * <p>If tag was not part of the original {@link CoGroupByKey}, throws an
   * IllegalArgumentException.
   */
  public <V> Iterable<V> getAll(TupleTag<V> tag) {
    int index = schema.getIndex(tag);
    if (index < 0) {
      throw new IllegalArgumentException("TupleTag " + tag + " is not in the schema");
    }
    @SuppressWarnings("unchecked")
    Iterable<V> unions = (Iterable<V>) valueMap.get(index);
    return unions;
  }

  /** Like {@link #getAll(TupleTag)} but using a String instead of a {@link TupleTag}. */
  public <V> Iterable<V> getAll(String tag) {
    return getAll(new TupleTag<>(tag));
  }

  /**
   * If there is a singleton value for the given tag, returns it. Otherwise, throws an
   * IllegalArgumentException.
   *
   * <p>If tag was not part of the original {@link CoGroupByKey}, throws an
   * IllegalArgumentException.
   */
  public <V> V getOnly(TupleTag<V> tag) {
    return innerGetOnly(tag, null, false);
  }

  /** Like {@link #getOnly(TupleTag)} but using a String instead of a TupleTag. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <V> V getOnly(String tag) {
    return getOnly(new TupleTag<>(tag));
  }

  /**
   * If there is a singleton value for the given tag, returns it. If there is no value for the given
   * tag, returns the defaultValue.
   *
   * <p>If tag was not part of the original {@link CoGroupByKey}, throws an
   * IllegalArgumentException.
   */
  public @Nullable <V> V getOnly(TupleTag<V> tag, @Nullable V defaultValue) {
    return innerGetOnly(tag, defaultValue, true);
  }

  /** Like {@link #getOnly(TupleTag, Object)} but using a String instead of a TupleTag. */
  public @Nullable <V> V getOnly(String tag, @Nullable V defaultValue) {
    return getOnly(new TupleTag<>(tag), defaultValue);
  }

  /** A {@link Coder} for {@link CoGbkResult}s. */
  public static class CoGbkResultCoder extends CustomCoder<CoGbkResult> {

    private final CoGbkResultSchema schema;
    private final UnionCoder unionCoder;

    /** Returns a {@link CoGbkResultCoder} for the given schema and {@link UnionCoder}. */
    public static CoGbkResultCoder of(CoGbkResultSchema schema, UnionCoder unionCoder) {
      return new CoGbkResultCoder(schema, unionCoder);
    }

    private CoGbkResultCoder(CoGbkResultSchema tupleTags, UnionCoder unionCoder) {
      this.schema = tupleTags;
      this.unionCoder = unionCoder;
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return ImmutableList.of(unionCoder);
    }

    public CoGbkResultSchema getSchema() {
      return schema;
    }

    public UnionCoder getUnionCoder() {
      return unionCoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void encode(CoGbkResult value, OutputStream outStream)
        throws CoderException, IOException {
      if (!schema.equals(value.getSchema())) {
        throw new CoderException("input schema does not match coder schema");
      }
      if (schema.size() == 0) {
        return;
      }
      for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
        tagListCoder(unionTag).encode(value.valueMap.get(unionTag), outStream);
      }
    }

    @Override
    public CoGbkResult decode(InputStream inStream) throws CoderException, IOException {
      if (schema.size() == 0) {
        return new CoGbkResult(schema, ImmutableList.<Iterable<?>>of());
      }
      List<Iterable<?>> valueMap = Lists.newArrayListWithExpectedSize(schema.size());
      for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
        valueMap.add(tagListCoder(unionTag).decode(inStream));
      }
      return new CoGbkResult(schema, valueMap);
    }

    @SuppressWarnings("rawtypes")
    private IterableCoder tagListCoder(int unionTag) {
      return IterableCoder.of(unionCoder.getElementCoders().get(unionTag));
    }

    @Override
    public boolean equals(@Nullable Object object) {
      if (this == object) {
        return true;
      }
      if (!(object instanceof CoGbkResultCoder)) {
        return false;
      }
      CoGbkResultCoder other = (CoGbkResultCoder) object;
      return schema.equals(other.schema) && unionCoder.equals(other.unionCoder);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(schema);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          this, "CoGbkResult requires the union coder to be deterministic", unionCoder);
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Methods for directly constructing a CoGbkResult
  //
  // (for example, creating test data for a transform that consumes a
  // CoGbkResult)

  /** Returns a new CoGbkResult that contains just the given tag and given data. */
  public static <V> CoGbkResult of(TupleTag<V> tag, List<V> data) {
    return CoGbkResult.empty().and(tag, data);
  }

  /**
   * Returns a new {@link CoGbkResult} based on this, with the given tag and given data added to it.
   */
  public <V> CoGbkResult and(TupleTag<V> tag, List<V> data) {
    if (nextTestUnionId != schema.size()) {
      throw new IllegalArgumentException(
          "Attempting to call and() on a CoGbkResult apparently not created by" + " of().");
    }
    List<Iterable<?>> valueMap = new ArrayList<>(this.valueMap);
    valueMap.add(data);
    return new CoGbkResult(
        new CoGbkResultSchema(schema.getTupleTagList().and(tag)), valueMap, nextTestUnionId + 1);
  }

  /** Returns an empty {@link CoGbkResult}. */
  public static <V> CoGbkResult empty() {
    return new CoGbkResult(
        new CoGbkResultSchema(TupleTagList.empty()), new ArrayList<Iterable<?>>());
  }

  //////////////////////////////////////////////////////////////////////////////

  private int nextTestUnionId = 0;

  private CoGbkResult(CoGbkResultSchema schema, List<Iterable<?>> valueMap, int nextTestUnionId) {
    this(schema, valueMap);
    this.nextTestUnionId = nextTestUnionId;
  }

  private CoGbkResult(CoGbkResultSchema schema, List<Iterable<?>> valueMap) {
    this.schema = schema;
    this.valueMap = valueMap;
  }

  private @Nullable <V> V innerGetOnly(
      TupleTag<V> tag, @Nullable V defaultValue, boolean useDefault) {
    int index = schema.getIndex(tag);
    if (index < 0) {
      throw new IllegalArgumentException("TupleTag " + tag + " is not in the schema");
    }
    @SuppressWarnings("unchecked")
    Iterator<V> unions = (Iterator<V>) valueMap.get(index).iterator();
    if (!unions.hasNext()) {
      if (useDefault) {
        return defaultValue;
      } else {
        throw new IllegalArgumentException(
            "TupleTag " + tag + " corresponds to an empty result, and no default was provided");
      }
    }
    V value = unions.next();
    if (unions.hasNext()) {
      throw new IllegalArgumentException(
          "TupleTag " + tag + " corresponds to a non-singleton result");
    }
    return value;
  }

  /**
   * A re-iterable that notifies an observer at every advance, and upon finishing, but only once
   * across all copies.
   *
   * @param <T> The value type of the underlying iterable.
   */
  private static class ObservingReiterator<T> implements Reiterator<T> {

    public interface Observer<T> {
      /**
       * Called exactly once, across all copies before advancing this iterator.
       *
       * <p>The iterator rather than the element is given so that the callee can perform a copy if
       * desired. This class offers a peek method to get at the current element without disturbing
       * the state of this iterator.
       */
      void observeAt(ObservingReiterator<T> reiterator);

      /** Called exactly once, across all copies, once this iterator is exhausted. */
      void done();
    }

    private PeekingReiterator<IndexingReiterator.Indexed<T>> underlying;
    private Observer<T> observer;

    // Used to keep track of what has been observed so far.
    // These are arrays to facilitate sharing values among all copies of the same root Reiterator.
    private final int[] lastObserved;
    private final boolean[] doneHasRun;
    private final PeekingReiterator<IndexingReiterator.Indexed<T>>[] mostAdvanced;

    public ObservingReiterator(Reiterator<T> underlying, Observer<T> observer) {
      this(new PeekingReiterator<>(new IndexingReiterator<>(underlying)), observer);
    }

    @SuppressWarnings("rawtypes") // array creation
    public ObservingReiterator(
        PeekingReiterator<IndexingReiterator.Indexed<T>> underlying, Observer<T> observer) {
      this(
          underlying,
          observer,
          new int[] {-1},
          new boolean[] {false},
          new PeekingReiterator[] {underlying});
    }

    private ObservingReiterator(
        PeekingReiterator<IndexingReiterator.Indexed<T>> underlying,
        Observer<T> observer,
        int[] lastObserved,
        boolean[] doneHasRun,
        PeekingReiterator<IndexingReiterator.Indexed<T>>[] mostAdvanced) {
      this.underlying = underlying;
      this.observer = observer;
      this.lastObserved = lastObserved;
      this.doneHasRun = doneHasRun;
      this.mostAdvanced = mostAdvanced;
    }

    @Override
    public Reiterator<T> copy() {
      return new ObservingReiterator<T>(
          underlying.copy(), observer, lastObserved, doneHasRun, mostAdvanced);
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = underlying.hasNext();
      if (!hasNext && !doneHasRun[0]) {
        mostAdvanced[0] = underlying;
        observer.done();
        doneHasRun[0] = true;
      }
      return hasNext;
    }

    @Override
    public T next() {
      peek(); // trigger observation *before* advancing
      return underlying.next().value;
    }

    public T peek() {
      IndexingReiterator.Indexed<T> next = underlying.peek();
      if (next.index > lastObserved[0]) {
        assert next.index == lastObserved[0] + 1;
        mostAdvanced[0] = underlying;
        lastObserved[0] = next.index;
        observer.observeAt(this);
      }
      return next.value;
    }

    public void fastForward() {
      if (underlying != mostAdvanced[0]) {
        underlying = mostAdvanced[0].copy();
      }
    }
  }

  /**
   * Assigns a monotonically increasing index to each item in the underling Reiterator.
   *
   * @param <T> The value type of the underlying iterable.
   */
  private static class IndexingReiterator<T> implements Reiterator<IndexingReiterator.Indexed<T>> {

    private Reiterator<T> underlying;
    private int index;

    public IndexingReiterator(Reiterator<T> underlying) {
      this(underlying, 0);
    }

    public IndexingReiterator(Reiterator<T> underlying, int start) {
      this.underlying = underlying;
      this.index = start;
    }

    @Override
    public IndexingReiterator<T> copy() {
      return new IndexingReiterator<>(underlying.copy(), index);
    }

    @Override
    public boolean hasNext() {
      return underlying.hasNext();
    }

    @Override
    public Indexed<T> next() {
      return new Indexed<T>(index++, underlying.next());
    }

    public static class Indexed<T> {
      public final int index;
      public final T value;

      public Indexed(int index, T value) {
        this.index = index;
        this.value = value;
      }
    }
  }

  /**
   * Adapts an Reiterator, giving it a peek() method that can be used to observe the next element
   * without consuming it.
   *
   * @param <T> The value type of the underlying iterable.
   */
  private static class PeekingReiterator<T> implements Reiterator<T> {
    private Reiterator<T> underlying;
    private T next;
    private boolean nextIsValid;

    public PeekingReiterator(Reiterator<T> underlying) {
      this(underlying, null, false);
    }

    private PeekingReiterator(Reiterator<T> underlying, T next, boolean nextIsValid) {
      this.underlying = underlying;
      this.next = next;
      this.nextIsValid = nextIsValid;
    }

    @Override
    public PeekingReiterator<T> copy() {
      return new PeekingReiterator<>(underlying.copy(), next, nextIsValid);
    }

    @Override
    public boolean hasNext() {
      return nextIsValid || underlying.hasNext();
    }

    @Override
    public T next() {
      if (nextIsValid) {
        nextIsValid = false;
        return next;
      } else {
        return underlying.next();
      }
    }

    public T peek() {
      if (!nextIsValid) {
        next = underlying.next();
        nextIsValid = true;
      }
      return next;
    }
  }

  /**
   * An Iterable corresponding to a single tag.
   *
   * <p>The values in this iterable are populated lazily via the offer method as tip advances for
   * any tag.
   *
   * @param <T> The value type of the corresponding tag.
   */
  private static class TagIterable<T> implements Iterable<T> {
    int tag;
    int cacheSize;

    ObservingReiterator<RawUnionValue> tip;

    List<T> head;
    Reiterator<RawUnionValue> tail;
    boolean finished;

    public TagIterable(
        List<T> head, int tag, int cacheSize, ObservingReiterator<RawUnionValue> tip) {
      this.tag = tag;
      this.cacheSize = cacheSize;
      this.head = head;
      this.tip = tip;
    }

    void offer(ObservingReiterator<RawUnionValue> tail) {
      assert !finished;
      assert tail.peek().getUnionTag() == tag;
      if (head.size() < cacheSize) {
        head.add((T) tail.peek().getValue());
      } else if (this.tail == null) {
        this.tail = tail.copy();
      }
    }

    void finish() {
      finished = true;
    }

    void seek(int tag) {
      while (tip.hasNext() && tip.peek().getUnionTag() != tag) {
        tip.next();
      }
    }

    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {

        boolean isDone;
        boolean advanced;
        T next;

        /** Keeps track of the index, in head, that this iterator points to. */
        int index = -1;
        /** If the index is beyond what was cached in head, this is this iterators view of tail. */
        Iterator<T> tailIter;

        @Override
        public boolean hasNext() {
          if (!advanced) {
            advance();
          }
          return !isDone;
        }

        @Override
        public T next() {
          if (!advanced) {
            advance();
          }
          if (isDone) {
            throw new NoSuchElementException();
          }
          advanced = false;
          return next;
        }

        // Invariant: After advanced is called, either isDone is true or next is valid.
        private void advance() {
          assert !advanced;
          assert !isDone;
          advanced = true;

          index++;
          if (maybeAdvance()) {
            return;
          }

          // We were unable to advance; advance tip to populate either head or tail.
          tip.fastForward();
          if (tip.hasNext()) {
            tip.next();
            seek(tag);
          }

          // A this point, either head or tail should be sufficient to advance.
          Preconditions.checkState(maybeAdvance());
        }

        // Invariant: If returns true, either isDone is true or next is valid.
        private boolean maybeAdvance() {
          if (index < head.size()) {
            // First consume head.
            assert tailIter == null;
            next = head.get(index);
            return true;
          } else if (tail != null) {
            // Next consume tail, if any.
            if (tailIter == null) {
              tailIter =
                  Iterators.transform(
                      Iterators.filter(
                          tail.copy(), taggedUnion -> taggedUnion.getUnionTag() == tag),
                      taggedUnion -> (T) taggedUnion.getValue());
            }
            if (tailIter.hasNext()) {
              next = tailIter.next();
            } else {
              isDone = true;
            }
            return true;
          } else if (finished) {
            // If there are no more elements in head, and tail was not populated, and we are
            // finished, this is the end of the iteration.
            isDone = true;
            return true;
          } else {
            // We need more elements in either head or tail.
            return false;
          }
        }
      };
    }
  }
}
