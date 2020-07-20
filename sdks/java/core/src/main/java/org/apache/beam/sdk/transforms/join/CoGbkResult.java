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
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A row result of a {@link CoGroupByKey}. This is a tuple of {@link Iterable}s produced for a given
 * key, and these can be accessed in different ways.
 */
public class CoGbkResult {
  /**
   * A map of integer union tags to a list of union objects. Note: the key and the embedded union
   * tag are the same, so it is redundant to store it multiple times, but for now it makes encoding
   * easier.
   */
  private final List<Iterable<?>> valueMap;

  private final CoGbkResultSchema schema;

  private static final int DEFAULT_IN_MEMORY_ELEMENT_COUNT = 10_000;

  private static final Logger LOG = LoggerFactory.getLogger(CoGbkResult.class);

  /**
   * A row in the {@link PCollection} resulting from a {@link CoGroupByKey} transform. Currently,
   * this row must fit into memory.
   *
   * @param schema the set of tuple tags used to refer to input tables and result values
   * @param taggedValues the raw results from a group-by-key
   */
  public CoGbkResult(CoGbkResultSchema schema, Iterable<RawUnionValue> taggedValues) {
    this(schema, taggedValues, DEFAULT_IN_MEMORY_ELEMENT_COUNT);
  }

  @SuppressWarnings("unchecked")
  public CoGbkResult(
      CoGbkResultSchema schema, Iterable<RawUnionValue> taggedValues, int inMemoryElementCount) {
    this.schema = schema;
    valueMap = new ArrayList<>();
    for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
      valueMap.add(new ArrayList<>());
    }

    // Demultiplex the first imMemoryElementCount tagged union values
    // according to their tag.
    final Iterator<RawUnionValue> taggedIter = taggedValues.iterator();
    int elementCount = 0;
    while (taggedIter.hasNext()) {
      if (elementCount++ >= inMemoryElementCount && taggedIter instanceof Reiterator) {
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
      List<Object> valueList = (List<Object>) valueMap.get(unionTag);
      valueList.add(value.getValue());
    }

    if (taggedIter.hasNext()) {
      // If we get here, there were more elements than we can afford to
      // keep in memory, so we copy the re-iterable of remaining items
      // and append filtered views to each of the sorted lists computed earlier.
      LOG.info(
          "CoGbkResult has more than "
              + inMemoryElementCount
              + " elements,"
              + " reiteration (which may be slow) is required.");
      final Reiterator<RawUnionValue> tail = (Reiterator<RawUnionValue>) taggedIter;
      // This is a trinary-state array recording whether a given tag is present in the tail. The
      // initial value is null (unknown) for all tags, and the first iteration through the entire
      // list will set these values to true or false to avoid needlessly iterating if filtering
      // against a given tag would not match anything.
      final Boolean[] containsTag = new Boolean[schema.size()];
      for (int unionTag = 0; unionTag < schema.size(); unionTag++) {
        updateUnionTag(tail, containsTag, unionTag);
      }
    }
  }

  private <T> void updateUnionTag(
      final Reiterator<RawUnionValue> tail, final Boolean[] containsTag, final int unionTag) {
    @SuppressWarnings("unchecked")
    final Iterable<T> head = (Iterable<T>) valueMap.get(unionTag);
    valueMap.set(
        unionTag,
        () ->
            Iterators.concat(
                head.iterator(), new UnionValueIterator<T>(unionTag, tail.copy(), containsTag)));
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
    public boolean equals(Object object) {
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
   * Lazily filters and recasts an {@code Iterator<RawUnionValue>} into an {@code Iterator<V>},
   * where V is the type of the raw union value's contents.
   */
  private static class UnionValueIterator<V> implements Iterator<V> {

    private final int tag;
    private final PeekingIterator<RawUnionValue> unions;
    private final Boolean[] containsTag;

    private UnionValueIterator(int tag, Iterator<RawUnionValue> unions, Boolean[] containsTag) {
      this.tag = tag;
      this.unions = Iterators.peekingIterator(unions);
      this.containsTag = containsTag;
    }

    @Override
    public boolean hasNext() {
      if (Boolean.FALSE.equals(containsTag[tag])) {
        return false;
      }
      advance();
      if (unions.hasNext()) {
        return true;
      } else {
        // Now that we've iterated over all the values, we can resolve all the "unknown" null
        // values to false.
        for (int i = 0; i < containsTag.length; i++) {
          if (containsTag[i] == null) {
            containsTag[i] = false;
          }
        }
        return false;
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public V next() {
      advance();
      return (V) unions.next().getValue();
    }

    private void advance() {
      while (unions.hasNext()) {
        int curTag = unions.peek().getUnionTag();
        containsTag[curTag] = true;
        if (curTag == tag) {
          break;
        }
        unions.next();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
