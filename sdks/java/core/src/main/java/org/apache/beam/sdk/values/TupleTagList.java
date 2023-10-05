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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * A {@link TupleTagList} is an immutable list of heterogeneously typed {@link TupleTag TupleTags}.
 * A {@link TupleTagList} is used, for instance, to specify the tags of the additional outputs of a
 * {@link ParDo}.
 *
 * <p>A {@link TupleTagList} can be created and accessed like follows:
 *
 * <pre>{@code
 * TupleTag<String> tag1 = ...;
 * TupleTag<Integer> tag2 = ...;
 * TupleTag<Iterable<String>> tag3 = ...;
 *
 * // Create a TupleTagList with three TupleTags:
 * TupleTagList tags = TupleTagList.of(tag1).and(tag2).and(tag3);
 *
 * // Create an empty TupleTagList:
 * Pipeline p = ...;
 * TupleTagList tags2 = TupleTagList.empty(p);
 *
 * // Get TupleTags out of a TupleTagList, by index (origin 0):
 * TupleTag<?> tagX = tags.get(1);
 * TupleTag<?> tagY = tags.get(0);
 * TupleTag<?> tagZ = tags.get(2);
 *
 * // Get a list of all TupleTags in a TupleTagList:
 * List<TupleTag<?>> allTags = tags.getAll();
 * }</pre>
 */
public class TupleTagList implements Serializable {
  /**
   * Returns an empty {@link TupleTagList}.
   *
   * <p>Longer {@link TupleTagList TupleTagLists} can be created by calling {@link #and} on the
   * result.
   */
  public static TupleTagList empty() {
    return new TupleTagList();
  }

  /**
   * Returns a singleton {@link TupleTagList} containing the given {@link TupleTag}.
   *
   * <p>Longer {@link TupleTagList TupleTagLists} can be created by calling {@link #and} on the
   * result.
   */
  public static TupleTagList of(TupleTag<?> tag) {
    return empty().and(tag);
  }

  /**
   * Returns a {@link TupleTagList} containing the given {@link TupleTag TupleTags}, in order.
   *
   * <p>Longer {@link TupleTagList TupleTagLists} can be created by calling {@link #and} on the
   * result.
   */
  public static TupleTagList of(List<TupleTag<?>> tags) {
    return empty().and(tags);
  }

  /**
   * Returns a new {@link TupleTagList} that has all the {@link TupleTag TupleTags} of this {@link
   * TupleTagList} plus the given {@link TupleTag} appended to the end.
   */
  public TupleTagList and(TupleTag<?> tag) {
    return new TupleTagList(
        new ImmutableList.Builder<TupleTag<?>>().addAll(tupleTags).add(tag).build());
  }

  /**
   * Returns a new {@link TupleTagList} that has all the {@link TupleTag TupleTags} of this {@link
   * TupleTagList} plus the given {@link TupleTag TupleTags} appended to the end, in order.
   */
  public TupleTagList and(List<TupleTag<?>> tags) {
    return new TupleTagList(
        new ImmutableList.Builder<TupleTag<?>>().addAll(tupleTags).addAll(tags).build());
  }

  /** Returns the number of TupleTags in this TupleTagList. */
  public int size() {
    return tupleTags.size();
  }

  /**
   * Returns the {@link TupleTag} at the given index (origin zero).
   *
   * @throws IndexOutOfBoundsException if the index is out of the range {@code [0..size()-1]}.
   */
  public TupleTag<?> get(int index) {
    return tupleTags.get(index);
  }

  /**
   * Returns an immutable List of all the {@link TupleTag TupleTags} in this {@link TupleTagList}.
   */
  public List<TupleTag<?>> getAll() {
    return tupleTags;
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  final List<TupleTag<?>> tupleTags;

  TupleTagList() {
    this(new ArrayList<>());
  }

  TupleTagList(List<TupleTag<?>> tupleTags) {
    this.tupleTags = Collections.unmodifiableList(tupleTags);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(TupleTagList.class).add("tupleTags", tupleTags).toString();
  }
}
