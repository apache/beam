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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A schema for the results of a {@link CoGroupByKey}. This maintains the full set of {@link
 * TupleTag}s for the results of a {@link CoGroupByKey} and facilitates mapping between {@link
 * TupleTag}s and {@link RawUnionValue} tags (which are used as secondary keys in the {@link
 * CoGroupByKey}).
 */
public class CoGbkResultSchema implements Serializable {

  private final TupleTagList tupleTagList;

  public static CoGbkResultSchema of(List<TupleTag<?>> tags) {
    TupleTagList tupleTags = TupleTagList.empty();
    for (TupleTag<?> tag : tags) {
      tupleTags = tupleTags.and(tag);
    }
    return new CoGbkResultSchema(tupleTags);
  }

  /** Maps TupleTags to union tags. This avoids needing to encode the tags themselves. */
  private final HashMap<TupleTag<?>, Integer> tagMap = new HashMap<>();

  /** Builds a schema from a tuple of {@code TupleTag<?>}s. */
  public CoGbkResultSchema(TupleTagList tupleTagList) {
    this.tupleTagList = tupleTagList;
    int index = -1;
    for (TupleTag<?> tag : tupleTagList.getAll()) {
      index++;
      tagMap.put(tag, index);
    }
  }

  /**
   * Returns the index for the given tuple tag, if the tag is present in this schema, -1 if it
   * isn't.
   */
  public int getIndex(TupleTag<?> tag) {
    Integer index = tagMap.get(tag);
    return index == null ? -1 : index;
  }

  /** Returns the tuple tag at the given index. */
  public TupleTag<?> getTag(int index) {
    return tupleTagList.get(index);
  }

  /** Returns the number of columns for this schema. */
  public int size() {
    return tupleTagList.getAll().size();
  }

  /** Returns the TupleTagList tuple associated with this schema. */
  public TupleTagList getTupleTagList() {
    return tupleTagList;
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof CoGbkResultSchema)) {
      return false;
    }
    CoGbkResultSchema other = (CoGbkResultSchema) obj;
    return tupleTagList.getAll().equals(other.tupleTagList.getAll());
  }

  @Override
  public int hashCode() {
    return tupleTagList.getAll().hashCode();
  }

  @Override
  public String toString() {
    return "CoGbkResultSchema: " + tupleTagList.getAll();
  }
}
