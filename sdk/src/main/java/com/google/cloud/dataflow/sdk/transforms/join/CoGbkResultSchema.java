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

package com.google.cloud.dataflow.sdk.transforms.join;

import static com.google.cloud.dataflow.sdk.util.Structs.addList;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A schema for the results of a CoGroupByKey.  This maintains the full
 * set of TupleTags for the results of a CoGroupByKey, and facilitates mapping
 * between TupleTags and Union Tags (which are used as secondary keys in the
 * CoGroupByKey).
 */
@SuppressWarnings("serial")
public class CoGbkResultSchema implements Serializable {

  private final TupleTagList tupleTagList;

  @JsonCreator
  public static CoGbkResultSchema of(
      @JsonProperty(PropertyNames.TUPLE_TAGS) List<TupleTag<?>> tags) {
    TupleTagList tupleTags = TupleTagList.empty();
    for (TupleTag<?> tag : tags) {
      tupleTags = tupleTags.and(tag);
    }
    return new CoGbkResultSchema(tupleTags);
  }

  /**
   * Maps TupleTags to union tags.  This avoids needing to encode the tags
   * themselves.
   */
  private final HashMap<TupleTag<?>, Integer> tagMap = new HashMap<>();

  /**
   * Builds a schema from a tuple of {@code TupleTag<?>}s.
   */
  public CoGbkResultSchema(TupleTagList tupleTagList) {
    this.tupleTagList = tupleTagList;
    int index = -1;
    for (TupleTag<?> tag : tupleTagList.getAll()) {
      index++;
      tagMap.put(tag, index);
    }
  }

  /**
   * Returns the index for the given tuple tag, if the tag is present in this
   * schema, -1 if it isn't.
   */
  public int getIndex(TupleTag<?> tag) {
    Integer index = tagMap.get(tag);
    return index == null ? -1 : index;
  }

  /**
   * Returns the JoinTupleTag at the given index.
   */
  public TupleTag<?> getTag(int index) {
    return tupleTagList.get(index);
  }

  /**
   * Returns the number of columms for this schema.
   */
  public int size() {
    return tupleTagList.getAll().size();
  }

  /**
   * Returns the TupleTagList tuple associated with this schema.
   */
  public TupleTagList getTupleTagList() {
    return tupleTagList;
  }

  public CloudObject asCloudObject() {
    CloudObject result = CloudObject.forClass(getClass());
    List<CloudObject> serializedTags = new ArrayList<>(tupleTagList.size());
    for (TupleTag<?> tag : tupleTagList.getAll()) {
      serializedTags.add(tag.asCloudObject());
    }
    addList(result, PropertyNames.TUPLE_TAGS, serializedTags);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
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
