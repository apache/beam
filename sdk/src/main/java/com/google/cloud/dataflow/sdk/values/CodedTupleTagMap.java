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

package com.google.cloud.dataflow.sdk.values;

import java.util.Map;

/**
 * A mapping of {@link CodedTupleTag}s to associated values.
 *
 * <p> Returned by
 * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState#lookup(java.util.List)}.
 */
public class CodedTupleTagMap {
  /**
   * Returns a {@code CodedTupleTagMap} containing the given mappings.
   *
   * <p> It is up to the caller to ensure that the value associated
   * with each CodedTupleTag in the map has the static type specified
   * by that tag.
   *
   * <p> Intended for internal use only.
   */
  public static CodedTupleTagMap of(Map<CodedTupleTag<?>, Object> map) {
    // TODO: Should we copy the Map here, to insulate this
    // map from any changes to the original argument?
    return new CodedTupleTagMap(map);
  }

  /**
   * Returns the value associated with the given tag in this
   * {@code CodedTupleTagMap}, or {@code null} if the tag has no
   * asssociated value.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(CodedTupleTag<T> tag) {
    return (T) map.get(tag);
  }

  //////////////////////////////////////////////

  private Map<CodedTupleTag<?>, Object> map;

  CodedTupleTagMap(Map<CodedTupleTag<?>, Object> map) {
    this.map = map;
  }
}
