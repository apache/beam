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

import com.google.cloud.dataflow.sdk.coders.Coder;

/**
 * A {@link TupleTag} combined with the {@link Coder} to use for
 * values associated with the tag.
 *
 * <p> Used as tags in
 * {@link com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState}.
 *
 * @param <T> the type of the values associated with this tag
 */
@SuppressWarnings("serial")
public class CodedTupleTag<T> extends TupleTag<T> {
  /**
   * Returns a {@code CodedTupleTag} with the given id which uses the
   * given {@code Coder} whenever a value associated with the tag
   * needs to be serialized.
   *
   * <p> It is up to the user to ensure that two
   * {@code CodedTupleTag}s with the same id actually mean the same
   * tag and carry the same generic type parameter.  Violating this
   * invariant can lead to hard-to-diagnose runtime type errors.
   *
   * <p> (An explicit id is required so that persistent keyed state
   * saved by one run of a streaming program can be reused if that
   * streaming program is upgraded to a new version.)
   *
   * @param <T> the type of the values associated with the tag
   */
  public static <T> CodedTupleTag<T> of(String id, Coder<T> coder) {
    return new CodedTupleTag<>(id, coder);
  }

  /**
   * Returns the {@code Coder} used for values associated with this tag.
   */
  public Coder<T> getCoder() {
    return coder;
  }


  ///////////////////////////////////////////////

  private final Coder<T> coder;

  CodedTupleTag(String id, Coder<T> coder) {
    super(id);
    this.coder = coder;
  }

  @Override
  public String toString() {
    return "CodedTupleTag<" + getId() + ", " + coder + ">";
  }
}
