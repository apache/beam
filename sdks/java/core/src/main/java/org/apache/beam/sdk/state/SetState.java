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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * A {@link ReadableState} cell containing a set of elements.
 *
 * <p>Implementations of this form of state are expected to implement set operations such as {@link
 * #contains(Object)} efficiently, reading as little of the overall set as possible.
 *
 * @param <T> The type of elements in the set.
 */
@Experimental(Kind.STATE)
public interface SetState<T> extends GroupingState<T, Iterable<T>> {
  /** Returns true if this set contains the specified element. */
  ReadableState<Boolean> contains(T t);

  /**
   * Ensures a value is a member of the set, returning {@code true} if it was added and {@code
   * false} otherwise.
   *
   * <p>Elements added will not be reflected in {@code OutputT} objects returned by previous calls
   * to {@link #read}.
   */
  ReadableState<Boolean> addIfAbsent(T t);

  /**
   * Removes the specified element from this set if it is present.
   *
   * <p>Changes will not be reflected in {@code OutputT} objects returned by previous calls to
   * {@link #read}.
   */
  void remove(T t);

  @Override
  SetState<T> readLater();
}
