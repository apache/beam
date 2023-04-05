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
package org.apache.beam.sdk.util.common;

import java.util.Iterator;
import org.apache.beam.sdk.annotations.Internal;

/**
 * An {@link Iterator} with the ability to copy its iteration state.
 *
 * @param <T> the type of elements returned by this iterator
 */
@Internal
public interface Reiterator<T> extends Iterator<T> {
  /**
   * Returns a copy of the current {@link Reiterator}. The copy's iteration state is logically
   * independent of the current iterator; each may be advanced without affecting the other.
   *
   * <p>The returned {@code Reiterator} is not guaranteed to return referentially identical
   * iteration results as the original {@link Reiterator}, although {@link Object#equals} will
   * typically return true for the corresponding elements of each if the original source is
   * logically immutable.
   */
  Reiterator<T> copy();
}
