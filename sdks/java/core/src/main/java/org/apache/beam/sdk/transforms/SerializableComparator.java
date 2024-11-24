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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@code Comparator} that is also {@code Serializable}.
 *
 * @param <T> type of values being compared
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {
  /**
   * Analogous to {@link Comparator#comparing(Function)}, except that it takes in a {@link
   * SerializableFunction} as the key extractor and returns a {@link SerializableComparator}.
   *
   * @param keyExtractor the function used to extract the {@link java.lang.Comparable} sort key
   * @return A {@link SerializableComparator} that compares by an extracted key
   * @param <T> the type of element to be compared
   * @param <V> the type of the {@code Comparable} sort key
   * @see Comparator#comparing(Function)
   */
  static <T, V extends Comparable<? super V>> SerializableComparator<T> comparing(
      SerializableFunction<? super T, ? extends V> keyExtractor) {
    Objects.requireNonNull(keyExtractor);
    return (SerializableComparator<T>)
        (c1, c2) -> keyExtractor.apply(c1).compareTo(keyExtractor.apply(c2));
  }
}
