/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;

/**
 * A composite key for operations applied on groups.
 */
public final class CompositeKey<T0, T1> extends Pair<T0, T1> {

  public static <T0, T1> CompositeKey<T0, T1> of(T0 first, T1 second) {
    return new CompositeKey<>(first, second);
  }

  CompositeKey(T0 first, T1 second) {
    super(first, second);
  }

  @Override
  public String toString() {
    return "CompositeKey{first='" + getFirst() + "', second='" + getSecond() + "'}";
  }
}
