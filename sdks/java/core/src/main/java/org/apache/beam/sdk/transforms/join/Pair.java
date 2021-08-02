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

import java.util.Objects;
import javax.annotation.Nullable;

/** A pair of two different objects. */
public class Pair<V1, V2> {
  private final V1 first;
  private final V2 second;

  public Pair(V1 first, V2 second) {
    this.first = first;
    this.second = second;
  }

  public static <V1, V2> Pair<V1, V2> of(V1 first, V2 second) {
    return new Pair<>(first, second);
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }

  @Override
  public String toString() {
    return "(" + first.toString() + ", " + second.toString() + ")";
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof Pair)) {
      return false;
    }
    Pair other = (Pair) object;
    return first.equals(other.getFirst()) && second.equals(other.getSecond());
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }
}
