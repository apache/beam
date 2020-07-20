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

// TODO: Think about making this a complete dynamic union by adding
// a schema.  Type would then be defined by the corresponding schema entry.

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This corresponds to an integer union tag and value. The mapping of union tag to type must come
 * from elsewhere.
 */
public class RawUnionValue {
  private final int unionTag;
  private final @Nullable Object value;

  /** Constructs a partial union from the given union tag and value. */
  public RawUnionValue(int unionTag, @Nullable Object value) {
    this.unionTag = unionTag;
    this.value = value;
  }

  public int getUnionTag() {
    return unionTag;
  }

  public @Nullable Object getValue() {
    return value;
  }

  @Override
  public String toString() {
    return unionTag + ":" + value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RawUnionValue that = (RawUnionValue) o;

    if (unionTag != that.unionTag) {
      return false;
    }
    return value != null ? value.equals(that.value) : that.value == null;
  }

  @Override
  public int hashCode() {
    int result = unionTag;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
