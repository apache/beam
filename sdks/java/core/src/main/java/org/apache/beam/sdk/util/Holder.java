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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Internal;

/**
 * A trivial boxing of a value, used when nullability needs to be added to a generic type. (Optional
 * does not work for this)
 *
 * <p>Example: For a generic type `T` the actual parameter may be nullable or not. So you cannot
 * check values for null to determine presence/absence. Instead you can store a {@code @Nullable
 * Holder<T>}.
 */
@Internal
public class Holder<T> {
  private T value;

  private Holder(T value) {
    this.value = value;
  }

  public static <ValueT> Holder<ValueT> of(ValueT value) {
    return new Holder<>(value);
  }

  public T get() {
    return value;
  };
}
