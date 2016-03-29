/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

/**
 * A {@code T} with an accompanying weight. Units are unspecified.
 *
 * @param <T> the underlying type of object
 */
public final class WeightedValue<T> implements Weighted {

  private final T value;
  private final long weight;

  private WeightedValue(T value, long weight) {
    this.value = value;
    this.weight = weight;
  }

  public static <T> WeightedValue<T> of(T value, long weight) {
    return new WeightedValue<>(value, weight);
  }

  public long getWeight() {
    return weight;
  }

  public T getValue() {
    return value;
  }
}
