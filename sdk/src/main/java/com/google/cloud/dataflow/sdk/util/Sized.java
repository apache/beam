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
 * A {@code T} with an accompanying size estimate. Units are unspecified.
 *
 * @param <T> the underlying type of object
 */
public final class Sized<T> {

  private final T value;
  private final long size;

  private Sized(T value, long size) {
    this.value = value;
    this.size = size;
  }

  public static <T> Sized<T> of(T value, long size) {
    return new Sized<>(value, size);
  }

  public long getSize() {
    return size;
  }

  public T getValue() {
    return value;
  }
}
