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
package org.apache.beam.sdk.io.aws2.kinesis;

import java.util.NoSuchElementException;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Similar to Guava {@code Optional}, but throws {@link NoSuchElementException} for missing element.
 */
abstract class CustomOptional<T> {

  @SuppressWarnings("unchecked")
  public static <T> CustomOptional<T> absent() {
    return (Absent<T>) Absent.INSTANCE;
  }

  public static <T> CustomOptional<T> of(T v) {
    return new Present<>(v);
  }

  public abstract boolean isPresent();

  public abstract T get();

  private static class Present<T> extends CustomOptional<T> {

    private final T value;

    private Present(T value) {
      this.value = value;
    }

    @Override
    public boolean isPresent() {
      return true;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (!(o instanceof Present)) {
        return false;
      }

      Present<?> present = (Present<?>) o;
      return Objects.equals(value, present.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value);
    }
  }

  private static class Absent<T> extends CustomOptional<T> {

    private static final Absent<Object> INSTANCE = new Absent<>();

    private Absent() {}

    @Override
    public boolean isPresent() {
      return false;
    }

    @Override
    public T get() {
      throw new NoSuchElementException();
    }

    @Override
    public boolean equals(@Nullable Object o) {
      return o instanceof Absent;
    }

    @Override
    public int hashCode() {
      return 0;
    }
  }
}
