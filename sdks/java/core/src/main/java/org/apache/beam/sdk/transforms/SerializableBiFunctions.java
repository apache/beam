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

import org.checkerframework.checker.nullness.qual.Nullable;

public class SerializableBiFunctions {
  public static <T, U, R> SerializableBiFunction<T, U, T> Select1st(
      SerializableBiFunction<@Nullable T, @Nullable U, R> biFunction) {
    return (t, u) -> t;
  }

  public static <T, U, R> SerializableBiFunction<T, U, U> Select2nd(
      SerializableBiFunction<@Nullable T, @Nullable U, R> biFunction) {
    return (t, u) -> u;
  }

  public static <T, U, R> SerializableFunction<U, R> fix1st(
      SerializableBiFunction<@Nullable T, @Nullable U, R> biFunction, @Nullable T value) {
    return u -> biFunction.apply(value, u);
  }

  public static <T, U, R> SerializableFunction<T, R> fix2nd(
      SerializableBiFunction<@Nullable T, @Nullable U, R> biFunction, @Nullable U value) {
    return t -> biFunction.apply(t, value);
  }

  public static <T, U, R> SerializableBiFunction<T, U, R> ignore1st(
      SerializableFunction<@Nullable U, R> function) {
    return (t, u) -> function.apply(u);
  }

  public static <T, U, R> SerializableBiFunction<T, U, R> ignore2nd(
      SerializableFunction<@Nullable T, R> function) {
    return (t, u) -> function.apply(t);
  }
}
