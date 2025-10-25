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

/** Useful {@link SerializableFunction} overrides. */
public class SerializableBiFunctions {
  /** Always returns the first argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableBiFunction<FirstInputT, SecondInputT, FirstInputT> select1st(
          SerializableBiFunction<@Nullable FirstInputT, @Nullable SecondInputT, OutputT>
              biFunction) {
    return (t, u) -> t;
  }

  /** Always returns the second argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableBiFunction<FirstInputT, SecondInputT, SecondInputT> select2nd(
          SerializableBiFunction<@Nullable FirstInputT, @Nullable SecondInputT, OutputT>
              biFunction) {
    return (t, u) -> u;
  }

  /** Convert to a unary function by fixing the first argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableFunction<SecondInputT, OutputT> fix1st(
          SerializableBiFunction<@Nullable FirstInputT, @Nullable SecondInputT, OutputT> biFunction,
          @Nullable FirstInputT value) {
    return u -> biFunction.apply(value, u);
  }

  /** Convert to a unary function by fixing the second argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableFunction<FirstInputT, OutputT> fix2nd(
          SerializableBiFunction<@Nullable FirstInputT, @Nullable SecondInputT, OutputT> biFunction,
          @Nullable SecondInputT value) {
    return t -> biFunction.apply(t, value);
  }

  /** Convert from a unary function by ignoring the first argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableBiFunction<FirstInputT, SecondInputT, OutputT> ignore1st(
          SerializableFunction<@Nullable SecondInputT, OutputT> function) {
    return (t, u) -> function.apply(u);
  }

  /** Convert from a unary function by ignoring the second argument. */
  public static <FirstInputT, SecondInputT, OutputT>
      SerializableBiFunction<FirstInputT, SecondInputT, OutputT> ignore2nd(
          SerializableFunction<@Nullable FirstInputT, OutputT> function) {
    return (t, u) -> function.apply(t);
  }
}
