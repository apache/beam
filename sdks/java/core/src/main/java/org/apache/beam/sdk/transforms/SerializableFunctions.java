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
import org.apache.beam.sdk.util.SerializableUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Useful {@link SerializableFunction} overrides. */
public class SerializableFunctions {
  public static <InT, OutT extends Serializable> SerializableFunction<InT, OutT> clonesOf(
      final OutT base) {
    return input -> SerializableUtils.clone(base);
  }

  private static class Identity<T> implements SerializableFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  private static class Constant<InT, OutT> implements SerializableFunction<InT, OutT> {
    @Nullable OutT value;

    Constant(@Nullable OutT value) {
      this.value = value;
    }

    @Override
    public OutT apply(InT input) {
      return value;
    }
  }

  public static <T> SerializableFunction<T, T> identity() {
    return new Identity<>();
  }

  public static <InT, OutT> SerializableFunction<InT, OutT> constant(@Nullable OutT value) {
    return new Constant<>(value);
  }
}
