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
package org.apache.beam.io.requestresponse;

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.checkerframework.checker.nullness.qual.NonNull;

/** Converts between a user defined type {@link T} and a byte array for cache storage. */
public interface CacheSerializer<@NonNull T> extends Serializable {

  /** Convert a byte array to a user defined type {@link T}. */
  @NonNull
  T deserialize(byte[] bytes) throws UserCodeExecutionException;

  /** Convert a user defined type {@link T} to a byte array. */
  byte[] serialize(@NonNull T t) throws UserCodeExecutionException;

  /**
   * Instantiates a {@link CacheSerializer} that converts between a JSON byte array and a user
   * defined type {@link T}.
   */
  static <@NonNull T> CacheSerializer<T> usingJson(Class<@NonNull T> clazz) {
    return new JsonCacheSerializer<>(clazz);
  }

  /**
   * Instantiates a {@link CacheSerializer} that converts between a byte array and a user defined
   * type {@link T}. Throws a {@link NonDeterministicException} if {@link Coder#verifyDeterministic}
   * does not pass.
   */
  static <@NonNull T> CacheSerializer<@NonNull T> usingDeterministicCoder(
      Coder<@NonNull T> deterministicCoder) throws NonDeterministicException {
    return new DeterministicCoderCacheSerializer<>(deterministicCoder);
  }
}
