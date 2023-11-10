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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteSource;
import org.checkerframework.checker.nullness.qual.NonNull;

public class DeterministicCoderCacheSerializer<T> implements CacheSerializer<T> {

  private final Coder<T> deterministicCoder;

  /**
   * Instantiates a {@link DeterministicCoderCacheSerializer} checking whether {@link
   * Coder#verifyDeterministic} passes.
   */
  DeterministicCoderCacheSerializer(Coder<T> deterministicCoder) throws NonDeterministicException {
    deterministicCoder.verifyDeterministic();
    this.deterministicCoder = deterministicCoder;
  }

  /** Converts from a byte array to a user defined type {@link T}. */
  @Override
  public @NonNull T deserialize(byte[] bytes) throws UserCodeExecutionException {
    try {
      return checkStateNotNull(deterministicCoder.decode(ByteSource.wrap(bytes).openStream()));
    } catch (IOException e) {
      throw new UserCodeExecutionException(e);
    }
  }

  /** Converts to a byte array from a user defined type {@link T}. */
  @Override
  public byte[] serialize(@NonNull T t) throws UserCodeExecutionException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      deterministicCoder.encode(t, baos);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new UserCodeExecutionException(e);
    }
  }
}
