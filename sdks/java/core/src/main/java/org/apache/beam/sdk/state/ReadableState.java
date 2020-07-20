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
package org.apache.beam.sdk.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link State} that can be read via {@link #read()}.
 *
 * <p>Use {@link #readLater()} for marking several states for prefetching. Runners can potentially
 * batch these into one read.
 *
 * @param <T> The type of value returned by {@link #read}.
 */
@Experimental(Kind.STATE)
public interface ReadableState<T> {
  /**
   * Read the current value, blocking until it is available.
   *
   * <p>If there will be many calls to {@link #read} for different state in short succession, you
   * should first call {@link #readLater} for all of them so the reads can potentially be batched
   * (depending on the underlying implementation}.
   *
   * <p>The returned object should be independent of the underlying state. Any direct modification
   * of the returned object should not modify state without going through the appropriate state
   * interface, and modification to the state should not be mirrored in the returned object.
   */
  @Nullable
  T read();

  /**
   * Indicate that the value will be read later.
   *
   * <p>This allows an implementation to start an asynchronous prefetch or to include this state in
   * the next batch of reads.
   *
   * @return this for convenient chaining
   */
  ReadableState<T> readLater();
}
