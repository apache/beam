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
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.annotations.Experimental.Kind;

/**
 * A {@code StateContents} is produced by the read methods on all {@link State} objects.
 * Calling {@link #read} returns the associated value.
 *
 * <p>This class is similar to {@link java.util.concurrent.Future}, but each invocation of
 * {@link #read} need not return the same value.
 *
 * <p>Getting the {@code StateContents} from a read method indicates the desire to eventually
 * read a value. Depending on the runner this may or may not immediately start the read.
 *
 * @param <T> The type of value returned by {@link #read}.
 */
@Experimental(Kind.STATE)
public interface ReadableState<T> {
  /**
   * Read the current value, blocking until it is available.
   *
   * <p>If there will be many calls to {@link #read} for different state in short succession,
   * you should first call {@link #readLater} for all of them so the reads can potentially be
   * batched (depending on the underlying {@link StateInternals} implementation}.
   */
  T read();

  /**
   * Indicate that the value will be read later.
   *
   * <p>This allows a {@link StateInternals} implementation to start an asynchronous prefetch or
   * to include this state in the next batch of reads.
   *
   * @return this for convenient chaining
   */
  ReadableState<T> readLater();
}
