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

import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/** <b><i>For internal use only; no backwards-compatibility guarantees.</i></b> */
@Internal
public class ReadableStates {

  /** A {@link ReadableState} constructed from a constant value, hence immediately available. */
  public static <T> ReadableState<T> immediate(final @Nullable T value) {
    return new ReadableState<T>() {
      @Override
      public @Nullable T read() {
        return value;
      }

      @Override
      public ReadableState<T> readLater() {
        return this;
      }
    };
  }
}
