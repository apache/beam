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

import org.apache.beam.sdk.annotations.Internal;

/**
 * Internal marker implemented by pure functional wrapper {@link DoFn}s (such as stateless {@link
 * MapElements} and {@link Filter}) so runners can invoke or code-generate the underlying {@link
 * ProcessFunction} directly without intermediate {@link DoFn.OutputReceiver} dispatch frames.
 */
@Internal
public interface InlineableFunctionProvider<InputT, OutputT> {
  /** Functional operation kind. */
  enum Kind {
    MAP,
    FILTER
  }

  /** Returns the kind of pure function wrapped by this {@link DoFn}. */
  Kind kind();

  /**
   * Returns the underlying function: {@code ProcessFunction<InputT, OutputT>} for {@link Kind#MAP},
   * or {@code ProcessFunction<InputT, Boolean>} for {@link Kind#FILTER}.
   */
  ProcessFunction<InputT, ?> function();
}
