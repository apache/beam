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
package org.apache.beam.sdk.util.state;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.GroupByKey;

/**
 * {@code StateInternals} describes the functionality a runner needs to provide for the
 * State API to be supported.
 *
 * <p>The SDK will only use this after elements have been partitioned by key. For instance, after a
 * {@link GroupByKey} operation. The runner implementation must ensure that any writes using
 * {@link StateInternals} are implicitly scoped to the key being processed and the specific step
 * accessing state.
 *
 * <p>The runner implementation must also ensure that any writes to the associated state objects
 * are persisted together with the completion status of the processing that produced these
 * writes.
 *
 * <p>This is a low-level API intended for use by the Beam SDK. It should not be
 * used directly, and is highly likely to change.
 */
@Experimental(Kind.STATE)
public interface StateInternals<K> {

  /** The key for this {@link StateInternals}. */
  K getKey();

  /**
   * Return the state associated with {@code address} in the specified {@code namespace}.
   */
  <T extends State> T state(StateNamespace namespace, StateTag<? super K, T> address);

  /**
   * Return the state associated with {@code address} in the specified {@code namespace}
   * with the {@link StateContext}.
   */
  <T extends State> T state(
      StateNamespace namespace, StateTag<? super K, T> address, StateContext<?> c);
}
