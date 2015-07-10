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

/**
 * {@code StateInternals} describes the functionality a runner needs to provide for the
 * State API to be supported.
 *
 * <p> This is a low-level API intended for use by the Dataflow SDK. It should not be
 * used directly, and is highly likely to change.
 */
public interface StateInternals  {

  /**
   * Return the state associated with {@code address} in the specified {@code namespace}.
   */
  <T extends State> T state(StateNamespace namespace, StateTag<T> address);

  /**
   * Return state that reads from all the source namespaces. Only required to ensure that
   * resultNamespace contains all the data that is added.
   *
   * <p> Merging state is potentially destructive, in that it may move information from the
   * {@code sourceNamespaces} to {@code resultNamespace}. As a result, after calling this all
   * future calls should include as their namespaces a superset of
   * {@code sourceNamespaces} and {@code resultNamespace}.
   */
  <T extends MergeableState<?, ?>> T mergedState(
      Iterable<StateNamespace> sourceNamespaces, StateNamespace resultNamespace,
      StateTag<T> address);
}
