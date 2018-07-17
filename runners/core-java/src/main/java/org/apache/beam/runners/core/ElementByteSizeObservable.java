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
package org.apache.beam.runners.core;

import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * An interface for things that allow observing the size in bytes of encoded values of type {@code
 * T}.
 *
 * @param <T> the type of the values being observed
 */
public interface ElementByteSizeObservable<T> {
  /**
   * Returns whether {@link #registerByteSizeObserver} is cheap enough to call for every element,
   * that is, if this {@code ElementByteSizeObservable} can calculate the byte size of the element
   * to be coded in roughly constant time (or lazily).
   */
  boolean isRegisterByteSizeObserverCheap(T value);

  /**
   * Notifies the {@code ElementByteSizeObserver} about the byte size of the encoded value using
   * this {@code ElementByteSizeObservable}.
   */
  void registerByteSizeObserver(T value, ElementByteSizeObserver observer) throws Exception;
}
