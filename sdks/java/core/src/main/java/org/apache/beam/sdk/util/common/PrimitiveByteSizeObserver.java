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
package org.apache.beam.sdk.util.common;

import java.util.Observable;
import java.util.Observer;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Zero-field base class for {@link Observer} implementations that receive primitive {@code long}
 * byte sizes directly, avoiding {@link Long} boxing on hot {@link
 * ElementByteSizeObservableIterator#notifyValueReturned(long)} paths without requiring wrapper
 * observers to inherit {@link ElementByteSizeObserver}'s mutable state.
 *
 * <p><b>Why an abstract class rather than an interface:</b> {@link
 * ElementByteSizeObservableIterator#notifyValueReturned(long)} checks {@code instanceof
 * ElementByteSizeObserver} and {@code instanceof PrimitiveByteSizeObserver} on every element.
 * Making this an abstract class places both types in HotSpot's fixed-offset primary supertype table
 * instead of the shared single-entry secondary supertype cache ({@code JDK-8180450}), eliminating
 * cross-core cache-line contention on multi-threaded workers while remaining an {@link Observer}
 * for {@link Observable#addObserver(Observer)}.
 *
 * <p><b>Dispatch contract:</b> {@link ElementByteSizeObservableIterator#notifyValueReturned(long)}
 * invokes {@link #update(long)} exclusively (bypassing {@link Observer#update(Observable,
 * Object)}), whereas legacy callers invoking {@link Observable#notifyObservers(Object)} directly
 * invoke {@link Observer#update(Observable, Object)}.
 */
@Internal
public abstract class PrimitiveByteSizeObserver implements Observer {
  protected PrimitiveByteSizeObserver() {}

  /**
   * Updates this observer with a primitive byte-size delta.
   *
   * @param byteSize observed size delta in bytes
   */
  public abstract void update(long byteSize);
}
