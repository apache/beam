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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A thread-safe {@link SerializableSupplier} that wraps a {@link SerializableSupplier} and retains
 * the supplier's result for the provided period. Lightweight locking and synchronization is used to
 * guarantee mutual exclusivity and visibility of updates at the expense of single nanosecond
 * precision.
 *
 * <p>The initial value and subsequently retained values are considered transient and will not be
 * serialized.
 */
public final class ExpiringMemoizingSerializableSupplier<T extends @Nullable Object>
    implements SerializableSupplier<T> {
  // TODO(sjvanrossum): Replace with VarHandle after JDK 8 support is dropped.
  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<ExpiringMemoizingSerializableSupplier>
      DEADLINE_NANOS =
          AtomicLongFieldUpdater.newUpdater(
              ExpiringMemoizingSerializableSupplier.class, "deadlineNanos");

  private final SerializableSupplier<T> supplier;
  private final long periodNanos;
  private transient T value;
  private transient volatile long deadlineNanos;

  public ExpiringMemoizingSerializableSupplier(
      SerializableSupplier<T> supplier, Duration period, T initialValue, Duration initialDelay) {
    this.supplier = supplier; // final store
    this.periodNanos = period.toNanos(); // final store
    this.value = initialValue; // normal store

    // Ordered stores may be reordered with subsequent loads.
    // The default value of deadlineNanos permits an indefinite initial expiration depending on the
    // clock's state.
    this.deadlineNanos =
        System.nanoTime() + initialDelay.toNanos()
            & ~1L; // volatile store (sequentially consistent release)
  }

  @Override
  public T get() {
    final long deadlineNanos = this.deadlineNanos; // volatile load (acquire)
    final long nowNanos;
    final T result;

    /*
     * Sacrificing 1ns precision to pack the lock state into the low bit of deadlineNanos is deemed acceptable.
     * Subsequent loads and stores are prevented from reordering before a volatile load.
     * Preceeding loads and stores are prevented from reordering after an ordered store.
     * A store to value can't be reordered after a store to deadlineNanos
     * A store to deadlineNanos can be reordered after a load of deadlineNanos.
     * The returned value will be as old as or younger than deadlineNanos.
     */
    if ((deadlineNanos & 1L) == 0
        && deadlineNanos - (nowNanos = System.nanoTime()) <= 0L
        && DEADLINE_NANOS
            .compareAndSet( // volatile load/store (sequentially consistent acquire/release)
                this, deadlineNanos, deadlineNanos | 1L)) {
      try {
        this.value = result = supplier.get(); // normal store
      } finally {
        DEADLINE_NANOS.lazySet(this, (nowNanos + periodNanos) & ~1L); // ordered store (release)
      }
    } else {
      result = this.value; // normal load
    }

    return result;
  }

  private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
    is.defaultReadObject();

    // Immediate initial expiration prevents a load of value before it is initialized.
    this.deadlineNanos =
        System.nanoTime() & ~1L; // volatile store (sequentially consistent release)
  }
}
