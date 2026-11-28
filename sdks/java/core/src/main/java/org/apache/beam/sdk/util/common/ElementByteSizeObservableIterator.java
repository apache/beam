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

import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstract class used for iterators that notify observers about size in bytes of their elements,
 * as they are being iterated over. The subclasses need to implement the standard {@link Iterator}
 * interface and call {@link #notifyValueReturned(long)} for each element read and/or iterated over.
 *
 * <p><b>Thread-safety and {@link Observable} bridge:</b> Unlike {@link Observable}, this class is
 * <em>not</em> thread-safe; callers that share an instance across threads must synchronize
 * externally. It retains {@code extends Observable} strictly for binary/source compatibility
 * (calling {@code super()} allocates an unused backing {@link java.util.Vector} inside {@code
 * Observable}). All 9 {@link Observable} methods are overridden without synchronization to avoid
 * uncontended monitor lock overhead on hot iteration paths. Subclasses must <em>not</em> invoke
 * {@code super.addObserver}, {@code super.deleteObserver}, {@code super.deleteObservers}, {@code
 * super.setChanged}, {@code super.clearChanged}, {@code super.hasChanged}, {@code
 * super.countObservers}, or {@code super.notifyObservers}, as superclass {@link Observable} state
 * is completely bypassed. Subclasses overriding any of the 7 formerly-synchronized {@link
 * Observable} methods without {@code synchronized} will need
 * {@code @SuppressWarnings("UnsynchronizedOverridesSynchronized")} under ErrorProne.
 *
 * @param <V> value type
 */
@Internal
@NotThreadSafe
public abstract class ElementByteSizeObservableIterator<V> extends Observable
    implements Iterator<V> {
  // Mutually exclusive storage: at most one of singleObserver (1 observer, 0 extra allocation) or
  // multipleObservers (immutable copy-on-write Observer[] snapshot of length >= 2, ordered by
  // registration) is non-null. Existing arrays are never mutated in place, giving lock-free
  // snapshot isolation during notification with zero per-element snapshot allocation.
  private @Nullable Observer singleObserver;
  private Observer @Nullable [] multipleObservers;
  private boolean changed;

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  public void addObserver(Observer o) {
    if (o == null) {
      throw new NullPointerException();
    }
    Observer @Nullable [] multi = this.multipleObservers;
    if (multi != null) {
      for (int i = 0; i < multi.length; i++) {
        if (o == multi[i] || o.equals(multi[i])) {
          return;
        }
      }
      Observer[] next = new Observer[multi.length + 1];
      System.arraycopy(multi, 0, next, 0, multi.length);
      next[multi.length] = o;
      this.multipleObservers = next;
      return;
    }
    @Nullable Observer single = this.singleObserver;
    if (single == null) {
      this.singleObserver = o;
    } else if (o != single && !o.equals(single)) {
      this.multipleObservers = new Observer[] {single, o};
      this.singleObserver = null;
    }
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  public void deleteObserver(@Nullable Observer o) {
    if (o == null) {
      return;
    }
    @Nullable Observer single = this.singleObserver;
    if (single != null) {
      if (o == single || o.equals(single)) {
        this.singleObserver = null;
      }
      return;
    }
    Observer @Nullable [] arr = this.multipleObservers;
    if (arr == null) {
      return;
    }
    int len = arr.length;
    for (int i = 0; i < len; i++) {
      if (o == arr[i] || o.equals(arr[i])) {
        if (len == 2) {
          this.singleObserver = arr[i == 0 ? 1 : 0];
          this.multipleObservers = null;
        } else {
          Observer[] next = new Observer[len - 1];
          System.arraycopy(arr, 0, next, 0, i);
          System.arraycopy(arr, i + 1, next, i, len - i - 1);
          this.multipleObservers = next;
        }
        return;
      }
    }
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  public void deleteObservers() {
    singleObserver = null;
    multipleObservers = null;
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  public int countObservers() {
    if (singleObserver != null) {
      return 1;
    }
    Observer @Nullable [] multi = this.multipleObservers;
    return multi == null ? 0 : multi.length;
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  protected void setChanged() {
    changed = true;
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  protected void clearChanged() {
    changed = false;
  }

  @Override
  @SuppressWarnings("UnsynchronizedOverridesSynchronized")
  public boolean hasChanged() {
    return changed;
  }

  @Override
  public void notifyObservers() {
    notifyObservers(null);
  }

  @Override
  public void notifyObservers(@Nullable Object arg) {
    if (!changed) {
      return;
    }
    @Nullable Observer single = this.singleObserver;
    Observer @Nullable [] multi = this.multipleObservers;
    clearChanged();
    if (single != null) {
      invokeUpdate(single, arg);
      return;
    }
    if (multi != null) {
      for (int i = multi.length - 1; i >= 0; i--) {
        invokeUpdate(multi[i], arg);
      }
    }
  }

  // java.util.Observer.update allows a nullable arg per the java.util.Observable contract.
  @SuppressWarnings("nullness:argument")
  private void invokeUpdate(Observer observer, @Nullable Object arg) {
    observer.update(this, arg);
  }

  /**
   * Notifies all registered observers (in reverse registration order) that an element of size
   * {@code byteSize} bytes has been returned.
   *
   * <p>Unlike {@link Observable#notifyObservers(Object)}, this method bypasses {@link
   * #setChanged()} and {@link #notifyObservers(Object)} for all observer types (including plain
   * {@link Observer}s and the zero-observer case), and invokes {@link #clearChanged()} before
   * callbacks only when {@link #hasChanged()} is currently {@code true}. Observers extending {@link
   * PrimitiveByteSizeObserver} (including {@link ElementByteSizeObserver} and {@code
   * IterableLikeCoder.IteratorObserver}) are notified directly via {@link
   * PrimitiveByteSizeObserver#update(long)} across both single- and multi-observer registrations,
   * avoiding {@link Long} boxing; plain {@link Observer} instances receive {@code update(this,
   * Long.valueOf(byteSize))}.
   *
   * @param byteSize element size in bytes
   */
  protected final void notifyValueReturned(long byteSize) {
    @Nullable Observer single = this.singleObserver;
    Observer @Nullable [] multi = this.multipleObservers;
    if (changed) {
      clearChanged();
    }
    if (single != null) {
      if (single instanceof ElementByteSizeObserver) {
        ((ElementByteSizeObserver) single).update(byteSize);
      } else if (single instanceof PrimitiveByteSizeObserver) {
        ((PrimitiveByteSizeObserver) single).update(byteSize);
      } else {
        single.update(this, byteSize);
      }
    } else if (multi != null) {
      notifyMultipleValueReturned(multi, byteSize);
    }
  }

  private void notifyMultipleValueReturned(Observer[] snapshot, long byteSize) {
    @Nullable Long boxed = null;
    for (int i = snapshot.length - 1; i >= 0; i--) {
      Observer observer = snapshot[i];
      if (observer instanceof ElementByteSizeObserver) {
        ((ElementByteSizeObserver) observer).update(byteSize);
      } else if (observer instanceof PrimitiveByteSizeObserver) {
        ((PrimitiveByteSizeObserver) observer).update(byteSize);
      } else {
        if (boxed == null) {
          boxed = byteSize;
        }
        observer.update(this, boxed);
      }
    }
  }
}
