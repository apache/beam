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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An abstract class used for iterators that notify observers about size in bytes of their elements,
 * as they are being iterated over. The subclasses need to implement the standard Iterator interface
 * and call method notifyValueReturned() for each element read and/or iterated over.
 *
 * @param <V> value type
 */
@Internal
// Iterators are single-threaded; avoid Observable's synchronized Vector overhead on hot paths.
@SuppressWarnings("UnsynchronizedOverridesSynchronized")
public abstract class ElementByteSizeObservableIterator<V> extends Observable
    implements Iterator<V> {
  private @Nullable Observer singleObserver;
  private @Nullable ArrayList<Observer> extraObservers;
  private boolean changed;

  @Override
  public void addObserver(Observer o) {
    if (o == null) {
      throw new NullPointerException();
    }
    if (singleObserver == null) {
      singleObserver = o;
      return;
    }
    if (singleObserver.equals(o)) {
      return;
    }
    ArrayList<Observer> extras = extraObservers;
    if (extras == null) {
      extras = new ArrayList<>(2);
      extraObservers = extras;
      extras.add(o);
    } else if (!extras.contains(o)) {
      extras.add(o);
    }
  }

  @Override
  public void deleteObserver(@Nullable Observer o) {
    if (singleObserver == null || o == null) {
      return;
    }
    if (singleObserver.equals(o)) {
      ArrayList<Observer> extras = extraObservers;
      if (extras == null || extras.isEmpty()) {
        singleObserver = null;
      } else {
        singleObserver = extras.remove(0);
      }
      return;
    }
    ArrayList<Observer> extras = extraObservers;
    if (extras != null) {
      extras.remove(o);
    }
  }

  @Override
  public void deleteObservers() {
    singleObserver = null;
    ArrayList<Observer> extras = extraObservers;
    if (extras != null) {
      extras.clear();
    }
  }

  @Override
  public int countObservers() {
    if (singleObserver == null) {
      return 0;
    }
    ArrayList<Observer> extras = extraObservers;
    return 1 + (extras == null ? 0 : extras.size());
  }

  @Override
  protected void setChanged() {
    changed = true;
  }

  @Override
  protected void clearChanged() {
    changed = false;
  }

  @Override
  public boolean hasChanged() {
    return changed;
  }

  @Override
  public void notifyObservers() {
    notifyObservers(null);
  }

  @Override
  @SuppressWarnings(
      "nullness") // java.util.Observer.update allows nullable arg per Observable contract
  public void notifyObservers(@Nullable Object arg) {
    if (!changed) {
      return;
    }
    changed = false;
    Observer first = singleObserver;
    if (first == null) {
      return;
    }
    ArrayList<Observer> extras = extraObservers;
    if (extras == null || extras.isEmpty()) {
      first.update(this, arg);
      return;
    }
    int extraSize = extras.size();
    Observer[] snapshot = new Observer[1 + extraSize];
    snapshot[0] = first;
    for (int i = 0; i < extraSize; i++) {
      snapshot[i + 1] = extras.get(i);
    }
    for (int i = snapshot.length - 1; i >= 0; i--) {
      snapshot[i].update(this, arg);
    }
  }

  protected final void notifyValueReturned(long byteSize) {
    Observer obs = singleObserver;
    if (obs == null) {
      changed = false;
      return;
    }
    ArrayList<Observer> extras = extraObservers;
    if (extras == null || extras.isEmpty()) {
      changed = false;
      if (obs instanceof ElementByteSizeObserver) {
        ((ElementByteSizeObserver) obs).update(byteSize);
      } else {
        obs.update(this, byteSize);
      }
      return;
    }
    setChanged();
    notifyObservers(byteSize);
  }
}
