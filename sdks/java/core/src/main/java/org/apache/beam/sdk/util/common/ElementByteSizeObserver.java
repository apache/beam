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
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An observer that gets notified when additional bytes are read and/or used.
 *
 * <p>Instances are not thread-safe; callers sharing an instance across threads must synchronize
 * externally.
 *
 * <p>All updates funnel through {@link #update(long)}, which adds directly to the accumulated size
 * (using exact {@code long} arithmetic when {@code scalingFactor == 1.0}). Subclasses should
 * override {@link #reportElementSize(long)} rather than overriding {@link #update(Observable,
 * Object)} or {@link #update(Object)} alone, since primitive callers and {@link
 * ElementByteSizeObservableIterator#notifyValueReturned(long)} dispatch directly to {@link
 * #update(long)} without invoking {@link #update(Observable, Object)}. Note that per JLS §15.12.2,
 * recompiled callers passing primitive integer values ({@code byte}, {@code short}, {@code char},
 * {@code int}, {@code long}) widen directly to {@link #update(long)} without boxing.
 */
@Internal
@NotThreadSafe
public abstract class ElementByteSizeObserver extends PrimitiveByteSizeObserver {
  private boolean isLazy = false;
  private long totalSize = 0;
  private double scalingFactor = 1.0;

  public ElementByteSizeObserver() {}

  /**
   * Called to report element byte size.
   *
   * @param elementByteSize byte size of the element
   */
  protected abstract void reportElementSize(long elementByteSize);

  /**
   * Sets byte counting for the current element as lazy. That is, the observer will get notified of
   * the element's byte count only as element's pieces are being processed or iterated over.
   */
  public void setLazy() {
    isLazy = true;
  }

  /**
   * Returns whether byte counting for the current element is lazy, that is, whether the observer
   * gets notified of the element's byte count only as element's pieces are being processed or
   * iterated over.
   */
  public boolean getIsLazy() {
    return isLazy;
  }

  /**
   * Updates the observer with a boxed byte size ({@link Long} or {@link Integer}), without an
   * instance of {@link Observable}. Delegates to {@link #update(Observable, Object)}.
   *
   * @param obj boxed {@link Long} or {@link Integer} size delta in bytes
   * @throws AssertionError if {@code obj} is not an instance of {@link Long} or {@link Integer}
   */
  public void update(@Nullable Object obj) {
    update(null, obj);
  }

  /**
   * Updates the observer with a primitive byte size, avoiding {@link Long} boxing.
   *
   * <p>When {@code scalingFactor == 1.0} (the default), adds {@code byteSize} directly with exact
   * {@code long} precision (no IEEE-754 {@code double} rounding above {@code 2^53}). Otherwise adds
   * {@code (long) (scalingFactor * byteSize)}.
   *
   * @param byteSize observed size delta in bytes
   */
  @Override
  public void update(long byteSize) {
    if (scalingFactor != 1.0) {
      byteSize = (long) (scalingFactor * byteSize);
    }
    totalSize += byteSize;
  }

  /** Sets a multiplier to use on observed sizes. */
  public void setScalingFactor(double scalingFactor) {
    this.scalingFactor = scalingFactor;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Accepts {@link Long} or {@link Integer} and delegates to {@link #update(long)}.
   *
   * @throws AssertionError if {@code obj} is not an instance of {@link Long} or {@link Integer}
   */
  @Override
  public void update(@Nullable Observable obs, @Nullable Object obj) {
    if (obj instanceof Long) {
      // .longValue() is required: calling update((Long) obj) binds to update(Object) per
      // JLS §15.12.2 and causes infinite recursion (StackOverflowError).
      update(((Long) obj).longValue());
    } else if (obj instanceof Integer) {
      // .longValue() is required to dispatch to update(long) rather than update(Object).
      update(((Integer) obj).longValue());
    } else {
      throw new AssertionError("unexpected parameter object");
    }
  }

  /**
   * Advances the observer to the next element. Adds the current total byte size to the counter, and
   * prepares the observer for the next element.
   */
  public void advance() {
    reportElementSize(totalSize);
    totalSize = 0;
    isLazy = false;
  }
}
