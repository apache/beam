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

/** An observer that gets notified when additional bytes are read and/or used. */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class ElementByteSizeObserver implements Observer {
  private boolean isLazy = false;
  private long totalSize = 0;
  private double scalingFactor = 1.0;

  public ElementByteSizeObserver() {}

  /** Called to report element byte size. */
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
   * Updates the observer with the byte size of an element piece without boxing.
   *
   * <p>Subclasses that override {@link #update(Object)} or {@link #update(Observable, Object)} must
   * also override this method if they intercept all updates, as this method updates internal state
   * directly rather than delegating to those overloads.
   */
  public void update(long bytes) {
    if (scalingFactor == 1.0) {
      totalSize += bytes;
    } else {
      totalSize += (long) (scalingFactor * bytes);
    }
  }

  /** Updates the observer with a context specified, but without an instance of the Observable. */
  public void update(Object obj) {
    update(null, obj);
  }

  /** Sets a multiplier to use on observed sizes. */
  public void setScalingFactor(double scalingFactor) {
    this.scalingFactor = scalingFactor;
  }

  @Override
  public void update(Observable obs, Object obj) {
    if (obj instanceof Long) {
      update(((Long) obj).longValue());
    } else if (obj instanceof Integer) {
      update(((Integer) obj).longValue());
    } else {
      throw new AssertionError("unexpected parameter object");
    }
  }

  /**
   * Resets any accumulated byte size, lazy state, and scaling factor to {@code 1.0} without
   * reporting. Unlike {@link #advance()}, this clears the scaling factor so a reused observer does
   * not inherit a prior element's scaling factor.
   */
  public void reset() {
    totalSize = 0;
    isLazy = false;
    scalingFactor = 1.0;
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
