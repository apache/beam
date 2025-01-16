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
package org.apache.beam.runners.dataflow.worker.util.common;

import java.util.concurrent.Semaphore;

public final class ResizableSemaphore extends Semaphore {

  private volatile int totalPermits;

  /**
   * Constructs a Resizable Semaphore with the given initial number of permits
   *
   * @param permits the initial number of permits available. This value may be negative, in which
   *     case releases must occur before any acquires will be granted.
   */
  public ResizableSemaphore(int permits) {
    super(permits);
    totalPermits = permits;
  }

  /**
   * Sets the new total number of permits. If the new value is larger than the current value, then
   * current blocking threads may be released. If the new value is smaller than the current value,
   * then future calls to {@link Semaphore#release()} will not unblock a thread until the total
   * number of acquired permits is less than the new total number of permits.
   *
   * @param permits the new total number of permits (must be non-negative)
   * @see #getTotalPermits
   */
  public void setTotalPermits(int permits) {
    synchronized (this) {
      if (totalPermits == permits) {
        return;
      }

      if (permits < 0) {
        throw new IllegalArgumentException("Cannot resize semaphore to a negative size.");
      }

      int diff = permits - totalPermits;

      if (diff > 0) {
        // Increase the number of permits
        this.release(diff);
      } else {
        this.reducePermits(-diff);
      }
      totalPermits = permits;
    }
  }

  /**
   * Gets the total number of permits. This value is equal to the number of permits that have been
   * acquired plus the number of permits that could be acquired without blocking.
   *
   * @return total number of permits
   */
  public int getTotalPermits() {
    return totalPermits;
  }
}
