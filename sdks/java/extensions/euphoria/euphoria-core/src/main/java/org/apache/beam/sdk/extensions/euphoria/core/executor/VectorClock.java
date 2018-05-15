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
package org.apache.beam.sdk.extensions.euphoria.core.executor;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/** Vector clock implementation for local executor. */
@Audience(Audience.Type.EXECUTOR)
public class VectorClock implements Serializable {

  final AtomicLong[] current;

  /**
   * Create vector clock with given dimensions.
   *
   * @param dimensions number of dimensions
   */
  public VectorClock(int dimensions) {
    current = new AtomicLong[dimensions];
    for (int i = 0; i < dimensions; i++) {
      current[i] = new AtomicLong();
    }
  }

  /**
   * Retrieve current stamp as indicated by this clock.
   *
   * @return current stamp
   */
  public long getCurrent() {
    return min(current);
  }

  /**
   * Update clock in given dimension.
   *
   * @param stamp the timestamp
   * @param dimension index of dimension
   */
  public void update(long stamp, int dimension) {
    current[dimension].accumulateAndGet(stamp, (old, update) -> old < update ? update : old);
  }

  private long min(AtomicLong[] arr) {
    long min = Long.MAX_VALUE;
    for (AtomicLong l : arr) {
      final long i = l.get();
      if (i < min) {
        min = i;
      }
    }
    return min;
  }
}
