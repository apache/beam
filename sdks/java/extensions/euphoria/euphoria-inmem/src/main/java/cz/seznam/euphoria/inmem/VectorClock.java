/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.inmem;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vector clock implementation for inmem.
 */
class VectorClock {

  final AtomicLong[] current;

  VectorClock(int dimensions) {
    current = new AtomicLong[dimensions];
    for (int i = 0; i < dimensions; i++) {
      current[i] = new AtomicLong();
    }
  }

  long getCurrent() {
    return _min(current);
  }

  void update(long stamp, int dimension) {
    current[dimension].accumulateAndGet(
        stamp, (old, update) -> old < update ? update : old);
  }

  private long _min(AtomicLong[] arr) {
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
