
package cz.seznam.euphoria.core.executor.inmem;

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
