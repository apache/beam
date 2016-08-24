
package cz.seznam.euphoria.core.executor.inmem;

/**
 * Vector clock implementation for inmem.
 */
class VectorClock {

  final long[] current;

  VectorClock(int dimensions) {
    current = new long[dimensions];
  }

  long getCurrent() {
    return _min(current);
  }

  void update(long stamp, int dimension) {
    current[dimension] = stamp;
  }

  private long _min(long[] arr) {
    long min = Long.MAX_VALUE;
    for (long l : arr) {
      if (l < min) {
        min = l;
      }
    }
    return min;
  }

}
