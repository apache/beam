package cz.seznam.euphoria.core.executor.greduce;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

import java.util.HashSet;
import java.util.PriorityQueue;

/**
 * Keeps track of the current watermark within a reduce operation and provides
 * services around timers. This implementation assumes the reduce operation is
 * performed on ascending data for one and the same key.
 */
class TimerSupport<W extends Window> {

  static final class Timer<W> implements Comparable<Timer<W>> {
    final W window;
    final long time;

    Timer(W window, long time) {
      this.window = window;
      this.time = time;
    }

    public W getWindow() {
      return window;
    }

    public long getTime() {
      return time;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Timer) {
        Timer that = (Timer) o;
        return time == that.time
            && (window != null ? window.equals(that.window) : that.window == null);
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = window != null ? window.hashCode() : 0;
      result = 31 * result + (int) (time ^ (time >>> 32));
      return result;
    }

    @Override
    public int compareTo(Timer<W> other) {
        return Long.compare(this.time, other.time);
    }
  }

  interface TimerHandler<W> {
    /**
     * Supposed to handle an alarming timer.
     *
     * @param stamp the timestamp for which the timer was scheduled
     * @param window the window for which the timer was scheduled
     */
    void handle(long stamp, W window);
  }

  final HashSet<Timer<W>> timers = new HashSet<>();
  final PriorityQueue<Timer<W>> queue = new PriorityQueue<>(100);

  long stamp = Long.MIN_VALUE;

  long getStamp() {
    return stamp;
  }

  /**
   * Updates the clock firing passed timers.
   */
  void updateStamp(long stamp, TimerHandler<W> handler) {
    while (true) {
      Timer<W> t = queue.peek();
      // if stamp == Long.MAX_VALUE fire all pending triggers
      if (t == null || (t.getTime() >= stamp && stamp != Long.MAX_VALUE)) {
        break;
      }
      // ~ fire if the timer is strictly less than the specified stamp
      t = queue.poll();
      timers.remove(t);
      handler.handle(t.getTime(), t.getWindow());
    }
    this.stamp = stamp;
  }

  boolean registerTimer(long stamp, W window) {
    Timer<W> t = new Timer<>(window, stamp);
    if (this.timers.add(t)) {
      this.queue.add(t);
    }
    return true;
  }

  void deleteTimer(long stamp, W window) {
    Timer<W> t = new Timer<>(window, stamp);
    if (this.timers.remove(t)) {
      this.queue.remove();
    }
  }
}
