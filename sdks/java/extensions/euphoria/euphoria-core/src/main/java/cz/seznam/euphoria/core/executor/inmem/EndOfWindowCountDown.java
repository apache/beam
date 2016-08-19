package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.HashMap;
import java.util.Map;

// ~ instances of this class are thread-safe;
// no external synchronization is necessary
class EndOfWindowCountDown {

  static final class CounterData {
    private int pending;

    CounterData(int pending) {
      assert pending > 0;
      this.pending = pending;
    }

    boolean countDown() {
      pending -= 1;
      return pending == 0;
    }
  }

  private final Map<WindowID, CounterData> counters = new HashMap<>();

  // ~ count down the arrival of the specified window `w`
  // ~ `expectedCountDowns` represents the awaited number of arrivals for the window
  // ~ returns {@code true} if the count down for the given window reached zero,
  // otherwise {@code false}
  boolean countDown(EndOfWindow eow, int expectedCountDowns) {
    if (expectedCountDowns <= 1) {
      return true;
    }

    synchronized (counters) {
      WindowContext w = eow.getWindowContext();
      CounterData cd = counters.get(w.getWindowID());
      if (cd == null) {
        counters.put(w.getWindowID(), new CounterData(expectedCountDowns - 1));
      } else if (cd.countDown()) {
        counters.remove(w.getWindowID());
        return true;
      }
    }
    return false;
  }
}
