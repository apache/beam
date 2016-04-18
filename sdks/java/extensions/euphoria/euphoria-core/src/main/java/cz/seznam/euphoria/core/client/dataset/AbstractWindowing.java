
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An abstract state aware windowing.
 */
public abstract class AbstractWindowing<T, KEY, W extends Window<KEY>>
    implements Windowing<T, KEY, W> {

  final Map<KEY, Set<W>> activeWindows = new HashMap<>();

  @Override
  public Set<W> getActive(KEY key) {
    Set<W> windows = activeWindows.get(key);
    if (windows == null) {
      return new HashSet<>();
    }
    return windows;
  }

  @Override
  public void close(W window) {
    window.flushAll();
    activeWindows.get(window.getKey()).remove(window);
  }

  protected W addNewWindow(W window, Triggering triggering,
      UnaryFunction<Window<?>, Void> evict) {
    KEY key = window.getKey();
    Set<W> s = activeWindows.get(key);
    if (s == null) {
      s = new HashSet<>();
      activeWindows.put(key, s);
    }
    s.add(window);
    window.registerTrigger(triggering, evict);
    return window;
  }

}
