package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;

import java.util.Objects;

public final class KeyedWindow<W, K> {
  private final WindowID<W> window;
  private final K key;

  KeyedWindow(WindowID<W> window, K key) {
    this.window = Objects.requireNonNull(window);
    this.key = key;
  }

  public WindowID<W> window() {
    return window;
  }

  public K key() {
    return key;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof KeyedWindow) {
      KeyedWindow other = (KeyedWindow) o;
      return window.equals(other.window) && Objects.equals(key, other.key);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = window.hashCode();
    result = 31 * result + (key != null ? key.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "KeyedWindow{" +
        "window=" + window +
        ", key=" + key +
        '}';
  }
}
