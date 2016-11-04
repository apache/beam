package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Ordering;
import org.apache.commons.collections.comparators.NullComparator;
import org.apache.commons.lang.ObjectUtils;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

final class KeyedWindow<W extends Window, K> {
  private final W window;
  private final K key;

  public KeyedWindow(W window, K key) {
    this.window = Objects.requireNonNull(window);
    this.key = key;
  }

  public W window() {
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

  static class Comparator implements java.util.Comparator<KeyedWindow>, Serializable {

    @Override
    public int compare(KeyedWindow kw1, KeyedWindow kw2) {

      // null safe compare
      int result = ObjectUtils.compare(
              (Comparable) kw1.key, (Comparable) kw2.key);

      if (result == 0) {
        result = ObjectUtils.compare(
                (Comparable) kw1.window, (Comparable) kw2.window);
      }

      return result;
    }
  }
}
