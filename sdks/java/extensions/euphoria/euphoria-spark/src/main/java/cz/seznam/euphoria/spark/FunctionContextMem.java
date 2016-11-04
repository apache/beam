package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.io.Context;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link Context} that holds all the
 * data in memory.
 */
class FunctionContextMem<T> implements Context<T>, Serializable {

  private final List<T> elements = new ArrayList<>(1);
  private Object window;

  @Override
  public void collect(T elem) {
    elements.add(elem);
  }

  @Override
  public Object getWindow() {
    return this.window;
  }

  public void setWindow(Object window) {
    this.window = window;
  }

  /**
   * Clears all stored elements.
   */
  public void clear() {
    elements.clear();
  }

  public Iterator<T> getOutputIterator() {
    // wrap output in WindowedElement
    return elements.iterator();
  }
}
