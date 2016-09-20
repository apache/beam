package cz.seznam.euphoria.flink.functions;

import java.util.Iterator;

/**
 * Adapter to make an {@link Iterator} instance appear to be an
 * {@link Iterable} instance. The iterable can be used only
 * in one iterative operation.
 */
public class IteratorIterable<T> implements Iterable<T> {

  /** the iterator being adapted into an iterable. */
  private final Iterator<T> iterator;

  private boolean consumed;

  public IteratorIterable(Iterator<T> iterator) {
    this.iterator = iterator;
  }

  @Override
  public Iterator<T> iterator() {
    if (consumed) {
      throw new IllegalStateException("This Iterable can be consumed just once");
    }

    this.consumed = true;
    return iterator;
  }
}
